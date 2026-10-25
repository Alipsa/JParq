package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies set operations materialized as derived-table inputs. */
class DerivedSetOperationTest {

  /**
   * Preserves computed Boolean types, values, and NULL multiplicity from a
   * nullable physical column through derived UNION and UNION ALL inputs.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @param directory
   *          the temporary fixture directory
   * @throws Exception
   *           if writing the fixture or executing a query fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void nullablePhysicalComparisonsRemainBoolean(boolean pushdown, @TempDir Path directory) throws Exception {
    var schema = SchemaBuilder.record("nullable_numbers").fields().optionalInt("n").endRecord();
    var file = new org.apache.hadoop.fs.Path(directory.resolve("o.parquet").toUri());
    var configuration = new Configuration(false);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, configuration))
        .withSchema(schema).withConf(configuration).build()) {
      Integer[] numbers = {
          null, 1, 2, 4
      };
      for (Integer number : numbers) {
        var record = new GenericData.Record(schema);
        record.put("n", number);
        writer.write(record);
      }
    }
    try (var connection = DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown)) {
      for (String operator : List.of("UNION", "UNION ALL")) {
        String sql = "SELECT c FROM (SELECT n > 1 AS c FROM o " + operator + " SELECT n > 3 AS c FROM o) d";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals(1, result.getMetaData().getColumnCount(), sql);
          assertEquals("c", result.getMetaData().getColumnLabel(1), sql);
          assertEquals(Types.BOOLEAN, result.getMetaData().getColumnType(1), sql);
          List<Boolean> actual = new ArrayList<>();
          while (result.next()) {
            Object value = result.getObject(1);
            assertEquals(value == null, result.wasNull(), sql);
            if (value != null) {
              assertInstanceOf(Boolean.class, value, sql);
            }
            actual.add((Boolean) value);
          }
          boolean all = operator.equals("UNION ALL");
          assertEquals(all ? 8 : 3, actual.size(), sql);
          assertEquals(all ? 3 : 1, Collections.frequency(actual, false), sql);
          assertEquals(all ? 3 : 1, Collections.frequency(actual, true), sql);
          assertEquals(all ? 2 : 1, Collections.frequency(actual, null), sql);
          assertFalse(result.next(), sql);
          assertEquals(Types.BOOLEAN, result.getMetaData().getColumnType(1), sql);
        }
      }
    }
  }

  /**
   * Reproduces issue 198 using a physical input and verifies duplicate counts.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws SQLException
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void physicalUnionAllPreservesEveryRow(boolean pushdown) throws SQLException {
    try (var connection = connect(pushdown)) {
      List<Integer> expected = new ArrayList<>(Collections.nCopies(32, 1));
      expected.addAll(Collections.nCopies(32, 2));
      assertRows(connection, "SELECT * FROM (SELECT 1 AS a FROM mtcars UNION ALL SELECT 2 FROM mtcars) x ORDER BY a",
          expected);
    }
  }

  /**
   * Checks each set operator, aliases, nesting, joins, and metadata for empty
   * inputs.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws SQLException
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void setOperatorsWorkAtDerivedBoundaries(boolean pushdown) throws SQLException {
    List<String> operators = List.of("UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL");
    List<List<Integer>> expected = List.of(List.of(1, 2, 3), List.of(1, 1, 1, 2, 3), List.of(1), List.of(1), List.of(2),
        List.of(1, 2));
    try (var connection = connect(pushdown)) {
      for (int i = 0; i < operators.size(); i++) {
        String body = "SELECT a FROM (VALUES (1), (1), (2)) l(a) " + operators.get(i)
            + " SELECT b FROM (VALUES (1), (3)) r(b)";
        for (String source : List.of("(" + body + ") x", "((" + body + ")) x",
            "(SELECT * FROM (" + body + ") nested) x", "(" + body + ") x JOIN (VALUES (1)) j(k) ON x.a >= j.k")) {
          assertRows(connection, "SELECT x.* FROM " + source + " ORDER BY x.a", expected.get(i));
        }
        assertRows(connection, "SELECT x.renamed FROM (" + body + ") x(renamed) ORDER BY renamed", expected.get(i));
        assertRows(connection, "SELECT x.a FROM (" + body + ") x WHERE x.a < 0", List.of());
        String emptyBody = "SELECT CAST(1 AS DECIMAL(10, 0)) AS a WHERE FALSE " + operators.get(i)
            + " SELECT CAST(2 AS DECIMAL(10, 0)) WHERE FALSE";
        assertRows(connection, "SELECT x.a FROM (" + emptyBody + ") x", List.of());
      }
    }
  }

  /**
   * Keeps set ordering and bounds local and preserves local and inherited CTE
   * scope.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws SQLException
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void boundsAndCteScopesRemainLocal(boolean pushdown) throws SQLException {
    try (var connection = connect(pushdown)) {
      assertRows(connection, "SELECT a FROM (SELECT 1 AS a UNION ALL SELECT 2 UNION ALL SELECT 3"
          + " ORDER BY a DESC OFFSET 1 ROW FETCH FIRST 2 ROWS ONLY) x WHERE a > 0 ORDER BY a LIMIT 1", List.of(1));
      assertRows(connection,
          "WITH c AS (SELECT 7 AS a) SELECT x.a FROM (SELECT a FROM c UNION ALL SELECT a FROM c) x ORDER BY a",
          List.of(7, 7));
      assertRows(connection, "WITH c AS (SELECT 7 AS a) SELECT x.a FROM (WITH c AS (SELECT 9 AS a)"
          + " SELECT a FROM c UNION ALL SELECT a FROM c) x ORDER BY a", List.of(9, 9));
      assertRows(connection, "SELECT a FROM ((SELECT 1 AS a UNION SELECT 2) INTERSECT SELECT 2) x", List.of(2));
    }
  }

  /**
   * Rebinds outer values in both branches of a LATERAL set query.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws SQLException
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void lateralSetBranchesUseCurrentOuterRow(boolean pushdown) throws SQLException {
    try (var connection = connect(pushdown)) {
      assertRows(connection,
          "SELECT x.a FROM (VALUES (1), (2)) s(id) CROSS JOIN LATERAL"
              + " (SELECT v.a FROM (VALUES (1), (2)) v(a) WHERE v.a = s.id UNION ALL"
              + " SELECT v.a FROM (VALUES (11), (12)) v(a) WHERE v.a = s.id + 10) x ORDER BY x.a",
          List.of(1, 2, 11, 12));
    }
  }

  /**
   * Validates decimal declarations in both set branches before reading rows.
   *
   * @param lateral
   *          whether the set query is a LATERAL input
   * @throws SQLException
   *           if JDBC setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void invalidDecimalBoundsRemainSyntaxErrors(boolean lateral) throws SQLException {
    try (var connection = connect(true)) {
      for (String body : List.of("SELECT CAST(NULL AS DECIMAL(2, 5)) AS a UNION ALL SELECT 1",
          "SELECT 1 AS a UNION ALL SELECT CAST(NULL AS DECIMAL(2, 5))",
          "SELECT 1 AS a UNION ALL SELECT 2 ORDER BY CAST(NULL AS DECIMAL(2, 5))")) {
        String sql = "SELECT * FROM " + (lateral ? "(VALUES (1)) s(id) CROSS JOIN LATERAL " : "") + "(" + body + ") x";
        try (var statement = connection.createStatement()) {
          SQLException error = assertThrows(SQLException.class, () -> statement.executeQuery(sql), sql);
          assertEquals("42000", error.getSQLState(), sql);
          assertTrue(error.getMessage().contains("Invalid decimal bounds"), error.getMessage());
        }
      }
    }
  }

  /**
   * Enforces positional alias counts even when a set query returns no rows.
   *
   * @param empty
   *          whether both branches are empty
   * @throws SQLException
   *           if JDBC setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void aliasCountMismatchHasSqlDiagnostics(boolean empty) throws SQLException {
    String predicate = empty ? " WHERE FALSE" : "";
    String body = "SELECT cyl FROM mtcars" + predicate + " UNION ALL SELECT cyl FROM mtcars" + predicate;
    try (var connection = connect(true); var statement = connection.createStatement()) {
      SQLException error = assertThrows(SQLException.class,
          () -> statement.executeQuery("SELECT * FROM (" + body + ") x(a, b)"));
      assertEquals("42000", error.getSQLState());
      assertTrue(error.getMessage().contains("Derived table alias declares 2 columns but subquery produces 1 columns"),
          error.getMessage());
    }
  }

  /**
   * Opens a connection over the physical regression fixture.
   *
   * @param pushdown
   *          whether to enable both pushdown options
   * @return a connection owned by the caller
   * @throws SQLException
   *           if the connection cannot be opened
   */
  private static Connection connect(boolean pushdown) throws SQLException {
    return DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath()
        + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
  }

  /**
   * Checks values and stable numeric metadata, including after exhaustion.
   *
   * @param connection
   *          the JDBC connection
   * @param sql
   *          the query to execute
   * @param expected
   *          the ordered integer values
   * @throws SQLException
   *           if JDBC execution fails
   */
  private static void assertRows(Connection connection, String sql, List<Integer> expected) throws SQLException {
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      assertEquals(1, result.getMetaData().getColumnCount(), sql);
      int type = result.getMetaData().getColumnType(1);
      assertTrue(type == Types.INTEGER || type == Types.BIGINT || type == Types.NUMERIC || type == Types.DECIMAL,
          sql + ": " + type);
      List<Integer> actual = new ArrayList<>();
      while (result.next()) {
        assertInstanceOf(Number.class, result.getObject(1), sql);
        actual.add(result.getInt(1));
      }
      assertEquals(expected, actual, sql);
      assertFalse(result.next(), sql);
      assertEquals(type, result.getMetaData().getColumnType(1), sql);
    }
  }
}
