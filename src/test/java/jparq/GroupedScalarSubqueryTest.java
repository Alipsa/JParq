package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Verifies scalar subqueries evaluated against grouped outer values. */
class GroupedScalarSubqueryTest {
  @TempDir
  Path base;

  @BeforeEach
  void fixtures() throws Exception {
    write("o", "n", Arrays.asList(1, 1, 2, 3, null));
    write("t", "v", Arrays.asList(1, 1, 2, null));
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void scalarSubqueriesReadEachGroup(boolean predicate, boolean projection) throws Exception {
    try (var connection = connect(predicate, projection)) {
      String sql = "SELECT o.n, (SELECT COUNT(*) FROM (VALUES (1), (1), (2)) t(v) WHERE t.v = o.n) c "
          + "FROM (VALUES (1), (1), (2), (3), (NULL)) o(n) GROUP BY o.n ORDER BY o.n NULLS LAST";
      assertEquals(Arrays.asList(List.of(1L, 2L), List.of(2L, 1L), List.of(3L, 0L), Arrays.asList(null, 0L)),
          rows(connection, sql));
      String select = "SELECT o.n, (SELECT COUNT(*) FROM t WHERE t.v = o.n) c FROM ";
      String grouped = " o GROUP BY o.n ORDER BY o.n NULLS LAST";
      List<List<Object>> expected = Arrays.asList(List.of(1, 2L), List.of(2, 1L), List.of(3, 0L),
          Arrays.asList(null, 0L));
      assertEquals(expected, rows(connection, select + "o GROUP BY o.n ORDER BY o.n NULLS LAST"));
      assertEquals(expected, rows(connection, select + "(SELECT n FROM o)" + grouped));
      assertEquals(expected, rows(connection, "WITH source AS (SELECT n FROM o) " + select + "source" + grouped));
      assertEquals(expected, rows(connection, "SELECT * FROM (" + select + "o GROUP BY o.n) d ORDER BY n NULLS LAST"));
      try (var statement = connection.prepareStatement(select + "o GROUP BY o.n");
          var result = statement.executeQuery()) {
        assertEquals(Types.BIGINT, result.getMetaData().getColumnType(2));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void correlationsRespectInnerScopesAndUnprojectedGroups(boolean predicate, boolean projection) throws Exception {
    try (var connection = connect(predicate, projection)) {
      String grouped = " FROM o GROUP BY o.n ORDER BY o.n NULLS LAST";
      List<List<Object>> counts = List.of(List.of(2L), List.of(1L), List.of(0L), List.of(0L));
      assertEquals(counts, rows(connection, "SELECT (SELECT COUNT(*) FROM t WHERE v = n)" + grouped));
      assertEquals(counts,
          rows(connection, "SELECT (SELECT COUNT(*) FROM t WHERE EXISTS " + "(SELECT 1 WHERE t.v = o.n))" + grouped));
      assertEquals(List.of(List.of(3L), List.of(3L), List.of(3L), List.of(3L)),
          rows(connection, "SELECT (SELECT COUNT(*) FROM (SELECT v AS n FROM t) d WHERE n IS NOT NULL)" + grouped));
      assertEquals(List.of(List.of(2L), List.of(2L), List.of(2L), List.of(2L)),
          rows(connection, "SELECT (SELECT COUNT(*) FROM (SELECT v AS n FROM t) o WHERE o.n = 1)" + grouped));
      assertEquals(Arrays.asList(List.of(1), List.of(2), List.of(3), Arrays.asList((Object) null)),
          rows(connection, "SELECT (SELECT o.n)" + grouped));
      assertEquals(Arrays.asList(List.of(1), List.of(2), List.of(3), Arrays.asList((Object) null)),
          rows(connection, "SELECT ((SELECT o.n)) FROM o GROUP BY (n) ORDER BY n NULLS LAST"));
      assertEquals(List.of(List.of(1, 2)),
          rows(connection, "SELECT (SELECT a.n), (SELECT b.n) FROM o a CROSS JOIN o b WHERE a.n = 1 AND b.n = 2 "
              + "GROUP BY a.n, b.n"));
      assertEquals(List.of(List.of(4L, 5L)), rows(connection, "SELECT (SELECT COUNT(*) FROM t), COUNT(*) FROM o"));
      assertEquals(List.of(List.of(5L, 4L)), rows(connection, "SELECT COUNT(*), (SELECT COUNT(*) FROM t) FROM o"));
      assertEquals(List.of(List.of(1L, 1L), List.of(0L, 1L), List.of(0L, 1L)),
          rows(connection, "SELECT (SELECT COUNT(*) FROM t WHERE v = o.n) c, COUNT(*) FROM o GROUP BY o.n "
              + "HAVING COUNT(*) = 1 ORDER BY c DESC"));
    }
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void emptyResultsAndGroupingSetsPreserveScalarSemantics(boolean predicate, boolean projection) throws Exception {
    try (var connection = connect(predicate, projection)) {
      assertEquals(List.of(),
          rows(connection, "SELECT (SELECT COUNT(*) FROM t WHERE v = o.n) FROM o WHERE n > 10 GROUP BY n"));
      assertEquals(
          Arrays.asList(Arrays.asList(1, null), List.of(2, 2), Arrays.asList(3, null), Arrays.asList(null, null)),
          rows(connection,
              "SELECT n, (SELECT v FROM t WHERE v = o.n AND v = 2) FROM o GROUP BY n " + "ORDER BY n NULLS LAST"));
      assertEquals(
          Arrays.asList(List.of(1, 2L, 0), List.of(2, 1L, 0), List.of(3, 0L, 0), Arrays.asList(null, 0L, 0),
              Arrays.asList(null, 0L, 1)),
          rows(connection, "SELECT n, (SELECT COUNT(*) FROM t WHERE v = o.n), GROUPING(n) g FROM o GROUP BY ROLLUP(n) "
              + "ORDER BY g, n NULLS LAST"));
    }
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void ungroupedCorrelationsAndNonScalarResultsAreRejected(boolean predicate, boolean projection) throws Exception {
    try (var connection = connect(predicate, projection)) {
      for (String sql : List.of("SELECT (SELECT o.n) FROM o GROUP BY n + 1", "SELECT (SELECT n) FROM o GROUP BY n + 1",
          "SELECT (SELECT n) FROM o WHERE n > 10 GROUP BY n + 1", "SELECT COUNT(*), (SELECT n) FROM o",
          "SELECT (SELECT t.v) FROM o CROSS JOIN t GROUP BY o.n",
          "SELECT (SELECT o.n) FROM o CROSS JOIN o other GROUP BY other.n",
          "SELECT (SELECT v FROM t WHERE v = o.n) FROM o GROUP BY n",
          "SELECT (SELECT v, v FROM t WHERE v = o.n) FROM o GROUP BY n")) {
        SQLException error = assertThrows(SQLException.class, () -> rows(connection, sql), sql);
        assertEquals("42000", error.getSQLState(), sql);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void scalarCardinalityErrorsMatchAcrossSelectAndHaving(boolean predicate, boolean projection) throws Exception {
    try (var connection = connect(predicate, projection)) {
      for (String scalar : List.of("SELECT v FROM t", "SELECT v, v FROM t WHERE v = 2",
          "SELECT v, v FROM t WHERE v > 10")) {
        for (String sql : List.of("SELECT (" + scalar + ") FROM o", "SELECT n, (" + scalar + ") FROM o GROUP BY n",
            "SELECT n FROM o GROUP BY n HAVING (" + scalar + ") = 2")) {
          SQLSyntaxErrorException error = assertThrows(SQLSyntaxErrorException.class, () -> rows(connection, sql), sql);
          assertEquals("42000", error.getSQLState(), sql);
          assertTrue(error.getMessage().contains(scalar.contains("v, v") ? "one column" : "one row"),
              error.getMessage());
        }
      }
      assertEquals(List.of(List.of(1), List.of(2), List.of(3)), rows(connection,
          "SELECT n FROM o GROUP BY n HAVING (SELECT v FROM t WHERE v = 2) = 2 AND n IS NOT NULL ORDER BY n"));
      assertEquals(List.of(), rows(connection, "SELECT n FROM o GROUP BY n HAVING (SELECT v FROM t WHERE v > 10) = 2"));
      assertEquals(List.of(),
          rows(connection, "SELECT n FROM o GROUP BY n HAVING (SELECT v FROM t WHERE v IS NULL) = 2"));
    }
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void havingSkipsRejectedGroupsAndEvaluatesAliasesOnDemand(boolean predicate, boolean projection) throws Exception {
    try (var connection = connect(predicate, projection)) {
      String select = "SELECT n, (SELECT v FROM t WHERE v = o.n) c FROM o GROUP BY n HAVING ";
      assertEquals(List.of(List.of(2, 2)), rows(connection, select + "n = 2"));
      assertEquals(List.of(), rows(connection, select + "COUNT(*) = 5"));
      assertEquals(List.of(List.of(2, 2)), rows(connection, select + "n = 2 AND c = 2"));
      assertEquals(List.of(List.of(2, 2)), rows(connection, select + "n = 2 AND " + "(SELECT c) = 2"));
      assertEquals(List.of(List.of(2, 2)), rows(connection, select + "NOT (n <> 2 OR c <> 2)"));
      assertEquals(List.of(List.of(1, 2L), List.of(2, 1L)), rows(connection,
          "SELECT n, (SELECT COUNT(*) FROM t WHERE v = o.n) c FROM o GROUP BY n HAVING c > 0 ORDER BY n"));
      String nullable = "SELECT n, (SELECT v FROM t WHERE v = o.n AND v = 2) c FROM o GROUP BY n HAVING ";
      assertEquals(Arrays.asList(Arrays.asList(1, null), Arrays.asList(3, null), Arrays.asList(null, null)),
          rows(connection, nullable + "c IS NULL ORDER BY n NULLS LAST"));
      assertEquals(List.of(List.of(2, 2)), rows(connection, nullable + "c = 2 AND n >= 2"));
      assertEquals(Arrays.asList(List.of(2, 2), Arrays.asList(3, null)),
          rows(connection, nullable + "c = 2 OR n = 3 ORDER BY n"));
      assertThrows(SQLException.class, () -> rows(connection, select + "c = 2"));
      String typed = "SELECT (SELECT CASE WHEN o.n = 1 THEN 1 WHEN o.n = 2 THEN CAST(2 AS INTEGER) ELSE NULL END) c "
          + "FROM o GROUP BY n HAVING c = 2";
      try (var statement = connection.prepareStatement(typed); var result = statement.executeQuery()) {
        assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertEquals(2, result.getObject(1));
        assertFalse(result.next());
      }
    }
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void scalarExpressionSortsAndEmptyCountKeepsItsType(boolean predicate, boolean projection) throws Exception {
    try (var connection = connect(predicate, projection)) {
      String scalar = "(SELECT COUNT(*) FROM t WHERE v = o.n)";
      String select = "SELECT n, " + scalar + " c FROM o GROUP BY n";
      List<List<Object>> expected = Arrays.asList(List.of(1, 2L), List.of(2, 1L), List.of(3, 0L),
          Arrays.asList(null, 0L));
      for (String order : List.of("c", "2", scalar, "(" + scalar + ")")) {
        assertEquals(expected, rows(connection, select + " ORDER BY " + order + " DESC, n NULLS LAST"));
      }
      for (String sql : List.of("SELECT " + scalar + " c FROM o WHERE n > 10 GROUP BY n",
          "SELECT " + scalar + " c FROM o GROUP BY n HAVING COUNT(*) = 5")) {
        try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
          assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Writes a nullable integer fixture column.
   *
   * @param table
   *          fixture table name
   * @param field
   *          fixture field name
   * @param values
   *          field values
   * @throws Exception
   *           if writing fails
   */
  private void write(String table, String field, List<Integer> values) throws Exception {
    Schema schema = SchemaBuilder.record("Row").fields().optionalInt(field).endRecord();
    var configuration = new Configuration(false);
    var file = new org.apache.hadoop.fs.Path(base.resolve(table + ".parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, configuration))
        .withSchema(schema).withConf(configuration).build()) {
      for (Integer value : values) {
        var row = new GenericData.Record(schema);
        row.put(field, value);
        writer.write(row);
      }
    }
  }

  /**
   * Opens a connection with independent pushdown settings.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @return fixture connection
   * @throws SQLException
   *           if connection creation fails
   */
  private Connection connect(boolean predicate, boolean projection) throws SQLException {
    return DriverManager
        .getConnection("jdbc:jparq:" + base + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection);
  }

  /**
   * Executes a prepared query and reads all values without JDBC conversion.
   *
   * @param connection
   *          fixture connection
   * @param sql
   *          query to execute
   * @return result rows
   * @throws SQLException
   *           if preparation or execution fails
   */
  private static List<List<Object>> rows(Connection connection, String sql) throws SQLException {
    try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
      List<List<Object>> rows = new ArrayList<>();
      while (result.next()) {
        List<Object> row = new ArrayList<>();
        for (int column = 1; column <= result.getMetaData().getColumnCount(); column++) {
          row.add(result.getObject(column));
        }
        rows.add(row);
      }
      return rows;
    }
  }
}
