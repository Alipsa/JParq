package jparq.derived;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Parquet-backed regressions for the three v1.4.0 issues. */
class Issue140RegressionTest {
  @TempDir
  Path directory;

  /**
   * Writes a deterministic four-row table and empty/single-row variants.
   *
   * @throws Exception
   *           if the fixtures cannot be written
   */
  @BeforeEach
  void createFixtures() throws Exception {
    var schema = SchemaBuilder.record("issue_rows").fields().requiredLong("id").requiredString("label").optionalInt("n")
        .optionalBoolean("ab").endRecord();
    for (String table : List.of("o", "empty", "single")) {
      var file = new org.apache.hadoop.fs.Path(directory.resolve(table + ".parquet").toUri());
      try (var writer = AvroParquetWriter
          .<GenericRecord>builder(HadoopOutputFile.fromPath(file, new Configuration(false))).withSchema(schema)
          .build()) {
        int count = "empty".equals(table) ? 0 : "single".equals(table) ? 1 : 4;
        for (int i = 0; i < count; i++) {
          var record = new GenericData.Record(schema);
          record.put("id", i + 1L);
          record.put("label", List.of("d", "a", "c", "b").get(i));
          record.put("n", i == 1 ? null : i + 1);
          record.put("ab", i == 1 ? null : i > 1);
          writer.write(record);
        }
      }
    }
  }

  /**
   * Writes an inner-subquery table with a column absent from the outer schema.
   *
   * @throws Exception
   *           if the fixture cannot be written
   */
  @BeforeEach
  void createLookupFixture() throws Exception {
    var schema = SchemaBuilder.record("lookup_rows").fields().requiredString("other").endRecord();
    var file = new org.apache.hadoop.fs.Path(directory.resolve("lookup.parquet").toUri());
    try (var writer = AvroParquetWriter
        .<GenericRecord>builder(HadoopOutputFile.fromPath(file, new Configuration(false))).withSchema(schema).build()) {
      for (String label : List.of("d", "b")) {
        var record = new GenericData.Record(schema);
        record.put("other", label);
        writer.write(record);
      }
    }
  }

  /**
   * Checks both same-name qualifier forms and the different-alias control.
   *
   * @throws Exception
   *           if a query fails
   */
  @Test
  void correlatedOrderingKeepsQueryScope() throws Exception {
    for (String source : List.of("o", "o o", "o t", "o \"o\"")) {
      String qualifier = source.endsWith("t") ? "t" : source.endsWith("\"o\"") ? "\"o\"" : "o";
      String inner = "SELECT " + qualifier + ".id FROM " + source
          + " ORDER BY (SELECT COUNT(*) FROM o i WHERE i.label < " + qualifier + ".label) DESC, " + qualifier + ".id";
      expectIds("SELECT id FROM (" + inner + " LIMIT 3) x ORDER BY id", List.of(1L, 3L, 4L));
      expectIds("SELECT id FROM (" + inner + ") x ORDER BY id", List.of(1L, 2L, 3L, 4L));
      expectIds("SELECT id FROM (" + inner + " LIMIT 3) x ORDER BY id DESC LIMIT 2", List.of(4L, 3L));
      expectIds("SELECT id FROM (" + inner + " LIMIT 3) x", List.of(1L, 3L, 4L), true);
    }
    expectIds(
        "WITH x AS (SELECT id FROM o ORDER BY "
            + "(SELECT COUNT(*) FROM o i WHERE i.label < o.label) DESC, id LIMIT 3) SELECT id FROM x ORDER BY id",
        List.of(1L, 3L, 4L));
    expectIds("SELECT id FROM o ORDER BY (SELECT COUNT(*) FROM o i WHERE i.label < o.label) DESC, id LIMIT 3",
        List.of(1L, 3L, 4L));
    expectIds(
        "SELECT id FROM (SELECT id, label FROM o ORDER BY "
            + "(SELECT COUNT(*) FROM o i WHERE i.label < o.label) DESC, id LIMIT 3) x ORDER BY id",
        List.of(1L, 3L, 4L));
    expectIds("SELECT id FROM (SELECT id FROM o WHERE "
        + "(SELECT COUNT(*) FROM o i WHERE i.label < o.label) > 0) x ORDER BY id", List.of(1L, 3L, 4L));
  }

  /**
   * Preserves hidden inputs, shadowed aliases, renamed outputs, and validation.
   *
   * @throws Exception
   *           if execution fails
   */
  @Test
  void correlatedOrderingHandlesHiddenInputsAndShadowing() throws Exception {
    expectIds(
        "SELECT item FROM (SELECT id AS item FROM o ORDER BY "
            + "(SELECT COUNT(*) FROM o i WHERE i.label < o.label) DESC, id LIMIT 3) x ORDER BY item",
        List.of(1L, 3L, 4L));
    expectIds("SELECT id FROM (SELECT id FROM o ORDER BY "
        + "(SELECT COUNT(*) FROM o i WHERE i.n < o.n) DESC, id LIMIT 3) x ORDER BY id", List.of(1L, 3L, 4L));
    expectIds("SELECT id FROM (SELECT id FROM o ORDER BY "
        + "(SELECT COUNT(*) FROM o o WHERE o.label < 'c') DESC, id LIMIT 3) x ORDER BY id", List.of(1L, 2L, 3L));
    expectIds("SELECT id FROM (SELECT id FROM o ORDER BY "
        + "(SELECT COUNT(*) FROM single i WHERE i.ab = o.ab) DESC, id LIMIT 3) x ORDER BY id", List.of(1L, 2L, 3L));
    expectIds(
        "SELECT id FROM (SELECT id FROM o ORDER BY "
            + "(SELECT COUNT(*) FROM lookup i WHERE i.other = o.label) DESC, id LIMIT 3) x ORDER BY id",
        List.of(1L, 2L, 4L));
    for (boolean projection : List.of(true, false)) {
      try (var connection = DriverManager.getConnection(url(projection, true));
          var statement = connection.createStatement()) {
        assertThrows(SQLException.class, () -> statement.executeQuery("SELECT id FROM (SELECT id FROM o ORDER BY "
            + "(SELECT COUNT(*) FROM o i WHERE i.label < o.missing) DESC, id LIMIT 3) x"));
        assertThrows(SQLException.class, () -> statement.executeQuery("SELECT * FROM o a JOIN o a ON TRUE"));
        try (var result = statement.executeQuery("SELECT c FROM (SELECT 'true' AS c FROM o) d")) {
          assertEquals(Types.VARCHAR, result.getMetaData().getColumnType(1));
          int count = 0;
          while (result.next()) {
            assertEquals("true", result.getObject(1));
            count++;
          }
          assertEquals(4, count);
        }
      }
    }
  }

  /**
   * Keeps predicates Boolean across materialization, including empty results.
   *
   * @throws Exception
   *           if a query fails
   */
  @Test
  void computedBooleansKeepTypesAndNulls() throws Exception {
    for (String expression : List.of("n > 1", "CAST((n > 1) AS BOOLEAN)", "NOT (n <= 1)", "n BETWEEN 3 AND 4",
        "n IN (3, 4)", "n > 1 AND id > 1", "n > 3 OR n = 3",
        "CASE WHEN id = 2 THEN CAST(NULL AS BOOLEAN) ELSE n > 1 END", "ab")) {
      String inner = "SELECT id, " + expression + " AS c FROM o";
      var expected = Arrays.asList(false, null, true, true);
      expectBooleans("SELECT c FROM (" + inner + ") d ORDER BY id", expected);
      expectBooleans("WITH d AS (" + inner + ") SELECT c FROM d ORDER BY id", expected);
      expectBooleans("SELECT c FROM (SELECT id, c FROM (" + inner + ") d) e ORDER BY id", expected);
      expectBooleans("SELECT " + expression + " AS c FROM o ORDER BY id", expected);
      expectBooleans("SELECT c FROM (" + inner + " WHERE 1 = 0) d", List.of());
      expectBooleans("SELECT c FROM (" + inner + " WHERE id = 2) d", Arrays.asList((Boolean) null));
      for (String predicate : List.of("c", "c IS TRUE", "NOT c", "c IS FALSE", "c IS UNKNOWN", "c IS NOT UNKNOWN")) {
        List<Long> ids = predicate.equals("NOT c") || predicate.equals("c IS FALSE")
            ? List.of(1L)
            : predicate.equals("c IS UNKNOWN")
                ? List.of(2L)
                : predicate.equals("c IS NOT UNKNOWN") ? List.of(1L, 3L, 4L) : List.of(3L, 4L);
        expectIds("SELECT id FROM (" + inner + ") d WHERE " + predicate + " ORDER BY id", ids);
      }
    }
    expectBooleans("SELECT n > 1 AS n FROM o ORDER BY id", Arrays.asList(false, null, true, true));
    expectBooleans("SELECT n FROM (SELECT id, n > 1 AS n FROM o) d ORDER BY id",
        Arrays.asList(false, null, true, true));
    expectBooleans("SELECT c FROM (SELECT id, COALESCE(ab, FALSE) AS c FROM o) d ORDER BY id",
        List.of(false, false, true, true));
    expectBooleans("SELECT c FROM (SELECT TRUE AS c FROM empty) d", List.of());
    expectBooleans("SELECT n > 1 AS c FROM o ORDER BY n NULLS FIRST", Arrays.asList(null, false, true, true));
    expectBooleans("SELECT c FROM (SELECT CAST(NULL AS BOOLEAN) AS c FROM o) d", Arrays.asList(null, null, null, null));
    expectBooleans("SELECT c FROM (SELECT n > 1 AS c FROM empty) d", List.of());
    expectBooleans("SELECT c FROM (SELECT (n > 1) AS c FROM o) d", Arrays.asList(false, null, true, true));
    expectBooleans("SELECT c FROM (SELECT label LIKE 'd%' AS c FROM o) d ORDER BY c",
        List.of(false, false, false, true));
    expectBooleans("SELECT ab AS c FROM o GROUP BY ab HAVING ab IS UNKNOWN", Arrays.asList((Boolean) null));
    expectBooleans("SELECT c FROM (SELECT ab IS UNKNOWN AS c FROM o) d ORDER BY c", List.of(false, false, false, true));
    expectBooleans("SELECT c FROM (SELECT n IS NULL AS c FROM o) d ORDER BY c", List.of(false, false, false, true));
    expectBooleans("SELECT c FROM (SELECT EXISTS (SELECT 1 FROM o i WHERE i.id = o.id) AS c FROM o) d",
        List.of(true, true, true, true));
  }

  /**
   * Verifies top-level set-operation types, duplicate handling, and ordering.
   *
   * @throws Exception
   *           if a query fails
   */
  @Test
  void booleanSetOperationsKeepTypes() throws Exception {
    expectBooleans("SELECT n > 1 AS c FROM o UNION ALL SELECT n > 3 FROM o",
        Arrays.asList(false, null, true, true, false, null, false, true));
    expectBooleans("SELECT n > 1 AS c FROM o UNION SELECT n > 3 FROM o ORDER BY c", Arrays.asList(false, true, null));
    expectBooleans("SELECT n > 1 AS c FROM empty UNION ALL SELECT n > 3 FROM empty", List.of());
    expectBooleans("SELECT n > 1 AS c FROM o UNION SELECT n > 3 FROM o ORDER BY NOT c",
        Arrays.asList(true, false, null));
  }

  /**
   * Verifies exact Parquet window cardinality with streaming and buffered paths.
   *
   * @throws Exception
   *           if a query fails
   */
  @Test
  void windowsConsumeEachParquetRowOnce() throws Exception {
    for (String function : List.of("ROW_NUMBER()", "RANK()", "DENSE_RANK()", "NTILE(4)", "SUM(id)", "AVG(id)",
        "MIN(id)", "MAX(id)", "COUNT(*)", "PERCENT_RANK()", "CUME_DIST()", "LAG(id)", "LEAD(id)", "FIRST_VALUE(id)",
        "LAST_VALUE(id)", "NTH_VALUE(id, 1)")) {
      String inner = "SELECT id, " + function + " OVER (ORDER BY id) AS w FROM o";
      expectIds(inner, List.of(1L, 2L, 3L, 4L));
      expectIds(inner + " LIMIT 2", List.of(1L, 2L));
      List<Double> values = switch (function) {
        case "SUM(id)" -> List.of(1.0, 3.0, 6.0, 10.0);
        case "AVG(id)" -> List.of(1.0, 1.5, 2.0, 2.5);
        case "MIN(id)", "FIRST_VALUE(id)", "NTH_VALUE(id, 1)" -> List.of(1.0, 1.0, 1.0, 1.0);
        case "PERCENT_RANK()" -> List.of(0.0, 1.0 / 3, 2.0 / 3, 1.0);
        case "CUME_DIST()" -> List.of(0.25, 0.5, 0.75, 1.0);
        case "LAG(id)" -> Arrays.asList(null, 1.0, 2.0, 3.0);
        case "LEAD(id)" -> Arrays.asList(2.0, 3.0, 4.0, null);
        default -> List.of(1.0, 2.0, 3.0, 4.0);
      };
      expectWindowValues(inner, values);
      expectWindowValues(inner + " LIMIT 2", values.subList(0, 2));
      expectIds(inner + " LIMIT 0", List.of());
      expectIds(inner + " LIMIT 1", List.of(1L));
      expectIds(inner + " ORDER BY id", List.of(1L, 2L, 3L, 4L));
      expectIds(inner + " OFFSET 1 ROWS", List.of(2L, 3L, 4L));
    }
    expectWindowValues("SELECT id, ROW_NUMBER() OVER () AS w FROM o", List.of(1.0, 2.0, 3.0, 4.0));
    expectWindowValues("SELECT id, ROW_NUMBER() OVER (PARTITION BY ab ORDER BY id) AS w FROM o",
        List.of(1.0, 1.0, 1.0, 2.0));
    for (String over : List.of("()", "(ORDER BY id)", "(PARTITION BY ab ORDER BY id)")) {
      expectIds("SELECT id, ROW_NUMBER() OVER " + over + " AS w FROM o", List.of(1L, 2L, 3L, 4L));
    }
    for (String filter : List.of("id <> 1", "id + 0 <> 1")) {
      expectIds("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS w FROM o WHERE " + filter, List.of(2L, 3L, 4L));
    }
    expectIds("SELECT id, ROW_NUMBER() OVER () FROM empty", List.of());
    expectIds("SELECT id, ROW_NUMBER() OVER () FROM single", List.of(1L));
    expectIds("SELECT DISTINCT id FROM (SELECT id, ROW_NUMBER() OVER () AS w FROM o) x", List.of(1L, 2L, 3L, 4L));
    expectIds("SELECT COUNT(*) FROM (SELECT id, ROW_NUMBER() OVER () AS w FROM o) x", List.of(4L));
    expectIds("SELECT DISTINCT id FROM (SELECT id FROM o OFFSET 1 ROWS) x", List.of(2L, 3L, 4L));
    expectIds("SELECT id FROM (SELECT id FROM o OFFSET 1 ROWS) x OFFSET 1 ROWS", List.of(3L, 4L));
    expectIds("SELECT id FROM (SELECT id FROM o ORDER BY id DESC LIMIT 3) x ORDER BY id LIMIT 2", List.of(2L, 3L));
  }

  /**
   * Checks window values and cardinality over a Parquet reader.
   *
   * @param sql
   *          the window query
   * @param expected
   *          expected values in ID order
   * @throws Exception
   *           if execution fails
   */
  private void expectWindowValues(String sql, List<Double> expected) throws Exception {
    for (boolean pushdown : List.of(true, false)) {
      try (var connection = DriverManager.getConnection(url(pushdown, pushdown));
          var statement = connection.createStatement();
          var result = statement.executeQuery(sql)) {
        for (int i = 0; i < expected.size(); i++) {
          assertTrue(result.next(), sql);
          assertEquals(i + 1L, result.getLong(1), sql);
          Object value = result.getObject(2);
          if (expected.get(i) == null) {
            assertNull(value, sql);
          } else {
            assertInstanceOf(Number.class, value, sql);
            assertEquals(expected.get(i), ((Number) value).doubleValue(), 1e-9, sql);
          }
        }
        assertFalse(result.next(), sql);
      }
    }
  }

  /**
   * Checks ordered IDs under all pushdown combinations.
   *
   * @param sql
   *          the query
   * @param expected
   *          expected IDs
   * @throws Exception
   *           if a query fails
   */
  private void expectIds(String sql, List<Long> expected) throws Exception {
    expectIds(sql, expected, false);
  }

  /**
   * Checks IDs and output shape under all pushdown combinations.
   *
   * @param sql
   *          the query
   * @param expected
   *          expected IDs
   * @param unordered
   *          whether to compare sorted IDs
   * @throws Exception
   *           if a query fails
   */
  private void expectIds(String sql, List<Long> expected, boolean unordered) throws Exception {
    for (boolean projection : List.of(true, false)) {
      for (boolean predicate : List.of(true, false)) {
        try (var connection = DriverManager.getConnection(url(projection, predicate));
            var statement = connection.createStatement();
            var result = statement.executeQuery(sql)) {
          if (sql.startsWith("SELECT id FROM") || sql.startsWith("SELECT DISTINCT id FROM")) {
            assertEquals(1, result.getMetaData().getColumnCount(), sql);
            assertEquals("id", result.getMetaData().getColumnLabel(1), sql);
          }
          var actual = new ArrayList<Long>();
          while (result.next()) {
            actual.add(result.getLong(1));
          }
          assertFalse(result.next());
          assertEquals(expected, unordered ? actual.stream().sorted().toList() : actual, sql);
        }
      }
    }
  }

  /**
   * Checks Boolean metadata and JDBC getters under all pushdown combinations.
   *
   * @param sql
   *          the query
   * @param expected
   *          expected values, including SQL NULL
   * @throws Exception
   *           if a query fails
   */
  private void expectBooleans(String sql, List<Boolean> expected) throws Exception {
    for (boolean projection : List.of(true, false)) {
      for (boolean predicate : List.of(true, false)) {
        try (var connection = DriverManager.getConnection(url(projection, predicate));
            var statement = connection.createStatement();
            var result = statement.executeQuery(sql)) {
          assertBooleanMetadata(result);
          var actual = new ArrayList<Boolean>();
          while (result.next()) {
            Object value = result.getObject(1);
            assertTrue(value == null || value instanceof Boolean, sql);
            actual.add((Boolean) value);
            assertEquals(Boolean.TRUE.equals(value), result.getBoolean(1), sql);
            assertEquals(value == null, result.wasNull(), sql);
          }
          assertEquals(expected, actual, sql);
          assertBooleanMetadata(result);
        }
      }
    }
  }

  /**
   * Verifies stable Boolean metadata even after exhaustion.
   *
   * @param result
   *          the result set
   * @throws SQLException
   *           if metadata cannot be read
   */
  private void assertBooleanMetadata(ResultSet result) throws SQLException {
    var metadata = result.getMetaData();
    assertEquals(1, metadata.getColumnCount());
    assertEquals(Types.BOOLEAN, metadata.getColumnType(1));
    assertEquals("BOOLEAN", metadata.getColumnTypeName(1));
    assertEquals(Boolean.class.getName(), metadata.getColumnClassName(1));
  }

  /**
   * Builds the fixture URL.
   *
   * @param projection
   *          whether to project columns
   * @param predicate
   *          whether to push predicates
   * @return the JDBC URL
   */
  private String url(boolean projection, boolean predicate) {
    return "jdbc:jparq:" + directory + "?projectionPushdown=" + projection + "&predicatePushdown=" + predicate;
  }
}
