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

/** Parquet-backed regressions for JParq version 1.4.0. */
class Version140RegressionTest {
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
   * Verifies that CTE materialization applies inner row bounds once.
   *
   * @throws Exception
   *           if a query fails
   */
  @Test
  void cteBoundsBelongToTheirOwnScope() throws Exception {
    for (String body : List.of("SELECT id FROM o ORDER BY id OFFSET 1 ROWS",
        "SELECT id FROM o ORDER BY id LIMIT 2 OFFSET 1 ROWS")) {
      List<Long> ascending = body.contains("LIMIT") ? List.of(2L, 3L) : List.of(2L, 3L, 4L);
      expectIds("WITH x AS (" + body + ") SELECT id FROM x", ascending, true);
      expectIds("WITH x AS (" + body + ") SELECT id FROM x ORDER BY id", ascending);
      expectIds("SELECT id FROM (" + body + ") x ORDER BY id", ascending);
    }
    expectIds("WITH x AS (SELECT id FROM o ORDER BY id LIMIT 2 OFFSET 1 ROWS) " + "SELECT id FROM x ORDER BY id DESC",
        List.of(3L, 2L));
    expectIds("WITH x AS (SELECT id FROM o OFFSET 2 ROWS) SELECT COUNT(*) FROM x", List.of(2L));
    expectIds("WITH x AS (SELECT id FROM o ORDER BY id LIMIT 3 OFFSET 1 ROWS) "
        + "SELECT id FROM x WHERE id > 2 ORDER BY id DESC LIMIT 1 OFFSET 1 ROWS", List.of(3L));
    expectIds("WITH x(k) AS (SELECT id FROM o ORDER BY id LIMIT 2 OFFSET 1 ROWS), "
        + "y AS (SELECT k FROM x ORDER BY k DESC LIMIT 1) SELECT k FROM y", List.of(3L));
    expectIds("WITH x AS (SELECT id FROM o LIMIT 0) SELECT id FROM x", List.of());
    expectIds("WITH x AS (SELECT id FROM o OFFSET 8 ROWS) SELECT id FROM x", List.of());
    expectIds("WITH x AS (SELECT id FROM empty) SELECT id FROM x", List.of());
    expectIds("WITH x AS (SELECT id FROM single OFFSET 0 ROWS) SELECT id FROM x", List.of(1L));
    expectIds("WITH x AS (SELECT id FROM o ORDER BY id LIMIT 2 OFFSET 1 ROWS) "
        + "SELECT a.id FROM x a JOIN x b ON a.id = b.id ORDER BY a.id", List.of(2L, 3L));
    expectIds("WITH x AS (SELECT DISTINCT ab FROM o LIMIT 3) SELECT COUNT(*) FROM x", List.of(3L));
    expectIds("WITH x AS (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM o "
        + "ORDER BY id LIMIT 2 OFFSET 1 ROWS) SELECT rn FROM x ORDER BY rn", List.of(2L, 3L));
  }

  /**
   * Wildcards read materialized output names and computed values.
   *
   * @throws Exception
   *           if a query fails
   */
  @Test
  void cteWildcardsPreserveMaterializedValues() throws Exception {
    expectRows(
        "WITH x(k, r) AS (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM o "
            + "ORDER BY id LIMIT 2 OFFSET 1 ROWS) SELECT * FROM x ORDER BY k",
        List.of(List.of(2L, 2L), List.of(3L, 3L)));
    expectRows(
        "WITH x AS (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM o "
            + "ORDER BY id LIMIT 2 OFFSET 1 ROWS) SELECT * FROM x ORDER BY id",
        List.of(List.of(2L, 2L), List.of(3L, 3L)));
    expectRows("SELECT * FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM o "
        + "ORDER BY id LIMIT 2 OFFSET 1 ROWS) x ORDER BY id", List.of(List.of(2L, 2L), List.of(3L, 3L)));
    expectRows("WITH x(k) AS (SELECT id FROM empty) SELECT * FROM x", List.of());
    for (boolean projection : List.of(true, false)) {
      try (var connection = DriverManager.getConnection(url(projection, true));
          var statement = connection.createStatement();
          var result = statement.executeQuery("WITH x(k) AS (SELECT id FROM empty) SELECT * FROM x")) {
        assertEquals(1, result.getMetaData().getColumnCount());
        assertEquals("k", result.getMetaData().getColumnLabel(1));
        assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Verifies computed DISTINCT keys after windows and before final row bounds.
   *
   * @throws Exception
   *           if a query fails
   */
  @Test
  void distinctUsesEvaluatedWindowProjections() throws Exception {
    expectRows("SELECT DISTINCT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM o ORDER BY id",
        List.of(List.of(1L, 1L), List.of(2L, 2L), List.of(3L, 3L), List.of(4L, 4L)));
    expectRows("SELECT DISTINCT id, ROW_NUMBER() OVER (ORDER BY id) FROM o ORDER BY id LIMIT 2",
        List.of(List.of(1L, 1L), List.of(2L, 2L)));
    expectRows("SELECT DISTINCT COUNT(*) OVER () AS id, id FROM o ORDER BY 2",
        List.of(List.of(4L, 1L), List.of(4L, 2L), List.of(4L, 3L), List.of(4L, 4L)));
    expectIds("SELECT DISTINCT COUNT(*) OVER () AS total FROM o", List.of(4L));
    expectIds("SELECT DISTINCT COUNT(*) OVER (PARTITION BY id > 2) AS total FROM o", List.of(2L));
    expectIds("SELECT DISTINCT COUNT(*) OVER () AS total FROM o WHERE id > 1", List.of(3L));
    expectIds("SELECT DISTINCT COUNT(*) OVER () AS total FROM empty", List.of());
    expectIds("SELECT DISTINCT COUNT(*) OVER () AS total FROM single", List.of(1L));
    expectIds("SELECT DISTINCT COUNT(*) OVER () AS total FROM o LIMIT 0", List.of());
    expectIds("SELECT DISTINCT RANK() OVER (ORDER BY ab) AS r FROM o ORDER BY r", List.of(1L, 2L, 4L));
    expectRows("SELECT DISTINCT LAG(n) OVER (ORDER BY id) AS previous FROM o ORDER BY previous",
        Arrays.asList(List.of(1), List.of(3), Arrays.asList((Object) null)));
    expectRows("SELECT DISTINCT LEAD(n, 1, CAST(id AS INTEGER)) OVER (ORDER BY id) AS following "
        + "FROM o ORDER BY following", Arrays.asList(List.of(3), List.of(4), Arrays.asList((Object) null)));
    expectIds("SELECT DISTINCT FIRST_VALUE(n) OVER (ORDER BY id) AS first FROM o", List.of(1L));
    expectBooleans("SELECT DISTINCT COUNT(*) OVER () > 2 AS c FROM o", List.of(true));
    expectBooleans("SELECT DISTINCT ab AS c FROM o ORDER BY c", Arrays.asList(false, true, null));
    expectIds("SELECT DISTINCT id / id AS k FROM o ORDER BY k", List.of(1L));
    expectIds("SELECT DISTINCT COUNT(*) OVER () AS total FROM o ORDER BY total OFFSET 1 ROWS", List.of());
    expectIds("SELECT DISTINCT rn FROM (SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn FROM o) x "
        + "ORDER BY rn DESC LIMIT 2 OFFSET 1 ROWS", List.of(3L, 2L));
  }

  /**
   * Verifies explicit and default NULL placement in set-operation ordering.
   *
   * @throws Exception
   *           if a query fails
   */
  @Test
  void setOrderingPreservesNullDirectives() throws Exception {
    var directives = List.of("ASC NULLS FIRST", "ASC NULLS LAST", "DESC NULLS FIRST", "DESC NULLS LAST");
    List<List<Boolean>> expected = List.of(Arrays.asList(null, false, true), Arrays.asList(false, true, null),
        Arrays.asList(null, true, false), Arrays.asList(true, false, null));
    List<List<Integer>> numeric = List.of(Arrays.asList(null, 1, 3, 4), Arrays.asList(1, 3, 4, null),
        Arrays.asList(null, 4, 3, 1), Arrays.asList(4, 3, 1, null));
    for (int i = 0; i < directives.size(); i++) {
      expectRows("SELECT n FROM o UNION SELECT n FROM empty ORDER BY 1 " + directives.get(i),
          numeric.get(i).stream().map(value -> Arrays.asList((Object) value)).toList());
      for (String key : List.of("c", "1", "NOT c")) {
        var values = key.equals("NOT c")
            ? expected.get(i).stream().map(v -> v == null ? null : !v).toList()
            : expected.get(i);
        expectBooleans("SELECT n > 1 AS c FROM o UNION SELECT n > 3 FROM o ORDER BY " + key + " " + directives.get(i),
            values);
      }
    }
    expectBooleans("SELECT n > 1 AS c FROM o UNION SELECT n > 3 FROM o ORDER BY c", Arrays.asList(false, true, null));
    expectBooleans("SELECT n > 1 AS c FROM o UNION SELECT n > 3 FROM o ORDER BY c DESC",
        Arrays.asList(null, true, false));
    for (String op : List.of("UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL")) {
      List<Boolean> values = switch (op) {
        case "UNION ALL" -> Arrays.asList(null, null, false, false, false, true, true, true);
        case "INTERSECT", "INTERSECT ALL" -> Arrays.asList(null, false, true);
        case "EXCEPT ALL" -> List.of(true);
        default -> List.of();
      };
      expectBooleans("SELECT n > 1 AS c FROM o " + op + " SELECT n > 3 FROM o " + "ORDER BY c ASC NULLS FIRST", values);
    }
    expectBooleans(
        "SELECT n > 1 AS c FROM o UNION SELECT n > 3 FROM o " + "ORDER BY c NULLS FIRST LIMIT 2 OFFSET 1 ROWS",
        List.of(false, true));
    expectBooleans(
        "SELECT ab AS c FROM o WHERE id = 2 UNION ALL SELECT ab FROM o WHERE id = 2 " + "ORDER BY c DESC NULLS LAST",
        Arrays.asList(null, null));
    expectBooleans("SELECT ab AS c FROM empty UNION SELECT ab FROM empty ORDER BY c NULLS FIRST", List.of());
    expectRows(
        "SELECT ab AS c, id FROM o UNION ALL SELECT ab, id FROM empty "
            + "ORDER BY c ASC NULLS FIRST, id DESC NULLS LAST",
        List.of(Arrays.asList(null, 2L), List.of(false, 1L), List.of(true, 4L), List.of(true, 3L)));
  }

  /**
   * Checks complete projected tuples under each independent pushdown setting.
   *
   * @param sql
   *          query to execute
   * @param expected
   *          expected tuples
   * @throws Exception
   *           if execution fails
   */
  private void expectRows(String sql, List<List<Object>> expected) throws Exception {
    for (boolean projection : List.of(true, false)) {
      for (boolean predicate : List.of(true, false)) {
        try (var connection = DriverManager.getConnection(url(projection, predicate));
            var statement = connection.createStatement();
            var result = statement.executeQuery(sql)) {
          if (!expected.isEmpty()) {
            assertEquals(expected.getFirst().size(), result.getMetaData().getColumnCount(), sql);
          }
          var rows = new ArrayList<List<Object>>();
          while (result.next()) {
            var row = new ArrayList<Object>();
            for (int c = 1; c <= result.getMetaData().getColumnCount(); c++) {
              row.add(result.getObject(c));
            }
            rows.add(row);
          }
          assertEquals(expected, rows, sql);
          assertFalse(result.next(), sql);
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
