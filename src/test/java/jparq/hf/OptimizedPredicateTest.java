package jparq.hf;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqPreparedStatement;
import se.alipsa.jparq.engine.ExpressionEvaluator;
import se.alipsa.jparq.engine.ParquetFilterBuilder;
import se.alipsa.jparq.engine.SqlTruth;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;

/**
 * Fixed expected results for NULL semantics, numeric bounds, and join pushdown.
 */
class OptimizedPredicateTest {
  @TempDir
  Path directory;
  private Schema schema;

  /** Creates a small purpose-built table containing NULLs. */
  @BeforeEach
  void createFixture() throws Exception {
    schema = SchemaBuilder.record("numbers").fields().optionalLong("id").optionalInt("n").optionalString("text")
        .endRecord();
    write("a", new Long[]{
        1L, 2L, 3L, 4L
    }, new Integer[]{
        1, null, 3, 4
    });
    write("b", new Long[]{
        1L, 3L, 5L, null
    }, new Integer[]{
        1, 3, 5, null
    });
  }

  /** Writes independent join input values. */
  private void write(String name, Long[] ids, Integer[] values) throws Exception {
    var file = new org.apache.hadoop.fs.Path(directory.resolve(name + ".parquet").toUri());
    try (var writer = AvroParquetWriter.<org.apache.avro.generic.GenericRecord>builder(
        org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(file, new Configuration(false))).withSchema(schema)
        .withConf(new Configuration(false)).build()) {
      for (int i = 0; i < ids.length; i++) {
        var record = new GenericData.Record(schema);
        record.put("id", ids[i]);
        record.put("n", values[i]);
        record.put("text", values[i] == null ? null : "prefix-" + values[i]);
        writer.write(record);
      }
    }
  }

  /**
   * Executes a query under both pushdown settings. NULL assertions always use
   * fixed expected values.
   */
  private void expect(String sql, List<Long> expected) throws Exception {
    for (boolean pushdown : new boolean[]{
        true, false
    }) {
      assertEquals(expected, query(sql, pushdown), sql + "; pushdown=" + pushdown);
    }
  }

  /** Collects the first column of a query. */
  private List<Long> query(String sql, boolean pushdown) throws Exception {
    List<Long> rows = new ArrayList<>();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      while (result.next()) {
        long value = result.getLong(1);
        rows.add(result.wasNull() ? null : value);
      }
    }
    return rows;
  }

  /**
   * Checks the plan's reported NULL regressions and their computed residual
   * equivalents.
   */
  @Test
  void nullNegationsFollowSqlTruthTables() throws Exception {
    expect("SELECT id FROM a WHERE n <> 3 ORDER BY id", List.of(1L, 4L));
    expect("SELECT id FROM a WHERE NOT (n = 3) ORDER BY id", List.of(1L, 4L));
    expect("SELECT id FROM a WHERE n NOT IN (3) ORDER BY id", List.of(1L, 4L));
    expect("SELECT id FROM a WHERE NOT (n = 1 OR id = 4) ORDER BY id", List.of(3L));
    expect("SELECT id FROM a WHERE NOT (n = 1 AND id = 1) ORDER BY id", List.of(2L, 3L, 4L));
    expect("SELECT id FROM a WHERE n NOT IN (3, NULL)", List.of());
    expect("SELECT id FROM a WHERE n IN (3, NULL)", List.of(3L));
    expect("SELECT id FROM a WHERE n + 0 <> 3 ORDER BY id", List.of(1L, 4L));
    expect("SELECT id FROM a WHERE NOT (n + 0 = 3) ORDER BY id", List.of(1L, 4L));
    expect("SELECT id FROM a WHERE NOT (n + 0 IN (1, 3)) ORDER BY id", List.of(4L));
    expect("SELECT id FROM a WHERE NOT (CAST(n AS VARCHAR) LIKE '3%') ORDER BY id", List.of(1L, 4L));
    expect("SELECT id FROM a WHERE CAST(n AS VARCHAR) NOT LIKE '3%' ORDER BY id", List.of(1L, 4L));
    expect("SELECT id FROM a WHERE n NOT BETWEEN 3 AND 4 ORDER BY id", List.of(1L));
    expect("SELECT id FROM a WHERE NOT (n < 3) ORDER BY id", List.of(3L, 4L));
    expect("SELECT id FROM a WHERE n = NULL", List.of());
    expect("SELECT id FROM a WHERE n <> NULL", List.of());
    expect("SELECT id FROM a WHERE n IS NULL", List.of(2L));
    expect("SELECT id FROM a WHERE n IS NOT NULL ORDER BY id", List.of(1L, 3L, 4L));
  }

  /** Covers each numeric operator, fractional rounding, casing, and overflow. */
  @Test
  void integralBoundsAreExact() throws Exception {
    expect("SELECT id FROM a WHERE ID = 3", List.of(3L));
    expect("SELECT id FROM a WHERE id < 2.5 ORDER BY id", List.of(1L, 2L));
    expect("SELECT id FROM a WHERE id <= 2.5 ORDER BY id", List.of(1L, 2L));
    expect("SELECT id FROM a WHERE id > 2.5 ORDER BY id", List.of(3L, 4L));
    expect("SELECT id FROM a WHERE id >= 2.5 ORDER BY id", List.of(3L, 4L));
    expect("SELECT id FROM a WHERE id = 2.5", List.of());
    expect("SELECT id FROM a WHERE id <> 2.5 ORDER BY id", List.of(1L, 2L, 3L, 4L));
    expect("SELECT id FROM a WHERE n < 2147483648 ORDER BY id", List.of(1L, 3L, 4L));
    expect("SELECT id FROM a WHERE id < 9223372036854775808 ORDER BY id", List.of(1L, 2L, 3L, 4L));
    assertTrue(ParquetFilterBuilder.build(schema, CCJSqlParserUtil.parseCondExpression("n = 'abc'")).isEmpty());
    var split = ParquetFilterBuilder.split(schema, CCJSqlParserUtil.parseCondExpression("id > 1 AND text LIKE '%3'"));
    assertTrue(split.predicate().isPresent());
    assertEquals("text LIKE '%3'", split.residual().toString());
    expect("SELECT id FROM a WHERE id > 1 AND text LIKE '%3'", List.of(3L));
    expect("SELECT DISTINCT n FROM a WHERE n > 1 ORDER BY n", List.of(3L, 4L));
    write("large", new Long[]{
        9007199254740992L, 9007199254740993L
    }, new Integer[]{
        1, 2
    });
    expect("SELECT id FROM large WHERE id = 9007199254740993.0", List.of(9007199254740993L));
    expect("SELECT id FROM a WHERE n > -2147483649 ORDER BY id", List.of(1L, 3L, 4L));
    expect("SELECT id FROM a WHERE id > -9223372036854775809 ORDER BY id", List.of(1L, 2L, 3L, 4L));
    for (String unsupported : List.of("text = UPPER('prefix-1')", "id = n + 1")) {
      assertTrue(ParquetFilterBuilder.build(schema, CCJSqlParserUtil.parseCondExpression(unsupported)).isEmpty());
    }
  }

  /**
   * Covers prefix allowlisting and the exact residual required for complex
   * patterns.
   */
  @Test
  void prefixRangesKeepTheirExactResidual() throws Exception {
    var split = ParquetFilterBuilder.split(schema, CCJSqlParserUtil.parseCondExpression("text LIKE 'prefix-%3'"));
    assertTrue(split.predicate().isPresent());
    assertNotNull(split.residual());
    for (String unsupported : List.of("text ILIKE 'prefix-%'", "n LIKE '1%'", "text LIKE '_refix%'",
        "text NOT LIKE 'prefix%'", "text LIKE 'prefix%' ESCAPE '!'")) {
      assertTrue(
          ParquetFilterBuilder.split(schema, CCJSqlParserUtil.parseCondExpression(unsupported)).predicate().isEmpty(),
          unsupported);
    }
    expect("SELECT id FROM a WHERE text LIKE 'prefix-%3'", List.of(3L));
    var or = ParquetFilterBuilder.split(schema, CCJSqlParserUtil.parseCondExpression("id = 1 OR text LIKE '%3'"));
    assertTrue(or.predicate().isEmpty());
  }

  /**
   * Confirms all nine SQL truth-table combinations and UNKNOWN Boolean
   * projections.
   */
  @Test
  void truthValuesSurviveUntilTheSelectionBoundary() throws Exception {
    Boolean[] values = {
        Boolean.TRUE, Boolean.FALSE, null
    };
    String[] expressions = {
        "1 = 1", "1 = 0", "n = 1"
    };
    var row = new GenericData.Record(schema);
    var evaluator = new ExpressionEvaluator(schema);
    var valueEvaluator = new ValueExpressionEvaluator(schema);
    for (int i = 0; i < values.length; i++) {
      assertEquals(SqlTruth.not(values[i]),
          evaluator.evalTruth(CCJSqlParserUtil.parseCondExpression("NOT (" + expressions[i] + ")"), row));
      for (int j = 0; j < values.length; j++) {
        for (String op : List.of("AND", "OR")) {
          var expression = CCJSqlParserUtil
              .parseCondExpression("(" + expressions[i] + ") " + op + " (" + expressions[j] + ")");
          Boolean expected = "AND".equals(op) ? SqlTruth.and(values[i], values[j]) : SqlTruth.or(values[i], values[j]);
          assertEquals(expected, evaluator.evalTruth(expression, row));
          assertEquals(expected, valueEvaluator.eval(expression, row));
        }
      }
    }
    expect("SELECT id FROM a WHERE CASE WHEN NOT (n = 3) THEN 1 ELSE 0 END = 1 ORDER BY id", List.of(1L, 4L));
    expect("SELECT MIN(id) FROM a GROUP BY n HAVING NOT (n = 3) ORDER BY MIN(id)", List.of(1L, 4L));
  }

  /** Exercises safe and unsafe join-side pushdown against fixed results. */
  @Test
  void joinFiltersPreserveOuterRowsAndMergedColumns() throws Exception {
    expect("SELECT a.id FROM a JOIN b ON a.id = b.id WHERE a.id > 1 ORDER BY a.id", List.of(3L));
    expect("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE b.n IS NULL ORDER BY a.id", List.of(2L, 4L));
    expect("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE b.n > 1 ORDER BY a.id", List.of(3L));
    expect("SELECT b.id FROM a RIGHT JOIN b ON a.id = b.id WHERE a.n > 1 ORDER BY b.id", List.of(3L));
    expect("SELECT id FROM a FULL JOIN b USING (id) WHERE id = 5", List.of(5L));
    expect("SELECT a.id FROM a FULL JOIN b ON a.id = b.id WHERE a.n > 1 ORDER BY a.id", List.of(3L, 4L));
    expect("SELECT a.id FROM a JOIN b ON NOT (a.n = b.n) WHERE a.id = 2", List.of());
    try (var connection = (JParqConnection) DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement("SELECT a.id FROM a JOIN b ON a.id = b.id WHERE a.id = 3")) {
      assertTrue(statement.getPushdownInfo().parquetPredicateAttached());
      assertTrue(statement.getPushdownInfo().residualFilterPresent());
      assertTrue(statement.getPushdownInfo().statisticsFilteringEnabled());
      assertTrue(statement.getPushdownInfo().columnIndexFilteringEnabled());
      assertTrue(statement.getPushdownInfo().dictionaryFilteringEnabled());
      assertTrue(statement.getPushdownInfo().bloomFilteringEnabled());
    }
    write("nullids", new Long[]{
        1L, null, null, 4L
    }, new Integer[]{
        1, 1, 3, null
    });
    expect("SELECT n FROM nullids WHERE NOT (id = 1 AND n = 1) ORDER BY n", java.util.Arrays.asList(3L, null));
  }

  /**
   * Verifies escape hatches, property precedence, and column validation when
   * projection is disabled.
   */
  @Test
  void switchesDoNotDisableColumnValidation() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("predicatePushdown", "false");
    properties.setProperty("projectionPushdown", "false");
    try (var connection = (JParqConnection) DriverManager
        .getConnection("jdbc:jparq:" + directory + "?predicatePushdown=true", properties)) {
      assertFalse(connection.isPredicatePushdown());
      assertFalse(connection.isProjectionPushdown());
      try (var statement = connection.prepareStatement("SELECT id FROM a WHERE n = 3")) {
        assertFalse(statement.getPushdownInfo().parquetPredicateAttached());
        assertTrue(statement.getPushdownInfo().message().contains("disabled by connection property"));
      }
      assertThrows(SQLException.class, () -> connection.prepareStatement("SELECT missing FROM a"));
    }
  }

  /** Tests quantified subquery truth values, including UNKNOWN under NOT. */
  @Test
  void subqueryNullsRemainUnknown() throws Exception {
    expect("SELECT id FROM a WHERE n NOT IN (SELECT n FROM b)", List.of());
    expect("SELECT id FROM a WHERE n IN (SELECT n FROM b) ORDER BY id", List.of(1L, 3L));
    expect("SELECT id FROM a WHERE NOT (n = ANY (SELECT n FROM b))", List.of());
    expect("SELECT id FROM a WHERE n <> ALL (SELECT n FROM b)", List.of());
    expect("SELECT id FROM a WHERE n = ANY (SELECT n FROM b) ORDER BY id", List.of(1L, 3L));
    expect("SELECT id FROM a WHERE n <> ALL (SELECT n FROM b WHERE id < 0) ORDER BY id", List.of(1L, 2L, 3L, 4L));
    expect("SELECT id FROM a WHERE n = ANY (SELECT n FROM b WHERE id < 0)", List.of());
  }

  /**
   * Ensures sample membership is chosen before WHERE regardless of pushdown
   * settings.
   */
  @Test
  void repeatableSamplesAreIndependentOfPushdown() throws Exception {
    String query = "SELECT id FROM a TABLESAMPLE SYSTEM (2 ROWS) REPEATABLE (42) WHERE id > 1 ORDER BY id";
    assertEquals(query(query, false), query(query, true));
    try (var connection = (JParqConnection) DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement(query)) {
      assertFalse(statement.getPushdownInfo().parquetPredicateAttached());
    }
    query = "SELECT a.id FROM a TABLESAMPLE SYSTEM (2 ROWS) REPEATABLE (42) "
        + "JOIN b ON a.id = b.id WHERE a.id > 1 ORDER BY a.id";
    assertEquals(query(query, false), query(query, true));
  }

  /**
   * Keeps sort dependencies, aliases, ordinals, and correlated scalar-subquery
   * dependencies.
   */
  @Test
  void orderByProjectionIncludesAllRequiredInputs() throws Exception {
    expect("SELECT id FROM a ORDER BY n DESC NULLS LAST", List.of(4L, 3L, 1L, 2L));
    expect("SELECT id AS number FROM a ORDER BY number DESC", List.of(4L, 3L, 2L, 1L));
    expect("SELECT id FROM a ORDER BY 1 DESC", List.of(4L, 3L, 2L, 1L));
    for (boolean projection : new boolean[]{
        true, false
    }) {
      try (
          var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?projectionPushdown=" + projection);
          var statement = connection.createStatement()) {
        for (String order : List.of("(SELECT COUNT(*) FROM b WHERE b.id < a.n)",
            "EXISTS (SELECT 1 FROM b WHERE b.id < a.n)")) {
          try (var result = statement.executeQuery("SELECT id FROM a ORDER BY " + order + ", id")) {
            for (long id : new long[]{
                1, 2, 3, 4
            }) {
              assertTrue(result.next());
              assertEquals(id, result.getLong(1));
            }
            assertFalse(result.next());
          }
        }
        try (var result = statement.executeQuery("SELECT * FROM a ORDER BY 2 DESC NULLS LAST")) {
          assertEquals(3, result.getMetaData().getColumnCount());
          for (long id : new long[]{
              4, 3, 1, 2
          }) {
            assertTrue(result.next());
            assertEquals(id, result.getLong(1));
            for (int column = 1; column <= 3; column++) {
              result.getObject(column);
            }
          }
          assertFalse(result.next());
        }
      }
    }
    expect("SELECT (SELECT b.n FROM b WHERE b.id = a.id) AS number FROM a ORDER BY number",
        java.util.Arrays.asList(1L, 3L, null, null));
  }

  /**
   * Proves both NULL operators prune pages when the file contains all-NULL pages.
   */
  @Test
  void nullChecksPrunePages() throws Exception {
    var path = new org.apache.hadoop.fs.Path(directory.resolve("nullpages.parquet").toUri());
    var configuration = new Configuration(false);
    try (var writer = AvroParquetWriter.<org.apache.avro.generic.GenericRecord>builder(
        org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(path, configuration)).withSchema(schema)
        .withPageRowCountLimit(16).withMinRowCountForPageSizeCheck(1).withMaxRowCountForPageSizeCheck(16).build()) {
      for (int i = 0; i < 1024; i++) {
        var row = new GenericData.Record(schema);
        row.put("id", (long) i);
        row.put("n", i < 256 ? null : Integer.valueOf(i));
        writer.write(row);
      }
    }
    JParqPreparedStatement.configureParquetFiltering(configuration);
    for (String condition : List.of("n IS NULL", "n IS NOT NULL")) {
      var predicate = ParquetFilterBuilder.build(schema, CCJSqlParserUtil.parseCondExpression(condition));
      var options = org.apache.parquet.HadoopReadOptions.builder(configuration, path)
          .withRecordFilter(org.apache.parquet.filter2.compat.FilterCompat.get(predicate.orElseThrow())).build();
      try (var reader = org.apache.parquet.hadoop.ParquetFileReader
          .open(org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(path, configuration), options)) {
        assertTrue(reader.getFilteredRecordCount() < reader.getRecordCount(), condition);
      }
    }
    expect("SELECT COUNT(*) FROM nullpages WHERE n IS NULL", List.of(256L));
    expect("SELECT COUNT(*) FROM nullpages WHERE n IS NOT NULL", List.of(768L));
  }
}
