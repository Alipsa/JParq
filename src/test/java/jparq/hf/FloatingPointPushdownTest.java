package jparq.hf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqPreparedStatement;
import se.alipsa.jparq.engine.ParquetFilterBuilder;
import se.alipsa.jparq.engine.ParquetSchemas;

/**
 * Verifies one floating-point order (-Infinity, finite values, +Infinity, NaN
 * with -0.0 equal to 0.0) with predicate pushdown on and off.
 *
 * <p>
 * The fixtures are written by PyArrow, which leaves NaN out of row-group
 * statistics and page-index min/max. parquet-java would hide that behavior.
 * </p>
 * <ul>
 * <li>{@code float_groups}: ids 1..8, d = f = 1, NaN, 2, 3 | NaN, NaN, 500,
 * 4</li>
 * <li>{@code float_special}: ids 1..6, d = f = -0.0, 0.0, NaN, +Inf, -Inf,
 * 1.0</li>
 * <li>{@code float_pages}: ids 1..2048, d = f = NaN when (id - 1) is a multiple
 * of five, otherwise id - 1</li>
 * </ul>
 */
class FloatingPointPushdownTest {

  /** A query on one fixture with the ids it must return. */
  private record Case(String table, String where, List<Long> expected) {
    @Override
    public String toString() {
      return table + ": " + where;
    }
  }

  private static List<Long> ids(long... values) {
    List<Long> list = new ArrayList<>();
    for (long value : values) {
      list.add(value);
    }
    return list;
  }

  private static List<Long> range(long from, long to) {
    List<Long> list = new ArrayList<>();
    for (long value = from; value <= to; value++) {
      list.add(value);
    }
    return list;
  }

  /** A single case on the named table. */
  private static Stream<Case> one(String table, String where, List<Long> expected) {
    return Stream.of(new Case(table, where, expected));
  }

  /** The d column (DOUBLE) and the f column (FLOAT) must behave identically. */
  private static Stream<Case> both(String table, String where, List<Long> expected) {
    return Stream.of(new Case(table, where, expected), new Case(table, where.replaceAll("\\bd\\b", "f"), expected));
  }

  /**
   * Supplies all operators on the NaN row groups.
   *
   * @return the cases
   */
  static Stream<Case> groupCases() {
    String t = "float_groups";
    return Stream.of(both(t, "d > 3", ids(2, 5, 6, 7, 8)), both(t, "d > 100", ids(2, 5, 6, 7)),
        both(t, "100 < d", ids(2, 5, 6, 7)), both(t, "d >= 500", ids(2, 5, 6, 7)),
        both(t, "d >= 4", ids(2, 5, 6, 7, 8)), both(t, "d <> 1", ids(2, 3, 4, 5, 6, 7, 8)),
        both(t, "NOT (d < 5)", ids(2, 5, 6, 7)), both(t, "NOT (d <= 3)", ids(2, 5, 6, 7, 8)),
        both(t, "NOT (d = 1)", ids(2, 3, 4, 5, 6, 7, 8)), both(t, "d < 3", ids(1, 3)), both(t, "d <= 3", ids(1, 3, 4)),
        both(t, "d = 3", ids(4)), both(t, "d = 1", ids(1)), both(t, "d IN (1.0, 2.0)", ids(1, 3)),
        both(t, "d NOT IN (1.0, 2.0)", ids(2, 4, 5, 6, 7, 8)), both(t, "d BETWEEN 2 AND 4", ids(3, 4, 8)),
        both(t, "d NOT BETWEEN 2 AND 4", ids(1, 2, 5, 6, 7)), both(t, "d > 3 AND id > 0", ids(2, 5, 6, 7, 8)),
        both(t, "d > 100 AND id >= 5", ids(5, 6, 7)), both(t, "d > 100 OR id = 1", ids(1, 2, 5, 6, 7)),
        both(t, "d < 2 OR d > 100", ids(1, 2, 5, 6, 7)), both(t, "d IS NULL", ids()),
        both(t, "d IS NOT NULL", range(1, 8))).flatMap(stream -> stream);
  }

  /**
   * Supplies infinities, signed zeros and NaN in one table.
   *
   * @return the cases
   */
  static Stream<Case> specialCases() {
    String t = "float_special";
    return Stream.of(both(t, "d = 0", ids(1, 2)), both(t, "d = 0.0", ids(1, 2)), both(t, "d = -0.0", ids(1, 2)),
        both(t, "0 = d", ids(1, 2)), both(t, "d <> 0", ids(3, 4, 5, 6)), both(t, "d < 0", ids(5)),
        both(t, "d <= 0", ids(1, 2, 5)), both(t, "d > 0", ids(3, 4, 6)), both(t, "d >= 0", ids(1, 2, 3, 4, 6)),
        both(t, "d >= -0.0", ids(1, 2, 3, 4, 6)), both(t, "d < -0.0", ids(5)), both(t, "d = 1", ids(6)),
        both(t, "d < 1", ids(1, 2, 5)), both(t, "d <= 1", ids(1, 2, 5, 6)), both(t, "d > 1", ids(3, 4)),
        both(t, "d >= 1", ids(3, 4, 6)), both(t, "d > 1e30", ids(3, 4)), both(t, "d < 1e30", ids(1, 2, 5, 6)),
        both(t, "d > -1e30", ids(1, 2, 3, 4, 6)), both(t, "d IN (0, 1)", ids(1, 2, 6)),
        both(t, "d NOT IN (0, 1)", ids(3, 4, 5)), both(t, "d BETWEEN -1 AND 1", ids(1, 2, 6)),
        both(t, "d BETWEEN 0 AND 1", ids(1, 2, 6)), both(t, "d NOT BETWEEN 0 AND 1", ids(3, 4, 5)),
        both(t, "d IS DISTINCT FROM d", ids()), both(t, "d IS NOT DISTINCT FROM d", range(1, 6)),
        one(t, "f < 1.00000000001", ids(1, 2, 5, 6)), one(t, "f > 0.99999999999", ids(3, 4, 6)),
        one(t, "f = 1.00000000001", ids()), one(t, "d = d", range(1, 6)),
        one(t, "d > ALL (SELECT x FROM (VALUES (1.0)) AS v(x))", ids(3, 4)),
        one(t, "d >= ANY (SELECT x FROM (VALUES (1.0)) AS v(x))", ids(3, 4, 6))).flatMap(stream -> stream);
  }

  /**
   * Supplies a multi-page file where each page mixes NaN with values.
   *
   * @return the cases
   */
  static Stream<Case> pageCases() {
    String t = "float_pages";
    return Stream.of(
        both(t, "d < 40",
            ids(2, 3, 4, 5, 7, 8, 9, 10, 12, 13, 14, 15, 17, 18, 19, 20, 22, 23, 24, 25, 27, 28, 29, 30, 32, 33, 34, 35,
                37, 38, 39, 40)),
        both(t, "d = 1001", ids(1002)), both(t, "d IN (1001, 1002)", ids(1002, 1003)),
        both(t, "d BETWEEN 100 AND 110", ids(102, 103, 104, 105, 107, 108, 109, 110)),
        both(t, "d <= 3 AND id > 0", ids(2, 3, 4))).flatMap(stream -> stream);
  }

  /**
   * Supplies counts that are too large to list.
   *
   * @return count queries with their expected values
   */
  static Stream<Case> countCases() {
    String t = "float_pages";
    return Stream.of(both(t, "d > 100", ids(1968)), both(t, "d >= 2000", ids(448)), both(t, "d <> 6", ids(2047)),
        both(t, "d > 3 AND id > 0", ids(2045)), both(t, "d NOT BETWEEN 100 AND 110", ids(2040)),
        both(t, "NOT (d < 100)", ids(1968)), both(t, "d IS NULL", ids(0))).flatMap(stream -> stream);
  }

  private static String directory() throws Exception {
    return java.nio.file.Path.of(FloatingPointPushdownTest.class.getResource("/hf").toURI()).toString();
  }

  private static List<Long> query(String sql, boolean pushdown) throws Exception {
    List<Long> rows = new ArrayList<>();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory() + "?predicatePushdown=" + pushdown);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      while (result.next()) {
        rows.add(result.getLong(1));
      }
    }
    return rows;
  }

  private static void assertBoth(String sql, List<Long> expected) throws Exception {
    assertEquals(expected, query(sql, true), sql + " with pushdown");
    assertEquals(expected, query(sql, false), sql + " without pushdown");
  }

  /**
   * Checks row ids against fixed values with pushdown on and off.
   *
   * @param testCase
   *          the table, condition and expected ids
   * @throws Exception
   *           if a query fails
   */
  @ParameterizedTest
  @MethodSource({
      "groupCases", "specialCases", "pageCases"
  })
  void rowsFollowTheSqlFloatingPointOrder(Case testCase) throws Exception {
    assertBoth("SELECT id FROM " + testCase.table() + " WHERE " + testCase.where() + " ORDER BY id",
        testCase.expected());
  }

  /**
   * Checks row counts with pushdown on and off.
   *
   * @param testCase
   *          the table, condition and expected count (as a single element)
   * @throws Exception
   *           if a query fails
   */
  @ParameterizedTest
  @MethodSource("countCases")
  void countsFollowTheSqlFloatingPointOrder(Case testCase) throws Exception {
    assertBoth("SELECT COUNT(*) FROM " + testCase.table() + " WHERE " + testCase.where(), testCase.expected());
  }

  /**
   * Sorting uses the same order and puts NaN last.
   *
   * @throws Exception
   *           if a query fails
   */
  @Test
  void orderByPlacesNaNLastAndMergesSignedZeros() throws Exception {
    assertBoth("SELECT id FROM float_groups ORDER BY d, id", ids(1, 3, 4, 8, 7, 2, 5, 6));
    assertBoth("SELECT id FROM float_groups ORDER BY d DESC, id", ids(2, 5, 6, 7, 8, 4, 3, 1));
    assertBoth("SELECT id FROM float_groups ORDER BY f, id", ids(1, 3, 4, 8, 7, 2, 5, 6));
    assertBoth("SELECT id FROM float_special ORDER BY d, id", ids(5, 1, 2, 6, 4, 3));
    assertBoth("SELECT id FROM float_special ORDER BY f DESC, id", ids(3, 4, 6, 1, 2, 5));
    assertBoth("SELECT id FROM float_special WHERE d > 0 ORDER BY d, id", ids(6, 4, 3));
    assertBoth("SELECT id FROM float_pages ORDER BY d DESC, id LIMIT 3", ids(1, 6, 11));
    assertBoth("SELECT id FROM float_pages ORDER BY d, id LIMIT 3", ids(2, 3, 4));
  }

  /**
   * MIN and MAX use the same order, so NaN is the maximum.
   *
   * @throws Exception
   *           if a query fails
   */
  @Test
  void minAndMaxUseTheSqlOrder() throws Exception {
    for (boolean pushdown : new boolean[]{
        true, false
    }) {
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory() + "?predicatePushdown=" + pushdown);
          var statement = connection.createStatement()) {
        try (var result = statement.executeQuery("SELECT MIN(d), MAX(d), MIN(f), MAX(f) FROM float_special")) {
          assertTrue(result.next());
          assertEquals(Double.NEGATIVE_INFINITY, result.getDouble(1));
          assertTrue(Double.isNaN(result.getDouble(2)));
          assertEquals(Float.NEGATIVE_INFINITY, result.getFloat(3));
          assertTrue(Float.isNaN(result.getFloat(4)));
        }
        try (var result = statement.executeQuery("SELECT MIN(d), MAX(d) FROM float_groups")) {
          assertTrue(result.next());
          assertEquals(1.0, result.getDouble(1));
          assertTrue(Double.isNaN(result.getDouble(2)));
        }
        try (var result = statement.executeQuery("SELECT MAX(d) FROM float_groups WHERE d < 100")) {
          assertTrue(result.next());
          assertEquals(4.0, result.getDouble(1));
        }
        try (var result = statement.executeQuery("SELECT MIN(d) FROM float_groups WHERE d > 100")) {
          assertTrue(result.next());
          assertEquals(500.0, result.getDouble(1));
        }
      }
    }
  }

  /**
   * Set operations and window functions order values the same way.
   *
   * @throws Exception
   *           if a query fails
   */
  @Test
  void setOperationsAndWindowsUseTheSqlOrder() throws Exception {
    assertBoth("SELECT id, d FROM float_special UNION SELECT id, d FROM float_special ORDER BY d, id",
        ids(5, 1, 2, 6, 4, 3));
    assertBoth(
        "SELECT id FROM (SELECT id, ROW_NUMBER() OVER (ORDER BY d) AS rn FROM float_special) AS w " + "ORDER BY rn",
        ids(5, 1, 2, 6, 4, 3));
  }

  /**
   * Counts the rows left after the production Parquet filter has pruned pages and
   * row groups.
   */
  private long filteredCount(String table, String condition) throws Exception {
    Path path = new Path(getClass().getResource("/hf/" + table + ".parquet").toURI());
    Configuration configuration = new Configuration(false);
    JParqPreparedStatement.configureParquetFiltering(configuration);
    var schema = ParquetSchemas.readAvroSchema(path, configuration);
    var predicate = ParquetFilterBuilder.split(schema, CCJSqlParserUtil.parseCondExpression(condition)).predicate();
    assertTrue(predicate.isPresent(), condition);
    var options = HadoopReadOptions.builder(configuration, path).withRecordFilter(FilterCompat.get(predicate.get()))
        .build();
    try (var reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, configuration), options)) {
      return reader.getFilteredRecordCount();
    }
  }

  private static ParquetFilterBuilder.PushdownSplit split(String table, String condition) throws Exception {
    Path path = new Path(FloatingPointPushdownTest.class.getResource("/hf/" + table + ".parquet").toURI());
    var schema = ParquetSchemas.readAvroSchema(path, new Configuration(false));
    return ParquetFilterBuilder.split(schema, CCJSqlParserUtil.parseCondExpression(condition));
  }

  /**
   * Confirms that the fixture has enough indexed pages for pruning to be
   * observable and that PyArrow left NaN out of the statistics.
   *
   * @throws Exception
   *           if the file cannot be read
   */
  @Test
  void fixtureHasIndexedPagesWithoutNaNStatistics() throws Exception {
    Path path = new Path(getClass().getResource("/hf/float_pages.parquet").toURI());
    try (var reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration(false)))) {
      assertEquals(2048, reader.getRecordCount());
      assertEquals(2, reader.getFooter().getBlocks().size());
      for (var group : reader.getFooter().getBlocks()) {
        for (var column : group.getColumns()) {
          assertTrue(column.getColumnIndexReference() != null, column.getPath().toDotString());
          if ("d".equals(column.getPath().toDotString())) {
            assertTrue(reader.readOffsetIndex(column).getPageCount() >= 16);
            assertFalse(Double.isNaN((Double) column.getStatistics().genericGetMax()));
          }
        }
      }
    }
    Path groups = new Path(getClass().getResource("/hf/float_groups.parquet").toURI());
    try (var reader = ParquetFileReader.open(HadoopInputFile.fromPath(groups, new Configuration(false)))) {
      assertEquals(3.0, reader.getFooter().getBlocks().getFirst().getColumns().get(1).getStatistics().genericGetMax());
    }
  }

  /**
   * NaN-safe operators still prune pages; the others are Java residuals.
   *
   * @throws Exception
   *           if planning fails
   */
  @Test
  void nanSafeOperatorsSkipPagesAndTheRestAreResiduals() throws Exception {
    for (String column : List.of("d", "f")) {
      for (String condition : List.of(column + " < 40", column + " <= 40", column + " = 1001",
          column + " IN (1001, 1002)", column + " BETWEEN 100 AND 110", column + " < 40 AND id > 0",
          column + " > 3 AND id < 100")) {
        long filtered = filteredCount("float_pages", condition);
        assertTrue(filtered > 0 && filtered <= 128, condition + " kept " + filtered);
      }
      for (String condition : List.of(column + " > 100", column + " >= 100", column + " <> 6", column + " NOT IN (6)",
          column + " NOT BETWEEN 1 AND 5", "NOT (" + column + " < 5)", column + " = 0", column + " < 0.0",
          column + " <= -0.0", column + " BETWEEN 0 AND 5", column + " IN (0, 1)", column + " = 0.10000000000000001")) {
        var split = split("float_pages", condition);
        assertTrue(split.predicate().isEmpty(), condition);
        assertTrue(split.residual() != null, condition);
      }
    }
    var mixed = split("float_pages", "d > 3 AND id > 0");
    assertEquals("d > 3", mixed.residual().toString());
    assertTrue(mixed.predicate().isPresent());
    assertNull(split("float_pages", "d < 3 AND id > 0").residual());
  }

  /**
   * The residual of an unsafe comparison is reported by the prepared statement.
   *
   * @throws Exception
   *           if planning fails
   */
  @Test
  void pushdownInfoReportsFloatingPointResiduals() throws Exception {
    try (var connection = (JParqConnection) DriverManager.getConnection("jdbc:jparq:" + directory());
        var statement = connection.prepareStatement("SELECT id FROM float_pages WHERE d > 100")) {
      var info = statement.getPushdownInfo();
      assertFalse(info.parquetPredicateAttached());
      assertEquals("d > 100", info.residualExpressionSummary());
    }
    try (var connection = (JParqConnection) DriverManager.getConnection("jdbc:jparq:" + directory());
        var statement = connection.prepareStatement("SELECT id FROM float_pages WHERE d < 100")) {
      var info = statement.getPushdownInfo();
      assertTrue(info.parquetPredicateAttached());
      assertFalse(info.residualFilterPresent());
    }
  }
}
