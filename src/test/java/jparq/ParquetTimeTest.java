package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import se.alipsa.jparq.engine.ParquetFilterBuilder;
import se.alipsa.jparq.engine.ParquetSchemas;

/**
 * Regression coverage for nullable Parquet TIME columns reported in issue 251.
 */
class ParquetTimeTest {

  /**
   * Verifies TIME metadata, decoded objects, getters, casts, and SQL NULLs.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if a query fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void decodesTimeColumns(boolean predicate, boolean projection) throws Exception {
    try (var connection = connection(predicate, projection)) {
      for (String column : List.of("t", "u")) {
        try (var statement = connection.createStatement();
            var result = statement.executeQuery(
                "SELECT " + column + ", CAST(" + column + " AS VARCHAR) AS text FROM typed ORDER BY id")) {
          assertTimeMetadata(result);
          for (String clock : List.of("10:00:00", "11:00:00")) {
            assertTrue(result.next());
            Time expected = Time.valueOf(clock);
            assertEquals(expected, assertInstanceOf(Time.class, result.getObject(1)));
            assertEquals(expected, result.getObject(column));
            assertEquals(expected, result.getTime(1));
            assertEquals(expected, result.getTime(column));
            assertEquals(clock, result.getString(1));
            assertEquals(clock, result.getString("text"));
            assertFalse(result.wasNull());
          }
          assertTrue(result.next());
          assertNull(result.getObject(1));
          assertTrue(result.wasNull());
          assertNull(result.getTime(column));
          assertTrue(result.wasNull());
          assertNull(result.getString("text"));
          assertTrue(result.wasNull());
          assertFalse(result.next());
        }
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT " + column + " FROM typed WHERE id < 0")) {
          assertTimeMetadata(result);
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Records that TIME literal comparisons currently require residual evaluation,
   * while eligible integer conjuncts can still be pushed down.
   *
   * @throws Exception
   *           if reading the fixture schema or parsing a predicate fails
   */
  @Test
  void timeLiteralComparisonsRemainResidual() throws Exception {
    var path = new org.apache.hadoop.fs.Path(ParquetTimeTest.class.getResource("/time/typed.parquet").toURI());
    var schema = ParquetSchemas.readAvroSchema(path, new Configuration(false));
    for (String column : List.of("t", "u")) {
      for (String condition : List.of(" = TIME '10:00:00'", " > TIME '12:00:00'", " < {t '10:30:00'}")) {
        var expression = CCJSqlParserUtil.parseCondExpression(column + condition);
        var split = ParquetFilterBuilder.split(schema, expression);
        assertTrue(split.predicate().isEmpty(), expression::toString);
        assertNotNull(split.residual());
        assertEquals(expression.toString(), split.residual().toString());
        var mixed = ParquetFilterBuilder.split(schema,
            CCJSqlParserUtil.parseCondExpression("id = 1 AND " + column + condition));
        assertTrue(mixed.predicate().isPresent());
        assertEquals("eq(id, 1)", mixed.predicate().orElseThrow().toString());
        assertNotNull(mixed.residual());
        assertEquals(expression.toString(), mixed.residual().toString());
      }
    }
  }

  /**
   * Checks the reported literal comparisons and ordering using clock semantics.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if a query fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void comparesAndOrdersTimeColumns(boolean predicate, boolean projection) throws Exception {
    try (var connection = connection(predicate, projection)) {
      for (String column : List.of("t", "u")) {
        for (String condition : List.of(" = TIME '10:00:00'", " = {t '10:00:00'}", " < TIME '10:30:00'",
            " BETWEEN TIME '09:00:00' AND TIME '10:30:00'", " IN (TIME '10:00:00', TIME '12:00:00')")) {
          assertEquals(List.of(1), ids(connection, "SELECT id FROM typed WHERE " + column + condition));
        }
        assertEquals(List.of(), ids(connection, "SELECT id FROM typed WHERE " + column + " > TIME '12:00:00'"));
        assertEquals(List.of(2), ids(connection, "SELECT id FROM typed WHERE " + column + " <> TIME '10:00:00'"));
        assertEquals(List.of(3), ids(connection, "SELECT id FROM typed WHERE " + column + " IS NULL"));
        assertEquals(List.of(1, 2, 3), ids(connection, "SELECT id FROM typed ORDER BY " + column + " ASC NULLS LAST"));
        assertEquals(List.of(3, 2, 1),
            ids(connection, "SELECT id FROM typed ORDER BY " + column + " DESC NULLS FIRST"));
      }
    }
  }

  /**
   * Verifies metadata agrees with the decoded JDBC object class.
   *
   * @param result
   *          query result, possibly empty
   * @throws Exception
   *           if reading metadata fails
   */
  private void assertTimeMetadata(ResultSet result) throws Exception {
    var metadata = result.getMetaData();
    assertEquals(Types.TIME, metadata.getColumnType(1));
    assertEquals("TIME", metadata.getColumnTypeName(1));
    assertEquals(Time.class.getName(), metadata.getColumnClassName(1));
  }

  /**
   * Collects the IDs returned by a query in result order.
   *
   * @param connection
   *          test connection
   * @param sql
   *          query to execute
   * @return returned IDs
   * @throws Exception
   *           if the query fails
   */
  private List<Integer> ids(Connection connection, String sql) throws Exception {
    List<Integer> ids = new ArrayList<>();
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      while (result.next()) {
        ids.add(result.getInt(1));
      }
    }
    return ids;
  }

  /**
   * Opens the committed fixture with explicit pushdown settings.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @return test connection
   * @throws Exception
   *           if opening the connection fails
   */
  private Connection connection(boolean predicate, boolean projection) throws Exception {
    Path directory = Path.of(ParquetTimeTest.class.getResource("/time/typed.parquet").toURI()).getParent();
    return DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection);
  }
}
