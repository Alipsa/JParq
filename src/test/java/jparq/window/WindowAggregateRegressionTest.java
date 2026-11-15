package jparq.window;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers window aggregate metadata and value conversions across expression
 * wrappers.
 */
class WindowAggregateRegressionTest {
  @TempDir
  Path directory;

  /**
   * Keeps mixed datetime operands executable both directly and after
   * materialization.
   *
   * @param function
   *          MIN or MAX
   * @throws SQLException
   *           if execution or metadata access fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "MIN", "MAX"
  })
  void mixedDatetimeCase(String function) throws SQLException {
    String query = "SELECT " + function + """
        (CASE WHEN id > 1 THEN CAST('2020-01-01' AS DATE)
              ELSE CAST('2020-01-01 10:00:00' AS TIMESTAMP) END) OVER () AS v
        FROM (VALUES (1),(2)) t(id)
        """;
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (boolean derived : List.of(false, true)) {
        try (var result = statement.executeQuery(derived ? "SELECT v FROM (" + query + ") d" : query)) {
          var metadata = result.getMetaData();
          assertEquals(derived ? "MIN".equals(function) ? Types.DATE : Types.TIMESTAMP : Types.OTHER,
              metadata.getColumnType(1));
          assertNotNull(metadata.getColumnClassName(1));
          assertDoesNotThrow(() -> metadata.getPrecision(1));
          assertDoesNotThrow(() -> metadata.getScale(1));
          int rows = 0;
          while (result.next()) {
            assertEquals(Timestamp.valueOf("MAX".equals(function) ? "2020-01-01 10:00:00" : "2020-01-01 00:00:00"),
                result.getTimestamp(1));
            rows++;
          }
          assertEquals(2, rows);
        }
      }
    }
  }

  /**
   * Supplies independent expectations for arithmetic and common numeric types.
   *
   * @return expression, JDBC type, precision, scale, and expected row values
   */
  static Stream<Arguments> numericWrappers() {
    return Stream.of(Arguments.of("SUM(id) OVER () + 1", Types.BIGINT, 19, 0, List.of(4L, 4L)),
        Arguments.of("SUM(id) OVER () / 2", Types.DECIMAL, -1, -1,
            List.of(new BigDecimal("1.5"), new BigDecimal("1.5"))),
        Arguments.of("SUM(CAST(id AS DECIMAL(8,2))) OVER () * 2", Types.DECIMAL, 28, 2,
            List.of(new BigDecimal("6.00"), new BigDecimal("6.00"))),
        Arguments.of("CASE WHEN id > 1 THEN SUM(id) OVER () ELSE 1.5 END", Types.DECIMAL, 20, 1,
            List.of(new BigDecimal("1.5"), new BigDecimal("3.0"))),
        Arguments.of("COALESCE(SUM(id) OVER (), 0)", Types.BIGINT, 19, 0, List.of(3L, 3L)));
  }

  /**
   * Checks values, Java classes, and metadata before and after materialization.
   *
   * @param expression
   *          wrapped window expression
   * @param type
   *          expected JDBC type
   * @param precision
   *          expected precision, or -1 for value-inferred bounds
   * @param scale
   *          expected scale, or -1 for value-inferred bounds
   * @param expected
   *          expected output values
   * @throws SQLException
   *           if execution fails
   */
  @ParameterizedTest
  @MethodSource("numericWrappers")
  void numericWrapperValues(String expression, int type, int precision, int scale, List<Object> expected)
      throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      String query = "SELECT " + expression + " AS v FROM (VALUES (1),(2)) t(id)";
      for (boolean derived : List.of(false, true)) {
        try (var result = statement.executeQuery(derived ? "SELECT v FROM (" + query + ") d" : query)) {
          var metadata = result.getMetaData();
          assertEquals(type, metadata.getColumnType(1));
          assertEquals(expected.getFirst().getClass().getName(), metadata.getColumnClassName(1));
          if (precision >= 0) {
            assertEquals(precision, metadata.getPrecision(1));
          }
          if (scale >= 0) {
            assertEquals(scale, metadata.getScale(1));
          }
          for (Object value : expected) {
            assertTrue(result.next());
            assertEquals(value, result.getObject(1));
          }
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Retains COALESCE's numeric type when an empty frame selects its fallback.
   *
   * @throws SQLException
   *           if execution or metadata access fails
   */
  @Test
  void coalesceEmptyFrame() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("""
            SELECT COALESCE(SUM(id) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING), 0) AS id
            FROM (VALUES (1), (2)) t(id)
            """)) {
      assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1));
      assertEquals(Long.class.getName(), result.getMetaData().getColumnClassName(1));
      for (long value : List.of(2L, 0L)) {
        assertTrue(result.next());
        assertEquals(value, result.getObject(1));
      }
      assertFalse(result.next());
    }
  }

  /**
   * Widens a window aggregate and decimal UNION branch to the same exact type.
   *
   * @throws SQLException
   *           if execution fails
   */
  @Test
  void unionWindowAndDecimal() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("""
            SELECT SUM(id) OVER () AS v FROM (VALUES (1),(2)) t(id)
            UNION ALL SELECT 1.5
            """)) {
      assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
      assertEquals(BigDecimal.class.getName(), result.getMetaData().getColumnClassName(1));
      assertEquals(20, result.getMetaData().getPrecision(1));
      assertEquals(1, result.getMetaData().getScale(1));
      for (String value : List.of("3.0", "3.0", "1.5")) {
        assertTrue(result.next());
        assertEquals(new BigDecimal(value), result.getObject(1));
      }
      assertFalse(result.next());
    }
  }

  /**
   * Makes empty-frame nullability independent of aliases and REQUIRED Parquet
   * fields.
   *
   * @throws Exception
   *           if writing or querying the fixture fails
   */
  @Test
  void requiredParquetInputsAndEmptyFrames() throws Exception {
    var schema = SchemaBuilder.record("required_windows").fields().requiredInt("id").requiredString("name").endRecord();
    var configuration = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("required.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      var row = new GenericData.Record(schema);
      row.put("id", 1);
      row.put("name", "Alice");
      writer.write(row);
    }
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      try (var source = statement.executeQuery("SELECT id, name FROM required")) {
        assertEquals(ResultSetMetaData.columnNoNulls, source.getMetaData().isNullable(1));
        assertEquals(ResultSetMetaData.columnNoNulls, source.getMetaData().isNullable(2));
      }
      for (String function : List.of("COUNT", "SUM", "AVG", "MIN", "MAX")) {
        for (String column : "MIN".equals(function) || "MAX".equals(function) ? List.of("id", "name") : List.of("id")) {
          for (String alias : List.of(column, "renamed")) {
            try (var result = statement.executeQuery("SELECT " + function + "(" + column + ") OVER (ORDER BY id "
                + "ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS " + alias + " FROM required")) {
              assertEquals(
                  "COUNT".equals(function) ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable,
                  result.getMetaData().isNullable(1));
              assertTrue(result.next());
              assertEquals("COUNT".equals(function) ? 0L : null, result.getObject(1));
              assertFalse(result.next());
            }
          }
        }
      }
    }
  }

  /**
   * Preserves narrow integer operands for MIN/MAX while widening SUM to BIGINT.
   *
   * @param operandType
   *          TINYINT or SMALLINT
   * @throws SQLException
   *           if query execution or metadata access fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "TINYINT", "SMALLINT"
  })
  void narrowIntegerOperands(String operandType) throws SQLException {
    int type = "TINYINT".equals(operandType) ? Types.TINYINT : Types.SMALLINT;
    Class<?> valueClass = "TINYINT".equals(operandType) ? Byte.class : Short.class;
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (boolean empty : List.of(false, true)) {
        String operand = "CAST(id AS " + operandType + ")";
        try (var result = statement
            .executeQuery("SELECT MIN(" + operand + ") OVER () AS mn, MAX(" + operand + ") OVER () AS mx, SUM("
                + operand + ") OVER () AS total FROM (VALUES (1), (2)) t(id)" + (empty ? " WHERE id < 0" : ""))) {
          for (int column = 1; column <= 2; column++) {
            assertEquals(type, result.getMetaData().getColumnType(column));
            assertEquals(valueClass.getName(), result.getMetaData().getColumnClassName(column));
          }
          assertEquals(Types.BIGINT, result.getMetaData().getColumnType(3));
          assertEquals(Long.class.getName(), result.getMetaData().getColumnClassName(3));
          int rows = 0;
          while (result.next()) {
            assertInstanceOf(valueClass, result.getObject(1));
            assertInstanceOf(valueClass, result.getObject(2));
            assertEquals(1, ((Number) result.getObject(1)).intValue());
            assertEquals(2, ((Number) result.getObject(2)).intValue());
            assertEquals(3L, result.getObject(3));
            rows++;
          }
          assertEquals(empty ? 0 : 2, rows);
        }
      }
    }
  }

  /**
   * Resolves aggregate types against stored Parquet column declarations.
   *
   * @throws Exception
   *           if the fixture cannot be opened or queried
   */
  @Test
  void mtcarsAggregateTypes() throws Exception {
    Path data = Path.of(getClass().getResource("/mtcars.parquet").toURI()).getParent();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + data);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT SUM(cyl) OVER () AS cyl, MAX(mpg) OVER () AS mpg FROM mtcars")) {
      assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1));
      assertEquals(Long.class.getName(), result.getMetaData().getColumnClassName(1));
      assertEquals(Types.DOUBLE, result.getMetaData().getColumnType(2));
      assertEquals(Double.class.getName(), result.getMetaData().getColumnClassName(2));
      assertEquals(ResultSetMetaData.columnNullable, result.getMetaData().isNullable(1));
      assertEquals(ResultSetMetaData.columnNullable, result.getMetaData().isNullable(2));
      assertTrue(result.next());
      assertInstanceOf(Long.class, result.getObject(1));
      assertEquals(33.9, result.getObject(2));
    }
  }
}
