package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.apache.avro.LogicalTypes;
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
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies declared arithmetic types and Java values at JDBC boundaries (#232).
 */
class ArithmeticResultTypeTest {
  @TempDir
  Path directory;

  /**
   * Writes nullable integer, long, approximate, and exact decimal operands.
   *
   * @throws Exception
   *           if writing the fixture fails
   */
  @BeforeEach
  void writeOperands() throws Exception {
    Schema decimal = LogicalTypes.decimal(5, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema schema = SchemaBuilder.record("operands").fields().requiredInt("id").optionalInt("n").requiredLong("l")
        .requiredDouble("d").requiredFloat("r").name("dec").type(decimal).noDefault().endRecord();
    Configuration configuration = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("o.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int id = 0; id < 3; id++) {
        var record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("n", id == 0 ? 7 : null);
        record.put("l", Long.MAX_VALUE);
        record.put("d", id == 2 ? Double.NaN : 0.1);
        record.put("r", id == 2 ? Float.POSITIVE_INFINITY : 0.5f);
        record.put("dec", ByteBuffer.wrap(new BigDecimal("999.99").unscaledValue().toByteArray()));
        writer.write(record);
      }
    }
    Schema keys = SchemaBuilder.record("keys").fields().requiredInt("id").requiredLong("l").endRecord();
    var keyOutput = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("keys.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(keyOutput).withSchema(keys).withConf(configuration)
        .build()) {
      for (int id = 0; id < 3; id++) {
        var record = new GenericData.Record(keys);
        record.put("id", id);
        record.put("l", id == 0 ? 5L : id == 1 ? 9L : 99L);
        writer.write(record);
      }
    }
  }

  /**
   * Checks each numeric family through direct, derived, CTE, and set-derived
   * outputs.
   *
   * @param pushdown
   *          whether both pushdown settings are enabled
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void numericFamilies(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      check(connection, "n * 2", Types.BIGINT, 14L);
      check(connection, "n + 1", Types.BIGINT, 8L);
      check(connection, "n - 1", Types.BIGINT, 6L);
      check(connection, "-n", Types.BIGINT, -7L);
      check(connection, "n % 3", Types.BIGINT, 1L);
      check(connection, "2 * 3", Types.BIGINT, 6L);
      check(connection, "CAST(n AS BIGINT) * 2", Types.BIGINT, 14L);
      check(connection, "CAST(n AS SMALLINT) * CAST(2 AS SMALLINT)", Types.BIGINT, 14L);
      check(connection, "CAST(2147483647 AS INTEGER) * CAST(2147483647 AS INTEGER)", Types.BIGINT,
          4611686014132420609L);
      check(connection, "d * 3", Types.DOUBLE, 0.30000000000000004);
      check(connection, "d + 0.2", Types.DOUBLE, 0.30000000000000004);
      check(connection, "r + r", Types.DOUBLE, 1.0);
      check(connection, "-r", Types.DOUBLE, -0.5);
      check(connection, "d / 2", Types.DOUBLE, 0.05);
      check(connection, "n * 1.5", Types.DECIMAL, new BigDecimal("10.5"));
      check(connection, "7 / 2", Types.DECIMAL, new BigDecimal("3.5"));
      check(connection, "1 / 3", Types.DECIMAL, new BigDecimal("0.3333333333333333"));
      check(connection, "n / 0", Types.DECIMAL, null);
      check(connection, "n % 0", Types.BIGINT, null);
      check(connection, "NULL + n", Types.BIGINT, null);
      check(connection, "n * NULL", Types.BIGINT, null);
      check(connection, "(NULL) + n", Types.BIGINT, null);
      check(connection, "SUM(n) * 2", Types.BIGINT, 14L);
      check(connection, "COUNT(*) + 1", Types.BIGINT, 2L);
      check(connection, "SUM(d) * 3", Types.DOUBLE, 0.30000000000000004);
      check(connection, "SUM(dec) * 2", Types.DECIMAL, new BigDecimal("1999.98"));
      check(connection, "AVG(n) + 1", Types.DOUBLE, 8.0);
      check(connection, "CAST(d AS DECIMAL) * 2", Types.DECIMAL, new BigDecimal("0.2"));
      check(connection, "1.5", Types.DECIMAL, new BigDecimal("1.5"));
      check(connection, "2147483648", Types.BIGINT, 2147483648L);
      check(connection, "-9223372036854775808", Types.BIGINT, Long.MIN_VALUE);
      check(connection, "1", Types.INTEGER, 1);
    }
  }

  /**
   * Retains the declared Java type of narrow casts used as arithmetic operands.
   *
   * @param pushdown
   *          whether both pushdown settings are enabled
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void narrowCastOperands(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT CAST(n AS SMALLINT), CAST(n AS TINYINT), "
            + "CAST(n AS SMALLINT) + CAST(n AS TINYINT) FROM o WHERE id = 0")) {
      assertEquals(Types.SMALLINT, result.getMetaData().getColumnType(1));
      assertEquals(Types.TINYINT, result.getMetaData().getColumnType(2));
      assertEquals(Types.BIGINT, result.getMetaData().getColumnType(3));
      assertTrue(result.next());
      assertEquals((short) 7, result.getObject(1));
      assertEquals((byte) 7, result.getObject(2));
      assertEquals(14L, result.getObject(3));
      assertEquals(Short.class.getName(), result.getMetaData().getColumnClassName(1));
      assertEquals(Byte.class.getName(), result.getMetaData().getColumnClassName(2));
      assertFalse(result.next());
    }
  }

  /**
   * Preserves non-finite approximate operands using double arithmetic.
   *
   * @param pushdown
   *          whether both pushdown settings are enabled
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void nonFiniteApproximateOperands(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      String inner = "SELECT d + 1 AS d, r * 2 AS r FROM o WHERE id = 2";
      for (String sql : boundaries(inner)) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals(Types.DOUBLE, result.getMetaData().getColumnType(1), sql);
          assertEquals(Types.DOUBLE, result.getMetaData().getColumnType(2), sql);
          assertTrue(result.next(), sql);
          assertEquals(Double.NaN, result.getObject(1), sql);
          assertEquals(Double.POSITIVE_INFINITY, result.getObject(2), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Checks decimal carry, product scale, and aliases shadowing source fields.
   *
   * @param pushdown
   *          whether both pushdown settings are enabled
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void decimalBoundsAndAliases(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      for (String expression : List.of("dec + 1", "dec - 1", "dec * 10", "dec * dec", "-dec")) {
        int precision = expression.equals("dec * dec")
            ? 10
            : expression.equals("-dec") ? 5 : expression.equals("dec * 10") ? 7 : 6;
        int scale = expression.equals("dec * dec") ? 4 : 2;
        for (int id : List.of(0, 99)) {
          String inner = "SELECT " + expression + " AS dec FROM o WHERE id = " + id;
          for (String sql : boundaries(inner)) {
            try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
              var metadata = result.getMetaData();
              assertEquals(Types.DECIMAL, metadata.getColumnType(1), sql);
              assertEquals(precision, metadata.getPrecision(1), sql);
              assertEquals(scale, metadata.getScale(1), sql);
              assertEquals(id == 0, result.next(), sql);
              if (id == 0) {
                assertInstanceOf(BigDecimal.class, result.getObject(1), sql);
              }
              assertFalse(result.next(), sql);
            }
          }
        }
      }
    }
  }

  /**
   * Verifies metadata without values, including NULL and aggregate expressions.
   *
   * @param pushdown
   *          whether both pushdown settings are enabled
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void emptyAndNullInputs(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      for (String expression : List.of("n * 2", "n + 1", "-n", "n % 3", "SUM(n) * 2", "COUNT(*) + 1")) {
        for (String filter : List.of("id = 1", "id = 99")) {
          String inner = "SELECT " + expression + " AS n FROM o WHERE " + filter;
          for (String sql : boundaries(inner)) {
            try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
              assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1), sql);
              assertEquals(Long.class.getName(), result.getMetaData().getColumnClassName(1), sql);
              if (result.next()) {
                if (expression.startsWith("COUNT")) {
                  assertInstanceOf(Long.class, result.getObject(1));
                } else {
                  assertNull(result.getObject(1), sql);
                  assertTrue(result.wasNull());
                }
              }
              assertFalse(result.next(), sql);
              assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1), sql);
            }
          }
        }
      }
    }
  }

  /**
   * Verifies checked BIGINT addition, subtraction, multiplication, and unary
   * signs.
   *
   * @param pushdown
   *          whether both pushdown settings are enabled
   * @throws Exception
   *           if JDBC connection setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void overflow(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      for (String expression : List.of("l + 1", "l * 2", "(-l - 1) - 1", "-(-l - 1)", "-(-9223372036854775808)")) {
        for (String sql : boundaries("SELECT " + expression + " AS x FROM o WHERE id = 0")) {
          SQLException failure = assertThrows(SQLException.class, () -> {
            try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
              result.next();
              result.getObject(1);
            }
          }, sql);
          assertEquals("22003", failure.getSQLState(), sql);
        }
      }
    }
  }

  /**
   * Keeps mixed INTEGER literal and BIGINT branch keys equal without changing the
   * values exposed by CASE and COALESCE.
   *
   * @param pushdown
   *          whether both pushdown settings are enabled
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void mixedWholeNumberKeys(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown); var statement = connection.createStatement()) {
      String expression = "CASE WHEN id = 2 THEN 5 ELSE l END";
      String grouped = "SELECT " + expression + " AS k, COUNT(*) FROM keys WHERE id >= 0 GROUP BY " + expression;
      for (String sql : boundaries(grouped)) {
        try (var result = statement.executeQuery(sql + " ORDER BY k")) {
          assertTrue(result.next(), sql);
          assertEquals(5L, result.getLong(1), sql);
          assertEquals(2L, result.getLong(2), sql);
          assertTrue(result.next(), sql);
          assertEquals(9L, result.getLong(1), sql);
          assertEquals(1L, result.getLong(2), sql);
          assertFalse(result.next(), sql);
        }
      }
      String distinct = "SELECT DISTINCT COALESCE(CASE WHEN id = 2 THEN NULL ELSE l END, 5) AS k "
          + "FROM keys WHERE id >= 0";
      for (String sql : boundaries(distinct)) {
        try (var result = statement.executeQuery(sql + " ORDER BY k")) {
          assertTrue(result.next(), sql);
          assertEquals(5L, result.getLong(1), sql);
          assertTrue(result.next(), sql);
          assertEquals(9L, result.getLong(1), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Preserves DECIMAL64 division text and scale across materialization
   * boundaries.
   *
   * @param pushdown
   *          whether both pushdown settings are enabled
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void divisionScales(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown); var statement = connection.createStatement()) {
      for (String expression : List.of("7 / 2", "1 / 3", "CAST(1 AS DECIMAL(38,10)) / CAST(1 AS DECIMAL(38,10))",
          "(CAST(1 AS DECIMAL(38,10)) / 3) / 3", "SUM(n) / 2")) {
        String inner = "SELECT " + expression + " AS x FROM o WHERE id = 0";
        BigDecimal expected;
        try (var result = statement.executeQuery(inner)) {
          assertTrue(result.next());
          expected = assertInstanceOf(BigDecimal.class, result.getObject(1));
          assertEquals(expected.toString(), result.getString(1));
        }
        for (String sql : boundaries(inner)) {
          try (var result = statement.executeQuery(sql)) {
            assertTrue(result.next(), sql);
            assertEquals(expected, result.getObject(1), sql);
            assertEquals(expected.toString(), result.getString(1), sql);
            assertTrue(result.getMetaData().getPrecision(1) <= 38, sql);
            assertFalse(result.next(), sql);
          }
          try (var result = statement.executeQuery("SELECT CAST(x AS VARCHAR(30)) FROM (" + sql + ") v")) {
            assertTrue(result.next(), sql);
            assertEquals(expected.toString(), result.getString(1), sql);
          }
        }
      }
    }
  }

  /**
   * Widens SUM's narrow whole-number inputs before deriving decimal product
   * bounds.
   *
   * @param pushdown
   *          whether both pushdown settings are enabled
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void narrowSums(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown); var statement = connection.createStatement()) {
      for (String type : List.of("SMALLINT", "TINYINT")) {
        int operand = type.equals("SMALLINT") ? 32000 : 127;
        String inner = "SELECT SUM(CAST(" + operand + " AS " + type
            + ")) * CAST(1 AS DECIMAL(1,0)) AS x FROM keys a CROSS JOIN keys b "
            + "CROSS JOIN keys c CROSS JOIN keys d";
        for (String sql : boundaries(inner)) {
          try (var result = statement.executeQuery(sql)) {
            assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1), sql);
            assertEquals(20, result.getMetaData().getPrecision(1), sql);
            assertTrue(result.next(), sql);
            assertEquals(BigDecimal.valueOf(81L * operand), result.getObject(1), sql);
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }

  /**
   * Reports whole-number type precision while retaining narrower literal bounds
   * internally for decimal arithmetic, and documents unary-sign promotion.
   *
   * @param pushdown
   *          whether both pushdown settings are enabled
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void literalMetadata(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT 1, 2147483648, -1, +1, 1.5, n, l FROM o WHERE id = 0")) {
      assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1));
      assertEquals(10, result.getMetaData().getPrecision(1));
      for (int column : List.of(2, 3, 4)) {
        assertEquals(Types.BIGINT, result.getMetaData().getColumnType(column));
        assertEquals(19, result.getMetaData().getPrecision(column));
      }
      assertEquals(2, result.getMetaData().getPrecision(5));
      assertEquals(1, result.getMetaData().getScale(5));
      assertEquals(10, result.getMetaData().getPrecision(6));
      assertEquals(19, result.getMetaData().getPrecision(7));
      assertTrue(result.next());
      assertEquals(1, result.getObject(1));
      assertEquals(2147483648L, result.getObject(2));
      assertEquals(-1L, result.getObject(3));
      assertEquals(1L, result.getObject(4));
      assertFalse(result.next());
    }
  }

  /**
   * Executes one projection across all materialization boundaries.
   *
   * @param connection
   *          current JDBC connection
   * @param expression
   *          projection to check
   * @param type
   *          expected JDBC type
   * @param expected
   *          expected value, or null
   * @throws Exception
   *           if execution fails
   */
  private void check(Connection connection, String expression, int type, Object expected) throws Exception {
    String inner = "SELECT " + expression + " AS x FROM o WHERE id = 0";
    for (String sql : boundaries(inner)) {
      try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
        assertEquals(type, result.getMetaData().getColumnType(1), sql);
        assertTrue(result.next(), sql);
        Object actual = result.getObject(1);
        if (expected instanceof BigDecimal decimal) {
          assertInstanceOf(BigDecimal.class, actual, sql);
          assertEquals(decimal, actual, sql);
        } else {
          assertEquals(expected, actual, sql);
        }
        if (actual != null) {
          assertEquals(actual.getClass().getName(), result.getMetaData().getColumnClassName(1), sql);
        }
        assertFalse(result.next(), sql);
      }
    }
  }

  /**
   * Builds direct, derived, CTE, and set-derived variants.
   *
   * @param inner
   *          inner SELECT statement
   * @return equivalent single-row or empty queries
   */
  private List<String> boundaries(String inner) {
    return List.of(inner, "SELECT * FROM (" + inner + ") q", "WITH q AS (" + inner + ") SELECT * FROM q",
        "SELECT * FROM (" + inner + " UNION ALL SELECT * FROM (" + inner + ") empty WHERE 1 = 0) q");
  }

  /**
   * Connects with both pushdown options configured together.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @return the connection
   * @throws SQLException
   *           if connection setup fails
   */
  private Connection connect(boolean pushdown) throws SQLException {
    return DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
  }
}
