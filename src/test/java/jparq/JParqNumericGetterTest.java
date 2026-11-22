package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.engine.NumericCastTarget;
import se.alipsa.jparq.engine.SqlExceptions;

/** Verifies numeric ResultSet getters for both column indexes and labels. */
class JParqNumericGetterTest {
  @TempDir
  Path directory;

  /**
   * Converts numeric text, preserving the original JDBC object and NULL status.
   *
   * @param target
   *          the getter's numeric target
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @EnumSource(NumericCastTarget.class)
  void convertsNumericText(NumericCastTarget target) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String text : List.of("1", " +12 ", "-12.75", "1.25e1")) {
        try (var result = statement.executeQuery("SELECT CAST('" + text + "' AS VARCHAR) AS v")) {
          assertTrue(result.next());
          assertEquals(text, result.getObject(1));
          double expected = Double.parseDouble(text);
          if (target != NumericCastTarget.REAL && target != NumericCastTarget.DOUBLE) {
            expected = (long) expected;
          }
          for (boolean byLabel : List.of(false, true)) {
            assertEquals(expected, read(result, target, byLabel).doubleValue());
            assertFalse(result.wasNull());
          }
          assertEquals(text, result.getObject("v"));
        }
      }
    }
  }

  /**
   * Keeps numbers, booleans, and SQL NULL consistent across both overloads.
   *
   * @param target
   *          the getter's numeric target
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @EnumSource(NumericCastTarget.class)
  void handlesNumbersAndNulls(NumericCastTarget target) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String expression : List.of("42", "CAST(42 AS BIGINT)", "CAST(42 AS REAL)", "CAST(42 AS DOUBLE)",
          "CAST(42 AS DECIMAL(5,2))", "TRUE", "FALSE", "CAST(TRUE AS BOOLEAN)", "CAST(FALSE AS BOOLEAN)",
          "CAST(NULL AS VARCHAR)", "CAST(NULL AS INTEGER)", "CAST(NULL AS BOOLEAN)")) {
        boolean isNull = expression.contains("NULL");
        double expected = 42d;
        if (isNull || expression.contains("FALSE")) {
          expected = 0d;
        } else if (expression.contains("TRUE")) {
          expected = 1d;
        }
        try (var result = statement.executeQuery("SELECT " + expression + " AS v")) {
          assertTrue(result.next());
          for (boolean byLabel : List.of(false, true)) {
            assertEquals(expected, read(result, target, byLabel).doubleValue(), expression);
            assertEquals(isNull, result.wasNull(), expression);
          }
        }
      }
      try (var result = statement.executeQuery("SELECT v FROM (VALUES (NULL), ('7')) AS t(v)")) {
        assertTrue(result.next());
        assertEquals(0d, read(result, target, false).doubleValue());
        assertTrue(result.wasNull());
        assertTrue(result.next());
        assertEquals(7d, read(result, target, true).doubleValue());
        assertFalse(result.wasNull());
      }
    }
  }

  /**
   * Reports invalid text and incompatible values through checked data exceptions.
   *
   * @param target
   *          the getter's numeric target
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @EnumSource(NumericCastTarget.class)
  void rejectsInvalidValues(NumericCastTarget target) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String expression : List.of("'not a number'", "''", "'   '", "'NaN'", "'Infinity'", "'-Infinity'",
          "DATE '2024-01-01'")) {
        try (var result = statement.executeQuery("SELECT " + expression + " AS v")) {
          assertTrue(result.next());
          for (boolean byLabel : List.of(false, true)) {
            var failure = assertThrows(SQLDataException.class, () -> read(result, target, byLabel), expression);
            assertEquals("22018", failure.getSQLState(), expression);
            assertTrue(failure.getMessage().contains("column 1 ('v')"), failure::getMessage);
            Throwable cause = failure.getCause();
            while (cause != null && !(cause instanceof NumberFormatException)) {
              cause = cause.getCause();
            }
            assertInstanceOf(NumberFormatException.class, cause);
            assertFalse(result.wasNull());
          }
        }
      }
    }
  }

  /**
   * Names the column label and position when either overload fails conversion.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void conversionDiagnosticsIncludeColumnLabel() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT 1 AS id, 'bad' AS price")) {
      assertTrue(result.next());
      for (boolean byLabel : List.of(false, true)) {
        var failure = assertThrows(SQLDataException.class, () -> {
          if (byLabel) {
            result.getInt("price");
          } else {
            result.getInt(2);
          }
        });
        assertEquals("22018", failure.getSQLState());
        assertTrue(failure.getMessage().contains("column 2 ('price') as INTEGER"), failure::getMessage);
      }
    }
  }

  /**
   * Redacts private bindings before bounding unaliased LATERAL expression labels.
   *
   * @param target
   *          the getter's numeric target
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @EnumSource(NumericCastTarget.class)
  void lateralDiagnosticsRedactAndBoundLabels(NumericCastTarget target) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String expression : List.of("'z' || CAST(a.n AS VARCHAR)", "CONCAT('z', a.n)",
          "'" + "z".repeat(100) + "' || CAST(a.n AS VARCHAR)")) {
        String sql = "SELECT x.* FROM (VALUES (1)) a(n) CROSS JOIN LATERAL (SELECT " + expression
            + " FROM (VALUES (1)) s(z)) x";
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next());
          String label = result.getMetaData().getColumnLabel(1);
          // #334 tracks private bindings in metadata labels; only diagnostics are
          // asserted here.
          String expected = SqlExceptions.diagnosticValue(SqlExceptions.describe(label));
          var failure = assertThrows(SQLDataException.class, () -> read(result, target, false));
          assertEquals("22018", failure.getSQLState());
          assertTrue(failure.getMessage().contains("column 1 ('" + expected + "')"), failure::getMessage);
          assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"), failure::getMessage);
          assertTrue(expected.length() <= 65, expected);
          assertFalse(failure.getMessage().contains("z".repeat(65)), failure::getMessage);
        }
      }
    }
  }

  /**
   * Uses the metadata label for wildcard columns with a different internal name.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void lateralDiagnosticsUseMetadataLabels() throws SQLException {
    String sql = "SELECT * FROM (VALUES (1)) a(n) "
        + "CROSS JOIN LATERAL (SELECT CONCAT('z', a.n) FROM (VALUES (1)) s(z)) x";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertTrue(result.next());
      String label = result.getMetaData().getColumnLabel(2);
      // Accept the metadata label regardless of how #334 changes expression labeling.
      assertEquals(2, result.findColumn(label));
      String expected = SqlExceptions.diagnosticValue(SqlExceptions.describe(label));
      for (boolean byLabel : List.of(false, true)) {
        var failure = assertThrows(SQLDataException.class, () -> {
          if (byLabel) {
            result.getInt(label);
          } else {
            result.getInt(2);
          }
        });
        assertEquals("22018", failure.getSQLState());
        assertTrue(failure.getMessage().contains("column 2 ('" + expected + "')"), failure::getMessage);
        assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"), failure::getMessage);
      }
    }
  }

  /**
   * Preserves conversion failures when diagnostic metadata cannot supply a label.
   *
   * @param scenario
   *          missing columns, inconsistent indexes, or failing metadata
   *          construction or access
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "missing", "inconsistent", "failed", "construction"
  })
  void metadataLookupCannotHideConversionFailure(String scenario) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT 'bad' AS price")) {
      assertTrue(result.next());
      var metadata = mock(ResultSetMetaData.class);
      if (!"missing".equals(scenario) && !"construction".equals(scenario)) {
        when(metadata.getColumnCount()).thenReturn(1);
        if ("inconsistent".equals(scenario)) {
          when(metadata.getColumnLabel(1)).thenThrow(new IndexOutOfBoundsException("Missing label"));
        } else {
          when(metadata.getColumnLabel(1)).thenThrow(new SQLException("Metadata unavailable"));
        }
      }
      var diagnostic = spy((JParqResultSet) result);
      if ("construction".equals(scenario)) {
        doThrow(new NullPointerException("Invalid metadata label")).when(diagnostic).getMetaData();
      } else {
        doReturn(metadata).when(diagnostic).getMetaData();
      }
      var failure = assertThrows(SQLDataException.class, () -> diagnostic.getInt(1));
      assertEquals("22018", failure.getSQLState());
      assertTrue(failure.getMessage().contains("column 1 ('?') as INTEGER"), failure::getMessage);
      assertTrue(failure.getMessage().contains("Cannot cast value 'bad'"), failure::getMessage);
    }
  }

  /**
   * Rejects text overflow instead of wrapping integers or producing infinity.
   *
   * @param target
   *          the getter's numeric target
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @EnumSource(NumericCastTarget.class)
  void rejectsOverflow(NumericCastTarget target) throws SQLException {
    String overflow = switch (target) {
      case TINYINT -> "128";
      case SMALLINT -> "32768";
      case INTEGER -> "2147483648";
      case BIGINT -> "9223372036854775808";
      case REAL -> "1e39";
      case DOUBLE -> "1e309";
    };
    String negativeOverflow = switch (target) {
      case REAL, DOUBLE -> "-" + overflow;
      default -> new BigInteger(overflow).negate().subtract(BigInteger.ONE).toString();
    };
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String expression : List.of("'" + overflow + "'", "'" + negativeOverflow + "'")) {
        try (var result = statement.executeQuery("SELECT " + expression + " AS v")) {
          assertTrue(result.next());
          for (boolean byLabel : List.of(false, true)) {
            var failure = assertThrows(SQLDataException.class, () -> read(result, target, byLabel), expression);
            assertEquals("22003", failure.getSQLState(), expression);
          }
        }
      }
    }
  }

  /**
   * Accepts integer boundary values without losing precision during conversion.
   *
   * @param target
   *          the getter's numeric target
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @EnumSource(value = NumericCastTarget.class, names = {
      "TINYINT", "SMALLINT", "INTEGER", "BIGINT"
  })
  void acceptsIntegerBoundaries(NumericCastTarget target) throws SQLException {
    long maximum = switch (target) {
      case TINYINT -> Byte.MAX_VALUE;
      case SMALLINT -> Short.MAX_VALUE;
      case INTEGER -> Integer.MAX_VALUE;
      default -> Long.MAX_VALUE;
    };
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (long boundary : List.of(maximum, -maximum - 1)) {
        String text = boundary + (boundary < 0 ? ".9" : ".0");
        try (var result = statement.executeQuery("SELECT '" + text + "' AS v")) {
          assertTrue(result.next());
          for (boolean byLabel : List.of(false, true)) {
            assertEquals(boundary, read(result, target, byLabel).longValue());
          }
        }
      }
    }
  }

  /**
   * Converts numeric VARCHAR values produced by a correlated LATERAL query.
   *
   * @param target
   *          the getter's numeric target
   * @throws SQLException
   *           if query execution fails
   */
  @ParameterizedTest
  @EnumSource(NumericCastTarget.class)
  void convertsLateralNumericText(NumericCastTarget target) throws SQLException {
    String sql = "SELECT x.v FROM (VALUES (1), (2)) AS a(n) "
        + "CROSS JOIN LATERAL (SELECT a.n / 1 AS v FROM (VALUES (1)) AS seed(z)) x ORDER BY x.v";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      for (int expected = 1; expected <= 2; expected++) {
        assertTrue(result.next());
        assertInstanceOf(String.class, result.getObject(1));
        for (boolean byLabel : List.of(false, true)) {
          assertEquals((double) expected, read(result, target, byLabel).doubleValue());
          assertFalse(result.wasNull());
        }
      }
      assertFalse(result.next());
    }
  }

  /**
   * Range-checks native numbers instead of wrapping integers or saturating
   * floating-point results, matching the text conversion rules.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void rejectsNativeNumberOverflow() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (boolean byLabel : List.of(false, true)) {
        assertRange(statement, "CAST(3000000000 AS BIGINT)", NumericCastTarget.INTEGER, byLabel);
        assertRange(statement, "'3000000000'", NumericCastTarget.INTEGER, byLabel);
        assertRange(statement, "CAST(300 AS INTEGER)", NumericCastTarget.TINYINT, byLabel);
        assertRange(statement, "CAST(-129 AS BIGINT)", NumericCastTarget.TINYINT, byLabel);
        assertRange(statement, "CAST(2147483648 AS BIGINT)", NumericCastTarget.INTEGER, byLabel);
        assertRange(statement, "CAST(1e20 AS DOUBLE)", NumericCastTarget.BIGINT, byLabel);
        assertRange(statement, "CAST(1e308 AS DOUBLE)", NumericCastTarget.BIGINT, byLabel);
        assertRange(statement, "CAST(9223372036854775807 AS DOUBLE)", NumericCastTarget.BIGINT, byLabel);
        assertRange(statement, "CAST('1e390' AS DECIMAL(400,0))", NumericCastTarget.DOUBLE, byLabel);
        assertRange(statement, "CAST('1e390' AS DECIMAL(400,0))", NumericCastTarget.BIGINT, byLabel);
        assertRange(statement, "CAST('1e40' AS DECIMAL(40,0))", NumericCastTarget.REAL, byLabel);
        assertRange(statement, "CAST(1e40 AS DOUBLE)", NumericCastTarget.REAL, byLabel);
      }
    }
  }

  /**
   * Keeps NaN and infinity in the floating-point getters while rejecting them
   * from integer targets, converts in-range values without representation loss,
   * and raises 22003 for finite values beyond the target range, so the primitive
   * getters and the typed Long target agree.
   *
   * @throws Exception
   *           if the fixture or a query fails
   */
  @Test
  void appliesRangeChecksToStoredDoubles() throws Exception {
    List<DoubleRow> rows = List.of(DoubleRow.nonFinite(Double.NaN), DoubleRow.nonFinite(Double.POSITIVE_INFINITY),
        DoubleRow.nonFinite(Double.NEGATIVE_INFINITY), DoubleRow.finite(2.9d), DoubleRow.finite(-2.9d),
        DoubleRow.finite(127.9d), DoubleRow.finite(-128.5d), DoubleRow.finite(-32768.5d),
        DoubleRow.finite(-2147483648.0d), DoubleRow.finite(-2147483648.5d), DoubleRow.finite(-9223372036854775808.0d),
        DoubleRow.finite(9223372036854774784.0d), DoubleRow.finite(1234567890123456768.0d),
        DoubleRow.unbounded(-9223372036854777856.0d), // Math.nextDown(-0x1p63)
        DoubleRow.unbounded(1e40d), DoubleRow.unbounded(1e308d));
    writeNumericFixture("doubles", Schema.Type.DOUBLE, rows);
    checkNumericFixture("doubles", rows);
  }

  /**
   * Applies the same range checks to stored REAL values, covering the float
   * source paths through the integer and double getters.
   *
   * @throws Exception
   *           if the fixture or a query fails
   */
  @Test
  void appliesRangeChecksToStoredFloats() throws Exception {
    List<DoubleRow> rows = List.of(DoubleRow.nonFinite(Double.NaN), DoubleRow.nonFinite(Double.POSITIVE_INFINITY),
        DoubleRow.nonFinite(Double.NEGATIVE_INFINITY), DoubleRow.finite(2.5f), DoubleRow.finite(127.9f),
        DoubleRow.unbounded(Float.MAX_VALUE));
    writeNumericFixture("floats", Schema.Type.FLOAT, rows);
    checkNumericFixture("floats", rows);
  }

  /**
   * Writes a fixture with an id column and a floating-point column.
   *
   * @param name
   *          the fixture file name and table name
   * @param type
   *          the floating-point schema type
   * @param rows
   *          the values to write, in id order
   * @throws Exception
   *           if the fixture cannot be written
   */
  private void writeNumericFixture(String name, Schema.Type type, List<DoubleRow> rows) throws Exception {
    Schema schema = SchemaBuilder.record(name).fields().requiredInt("id").name("d").type(Schema.create(type))
        .noDefault().endRecord();
    var file = new org.apache.hadoop.fs.Path(directory.resolve(name + ".parquet").toUri());
    try (
        var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, new Configuration(false)))
            .withSchema(schema).withConf(new Configuration(false)).build()) {
      for (int id = 1; id <= rows.size(); id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        double value = rows.get(id - 1).value();
        row.put("d", type == Schema.Type.FLOAT ? (float) value : value);
        writer.write(row);
      }
    }
  }

  /**
   * Reads every fixture row through the double, float, integer, and typed numeric
   * getters by index and by label, comparing with the row's expectations.
   *
   * @param table
   *          the fixture table name
   * @param rows
   *          the expected values, in id order
   * @throws SQLException
   *           if query execution fails
   */
  private void checkNumericFixture(String table, List<DoubleRow> rows) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (int id = 1; id <= rows.size(); id++) {
        DoubleRow row = rows.get(id - 1);
        for (boolean byLabel : List.of(false, true)) {
          try (var result = statement.executeQuery("SELECT d AS v FROM " + table + " WHERE id = " + id)) {
            String context = table + " id " + id;
            assertTrue(result.next(), context);
            assertEquals(row.value(), result.getDouble(1), context);
            float floatValue = (float) row.value();
            if (!Float.isFinite(floatValue) && Double.isFinite(row.value())) {
              var failure = assertThrows(SQLDataException.class, () -> read(result, NumericCastTarget.REAL, byLabel),
                  context);
              assertEquals("22003", failure.getSQLState(), context);
            } else {
              assertEquals(floatValue, read(result, NumericCastTarget.REAL, byLabel).floatValue(), context);
            }
            for (NumericCastTarget target : List.of(NumericCastTarget.TINYINT, NumericCastTarget.SMALLINT,
                NumericCastTarget.INTEGER, NumericCastTarget.BIGINT)) {
              Long expected = row.integerValue(target);
              if (expected == null) {
                var failure = assertThrows(SQLDataException.class, () -> read(result, target, byLabel), context);
                assertEquals(row.integerState(), failure.getSQLState(), context);
              } else {
                assertEquals(expected, read(result, target, byLabel).longValue(), context);
              }
            }
            assertTypedNumber(result, Long.class, row.integerValue(NumericCastTarget.BIGINT), row.integerState(),
                context);
            assertTypedNumber(result, Byte.class, row.integerValue(NumericCastTarget.TINYINT), row.integerState(),
                context);
            assertTypedNumber(result, Short.class, row.integerValue(NumericCastTarget.SMALLINT), row.integerState(),
                context);
            assertTypedNumber(result, Integer.class, row.integerValue(NumericCastTarget.INTEGER), row.integerState(),
                context);
            assertTypedFloat(result, row.floatValue(), context);
          }
        }
      }
    }
  }

  /**
   * Asserts a typed numeric getObject result against the row's expectation.
   *
   * @param result
   *          the positioned result set
   * @param type
   *          the requested numeric Java type
   * @param expected
   *          the expected value, or null for a failure
   * @param state
   *          the SQLState to expect when the conversion fails
   * @param context
   *          the failure message context
   * @throws SQLException
   *           if retrieval fails unexpectedly
   */
  private static void assertTypedNumber(ResultSet result, Class<?> type, Long expected, String state, String context)
      throws SQLException {
    if (expected == null) {
      var failure = assertThrows(SQLDataException.class, () -> result.getObject(1, type), context);
      assertEquals(state, failure.getSQLState(), context);
    } else {
      assertEquals(expected, ((Number) result.getObject(1, type)).longValue(), context);
    }
  }

  /**
   * Asserts the typed Float getObject result against the row's expectation.
   *
   * @param result
   *          the positioned result set
   * @param expected
   *          the expected float, or null when 22003 must be raised
   * @param context
   *          the failure message context
   * @throws SQLException
   *           if retrieval fails unexpectedly
   */
  private static void assertTypedFloat(ResultSet result, Float expected, String context) throws SQLException {
    if (expected == null) {
      var failure = assertThrows(SQLDataException.class, () -> result.getObject(1, Float.class), context);
      assertEquals("22003", failure.getSQLState(), context);
    } else {
      assertEquals(expected, result.getObject(1, Float.class), context);
    }
  }

  /**
   * A stored floating-point row with its expected integer-getter results.
   *
   * @param value
   *          the stored value
   * @param boundedInteger
   *          the truncated value when finite and within the BIGINT range
   * @param failureState
   *          the SQLState the integer getters must raise when boundedInteger is
   *          null
   */
  private record DoubleRow(double value, Long boundedInteger, String failureState) {
    /**
     * Creates a row for a finite value within the BIGINT range.
     *
     * @param value
     *          the stored value
     * @return the row with the truncated value as its integer expectation
     */
    static DoubleRow finite(double value) {
      return new DoubleRow(value, (long) value, null);
    }

    /**
     * Creates a row for NaN or an infinity.
     *
     * @param value
     *          the stored non-finite value
     * @return the row expecting 22018 from the integer getters
     */
    static DoubleRow nonFinite(double value) {
      return new DoubleRow(value, null, "22018");
    }

    /**
     * Creates a row for a finite value beyond the BIGINT range.
     *
     * @param value
     *          the stored value
     * @return the row expecting 22003 from the integer getters
     */
    static DoubleRow unbounded(double value) {
      return new DoubleRow(value, null, "22003");
    }

    /**
     * Returns the expected float value, or null when the float target must raise
     * 22003.
     *
     * @return the expected value, or null for a failure
     */
    Float floatValue() {
      float result = (float) value;
      return !Float.isFinite(result) && Double.isFinite(value) ? null : result;
    }

    /**
     * Returns the expected integer value, or null when the target must raise.
     *
     * @param target
     *          the integer getter's target
     * @return the expected value, or null for a failure
     */
    Long integerValue(NumericCastTarget target) {
      if (boundedInteger == null) {
        return null;
      }
      long maximum = switch (target) {
        case TINYINT -> Byte.MAX_VALUE;
        case SMALLINT -> Short.MAX_VALUE;
        case INTEGER -> Integer.MAX_VALUE;
        default -> Long.MAX_VALUE;
      };
      return boundedInteger >= -maximum - 1 && boundedInteger <= maximum ? boundedInteger : null;
    }

    /**
     * Returns the SQLState the integer getters must raise.
     *
     * @return 22018 for non-finite values, 22003 otherwise
     */
    String integerState() {
      return failureState != null ? failureState : "22003";
    }
  }

  /**
   * Asserts that reading an expression through a getter raises 22003.
   *
   * @param statement
   *          the active statement
   * @param expression
   *          the value expression to select
   * @param target
   *          the getter's numeric target
   * @param byLabel
   *          whether to use the column label instead of its index
   * @throws SQLException
   *           if query execution fails
   */
  private static void assertRange(Statement statement, String expression, NumericCastTarget target, boolean byLabel)
      throws SQLException {
    try (var result = statement.executeQuery("SELECT " + expression + " AS v")) {
      assertTrue(result.next(), expression);
      var failure = assertThrows(SQLDataException.class, () -> read(result, target, byLabel), expression);
      assertEquals("22003", failure.getSQLState(), expression);
    }
  }

  /**
   * Calls the getter overload matching the target and access mode.
   *
   * @param result
   *          the positioned result set
   * @param target
   *          the getter's numeric target
   * @param byLabel
   *          whether to use the column label instead of its index
   * @return the boxed primitive result
   * @throws SQLException
   *           if retrieval or conversion fails
   */
  private static Number read(ResultSet result, NumericCastTarget target, boolean byLabel) throws SQLException {
    return switch (target) {
      case TINYINT -> byLabel ? result.getByte("v") : result.getByte(1);
      case SMALLINT -> byLabel ? result.getShort("v") : result.getShort(1);
      case INTEGER -> byLabel ? result.getInt("v") : result.getInt(1);
      case BIGINT -> byLabel ? result.getLong("v") : result.getLong(1);
      case REAL -> byLabel ? result.getFloat("v") : result.getFloat(1);
      case DOUBLE -> byLabel ? result.getDouble("v") : result.getDouble(1);
    };
  }
}
