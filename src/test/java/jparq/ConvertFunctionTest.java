package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for SQL CONVERT handling including character set transcoding. */
class ConvertFunctionTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setUp() throws URISyntaxException {
    URL mtcarsUrl = ConvertFunctionTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be available on the classpath");
    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + mtcarsPath.getParent().toAbsolutePath());
  }

  @Test
  void convertUsingUtf8IsNoop() {
    String sql = "VALUES (CONVERT('Café' USING UTF8))";
    jparqSql.query(sql, rs -> assertSingleString(rs, "Café"));
  }

  @Test
  void convertUsingAsciiReplacesUnsupportedCharacters() {
    String sql = "VALUES (CONVERT('Café' USING ASCII))";
    String expected = new String("Café".getBytes(StandardCharsets.UTF_8), StandardCharsets.US_ASCII);
    jparqSql.query(sql, rs -> assertSingleString(rs, expected));
  }

  @Test
  void convertToTypeBehavesLikeCast() {
    String sql = """
        SELECT CONVERT(hp, DOUBLE) AS converted, CAST(hp AS DOUBLE) AS casted
        FROM mtcars
        WHERE model = 'Mazda RX4'
        """;
    jparqSql.query(sql, rs -> {
      try {
        assertTrue(rs.next(), "Expected a row for Mazda RX4");
        assertEquals(rs.getDouble("casted"), rs.getDouble("converted"));
        assertFalse(rs.next(), "Only one row should match Mazda RX4");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void jdbcEscapeConvertIsRewrittenToCast() {
    String sql = """
        SELECT {fn convert(hp, INTEGER)} AS converted, CAST(hp AS INTEGER) AS casted
        FROM mtcars
        WHERE model = 'Datsun 710'
        """;
    jparqSql.query(sql, rs -> {
      try {
        assertTrue(rs.next(), "Expected a row for Datsun 710");
        assertEquals(rs.getInt("casted"), rs.getInt("converted"));
        assertFalse(rs.next(), "Only one row should match Datsun 710");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void invalidCharsetRaisesError() {
    RuntimeException ex = assertThrows(RuntimeException.class,
        () -> jparqSql.query("VALUES (CONVERT('abc' USING DOES_NOT_EXIST))", rs -> {
          try {
            rs.next();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }));
    assertTrue(ex.getCause() instanceof SQLException, "Expected SQLException cause for invalid charset");
  }

  @Test
  void convertHandlesColumnNameMatchingTypeName() {
    // This test verifies the argument swapping logic in
    // ValueExpressionEvaluator.resolveConvertArguments()
    // JSqlParser swaps CONVERT arguments: CONVERT(value, type) is parsed as
    // expression=type, colDataType=value
    // The swap logic detects this and corrects it, unless both arguments could be
    // valid column names
    String sql = """
        SELECT CONVERT(num_value, VARCHAR) AS value_as_str,
               CAST(num_value AS VARCHAR) AS value_cast_str
        FROM (SELECT hp AS num_value FROM mtcars WHERE model = 'Mazda RX4') AS data
        """;
    jparqSql.query(sql, rs -> {
      try {
        assertTrue(rs.next(), "Expected a row from derived table");
        // Both CONVERT and CAST should produce the same result
        assertEquals(rs.getString("value_cast_str"), rs.getString("value_as_str"),
            "CONVERT should behave like CAST for type conversion");
        assertEquals("110", rs.getString("value_as_str"));
        assertFalse(rs.next(), "Only one row should be returned");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void jdbcEscapeConvertWithNestedFunctionCall() {
    String sql = """
        SELECT {fn convert(CONCAT(model, ' model'), VARCHAR)} AS converted
        FROM mtcars
        WHERE model = 'Mazda RX4'
        """;
    jparqSql.query(sql, rs -> {
      try {
        assertTrue(rs.next(), "Expected a row for Mazda RX4");
        assertEquals("Mazda RX4 model", rs.getString("converted"));
        assertFalse(rs.next(), "Only one row should match Mazda RX4");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void convertWithColumnNamedAfterType() {
    // Edge case: column named after a SQL type keyword
    // Tests argument swapping when the column name itself is "integer"
    String sql = """
        SELECT CONVERT(integer, VARCHAR) AS converted
        FROM (SELECT hp AS integer FROM mtcars WHERE model = 'Mazda RX4') AS data
        """;
    jparqSql.query(sql, rs -> {
      try {
        assertTrue(rs.next(), "Expected a row");
        // Should convert the "integer" column value (110) to VARCHAR
        assertEquals("110", rs.getString("converted"));
        assertFalse(rs.next());
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void jdbcEscapeConvertWithUsingClauseIsNotRewrittenToCast() {
    // JDBC escape CONVERT with USING clause should NOT be rewritten to CAST.
    // Even though {fn convert('text', USING charset)} is non-standard JDBC syntax,
    // the resolver correctly identifies it as charset conversion (not type cast)
    // and preserves it as a CONVERT call rather than incorrectly rewriting to CAST.
    // Note: This test verifies the resolver's behavior, not that the final SQL is
    // valid.
    String escapedSql = "{fn convert('Café', USING UTF8)}";
    String resolved = se.alipsa.jparq.helper.FunctionEscapeResolver
        .resolveJdbcFunctionEscapes("VALUES (" + escapedSql + ")");
    // Should NOT be rewritten to CAST - should remain as CONVERT
    assertFalse(resolved.contains("CAST"), "CONVERT with USING should not be rewritten to CAST: " + resolved);
    assertTrue(resolved.contains("CONVERT"), "Should preserve CONVERT function: " + resolved);
  }

  @Test
  void jdbcEscapeConvertWithMultipleCommasInNestedFunction() {
    // Edge case: nested function with multiple commas should still parse correctly
    String sql = """
        SELECT {fn convert(SUBSTRING(model, 1, 5), VARCHAR)} AS converted
        FROM mtcars
        WHERE model = 'Mazda RX4'
        """;
    jparqSql.query(sql, rs -> {
      try {
        assertTrue(rs.next(), "Expected a row for Mazda RX4");
        assertEquals("Mazda", rs.getString("converted"));
        assertFalse(rs.next(), "Only one row should match Mazda RX4");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void jdbcEscapeConvertWithComplexNestedExpression() {
    // Edge case: complex nested expression with parentheses and commas
    String sql = """
        SELECT {fn convert(COALESCE(NULL, model, 'default'), VARCHAR)} AS converted
        FROM mtcars
        WHERE model = 'Datsun 710'
        """;
    jparqSql.query(sql, rs -> {
      try {
        assertTrue(rs.next(), "Expected a row for Datsun 710");
        assertEquals("Datsun 710", rs.getString("converted"));
        assertFalse(rs.next(), "Only one row should match Datsun 710");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  private void assertSingleString(ResultSet rs, String expected) {
    try {
      assertTrue(rs.next(), "Expected a single row from VALUES");
      assertEquals(expected, rs.getString(1));
      assertFalse(rs.next(), "Only one row should be returned");
    } catch (SQLException e) {
      fail(e);
    }
  }
}
