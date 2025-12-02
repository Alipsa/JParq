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
