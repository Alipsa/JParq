package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.SqlText;
import se.alipsa.jparq.engine.function.StringFunctions;

/**
 * Verifies binary CAST conversion, declaration validation, and JDBC accessors.
 */
class BinaryCastBehaviorTest {

  /**
   * Binary casts round-trip through explicit UTF-8 character conversion.
   *
   * @param target
   *          binary SQL type
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "VARBINARY", "BINARY(6)", "BLOB"
  })
  void decodesExplicitCharacterCasts(String target) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      String binary = "CAST('å🙂' AS " + target + ")";
      for (String sql : List.of("SELECT CAST(" + binary + " AS VARCHAR)",
          "SELECT CAST(x AS VARCHAR) FROM (SELECT " + binary + " AS x) d",
          "WITH c AS (SELECT " + binary + " AS x) SELECT CAST(x AS VARCHAR) FROM c")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals("å🙂", result.getString(1), sql);
        }
      }
    }
  }

  /**
   * OCTET_LENGTH counts octets rather than a Java binary object's identity text.
   *
   * @param expression
   *          binary or character expression
   * @param length
   *          expected byte count
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "CAST('xy' AS VARBINARY) | 2", "CAST('å🙂' AS VARBINARY) | 6", "CAST('xy' AS BINARY(4)) | 4",
      "CAST('xy' AS BLOB) | 2", "X'7879' | 2", "CAST('' AS VARBINARY) | 0", "'''å''' | 2"
  })
  void countsBinaryOctets(String expression, int length) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT OCTET_LENGTH(" + expression + ")",
          "SELECT OCTET_LENGTH(x) FROM (SELECT " + expression + " AS x) d")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(length, result.getInt(1), sql);
        }
      }
    }
  }

  /**
   * Character-only functions reject implicit binary-to-character conversion.
   *
   * @param expression
   *          SQL expression containing a binary operand
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CHAR_LENGTH(CAST('xy' AS VARBINARY))", "UPPER(CAST('xy' AS VARBINARY))", "LOWER(CAST('xy' AS VARBINARY))",
      "SUBSTRING(CAST('xyz' AS VARBINARY) FROM 1 FOR 2)", "CONCAT('a', CAST('xy' AS VARBINARY))"
  })
  void rejectsImplicitBinaryText(String expression) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      SQLException error = assertThrows(SQLException.class, () -> {
        try (var result = statement.executeQuery("SELECT " + expression)) {
          while (result.next()) {
            result.getObject(1);
          }
        }
      });
      assertEquals("42000", error.getSQLState());
    }
  }

  /**
   * Unsupported known source types fail at preparation, including empty inputs.
   *
   * @param source
   *          noncharacter source expression
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "1", "1.5", "TRUE", "DATE '2020-01-01'", "TIME '10:00:00'", "CAST(NULL AS INTEGER)", "cyl", "cyl + 1"
  })
  void rejectsNonbinarySourcesDuringPreparation(String source) throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      for (String target : List.of("BINARY(4)", "VARBINARY", "BLOB")) {
        for (String predicate : List.of("", " WHERE 1=0")) {
          String sql = "SELECT CAST(" + source + " AS " + target + ") FROM mtcars" + predicate;
          SQLException error = assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql);
          assertEquals("42000", error.getSQLState(), sql);
        }
      }
    }
  }

  /**
   * Invalid lengths are rejected even for NULL, empty, and skipped expressions.
   *
   * @param type
   *          invalid binary declaration
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "BINARY(0)", "VARBINARY(0)", "BLOB(0)", "BINARY(-1)", "VARBINARY(2147483648)"
  })
  void rejectsInvalidLengthsDuringPreparation(String type) throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      String cast = "CAST(NULL AS " + type + ")";
      for (String sql : List.of("SELECT " + cast, "SELECT " + cast + " FROM mtcars WHERE 1=0",
          "WITH c AS (SELECT " + cast + " AS x) SELECT x FROM c", "SELECT CASE WHEN 1=1 THEN 'a' ELSE " + cast + " END",
          "SELECT 1 WHERE 1=1 OR EXISTS(SELECT " + cast + ")")) {
        SQLException error = assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql);
        assertEquals("42000", error.getSQLState(), sql);
      }
    }
  }

  /**
   * Both BLOB getters return detached data and preserve SQL NULL and wasNull.
   *
   * @throws Exception
   *           if query execution or BLOB access fails
   */
  @Test
  void exposesDetachedBlobs() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      String body = "SELECT CAST('xy' AS BLOB) AS x UNION ALL SELECT CAST(NULL AS BLOB)";
      for (String sql : List.of(body, "SELECT x FROM (" + body + ") d", "WITH c AS (" + body + ") SELECT x FROM c")) {
        java.sql.Blob detached;
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          detached = result.getBlob(1);
          assertFalse(result.wasNull(), sql);
          var labeled = result.getBlob("x");
          assertArrayEquals(new byte[]{
              120, 121
          }, labeled.getBytes(1, 2), sql);
          labeled.setBytes(1, new byte[]{
              0
          });
          assertArrayEquals(new byte[]{
              120, 121
          }, result.getBytes(1), sql);
          labeled.free();
          assertEquals(2, detached.length(), sql);
          assertTrue(result.next(), sql);
          assertNull(result.getBlob(1), sql);
          assertTrue(result.wasNull(), sql);
          assertNull(result.getBlob("x"), sql);
          assertTrue(result.wasNull(), sql);
        }
        assertArrayEquals(new byte[]{
            120, 121
        }, detached.getBytes(1, 2), sql);
        detached.free();
      }
    }
  }

  /**
   * A text column cannot silently become a SQL NULL BLOB.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void rejectsNonbinaryBlobAccess() throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT 'xy' AS x")) {
      assertTrue(result.next());
      assertEquals("22005", assertThrows(SQLException.class, () -> result.getBlob(1)).getSQLState());
      assertEquals("22005", assertThrows(SQLException.class, () -> result.getBlob("x")).getSQLState());
    }
  }

  /**
   * Binary text conversions and octet counts retain SQL NULL semantics.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void preservesNullConversions() throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery(
            "SELECT OCTET_LENGTH(CAST(NULL AS VARBINARY)), " + "CAST(CAST(NULL AS VARBINARY) AS VARCHAR)")) {
      assertTrue(result.next());
      assertNull(result.getObject(1));
      assertTrue(result.wasNull());
      assertNull(result.getObject(2));
      assertTrue(result.wasNull());
    }
  }

  /**
   * Predicate pushdown and residual comparison reject implicit binary text.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void rejectsImplicitBinaryTextComparison(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url() + "?predicatePushdown=" + pushdown);
        var query = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      query.setBytes(1, "Mazda RX4".getBytes(StandardCharsets.UTF_8));
      SQLException error = assertThrows(SQLException.class, () -> {
        try (var result = query.executeQuery()) {
          while (result.next()) {
            result.getObject(1);
          }
        }
      });
      assertEquals("42000", error.getSQLState());
    }
  }

  /** Verifies ByteBuffer decoding and lengths preserve the caller's position. */
  @Test
  void preservesBinaryBufferPosition() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[]{
        0, 120, 121, 0
    });
    buffer.position(1);
    buffer.limit(3);
    assertEquals("xy", SqlText.cast(buffer));
    assertEquals(2, StringFunctions.octetLength(buffer));
    assertEquals(1, buffer.position());
    assertEquals(3, buffer.limit());
    assertEquals("xy", SqlText.cast("xy".getBytes(StandardCharsets.UTF_8)));
    assertThrows(IllegalArgumentException.class, () -> SqlText.valueOf(buffer));
  }

  /**
   * Returns the fixture database URL.
   *
   * @return JDBC connection URL
   */
  private static String url() {
    return "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath();
  }
}
