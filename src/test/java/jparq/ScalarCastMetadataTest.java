package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies scalar metadata does not claim unimplemented CAST representations.
 */
class ScalarCastMetadataTest {

  /**
   * Retains the native local timestamp representation and metadata of value
   * functions.
   *
   * @param expression
   *          native timestamp conditional
   * @throws Exception
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "COALESCE(LOCALTIMESTAMP,NULL)", "CASE WHEN TRUE THEN LOCALTIMESTAMP ELSE NULL END",
      "COALESCE(CASE WHEN TRUE THEN LOCALTIMESTAMP END,LOCALTIMESTAMP)"
  })
  void preservesNativeTimestampConditionals(String expression) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT " + expression + " AS x", "SELECT x FROM (SELECT " + expression + " AS x) d",
          "WITH c AS (SELECT " + expression + " AS x) SELECT x FROM c")) {
        try (var result = statement.executeQuery(sql)) {
          assertEquals(Types.TIMESTAMP, result.getMetaData().getColumnType(1), sql);
          assertEquals(java.time.LocalDateTime.class.getName(), result.getMetaData().getColumnClassName(1), sql);
          assertTrue(result.next());
          assertInstanceOf(java.time.LocalDateTime.class, result.getObject(1), sql);
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Supplies scalar CASTs whose declared representations are not implemented.
   *
   * @return SQL expressions and their existing evaluated values
   */
  static Stream<Arguments> unresolvedCasts() {
    return Stream.of(Arguments.of("CAST('x' AS CLOB)", "x"),
        Arguments.of("CAST('10:00:00' AS TIME WITH TIME ZONE)", Time.valueOf("10:00:00")));
  }

  /**
   * Reads metadata before rows for literal, physical-table, and empty inputs.
   *
   * @param expression
   *          unresolved CAST expression
   * @param expected
   *          existing evaluated value
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @MethodSource("unresolvedCasts")
  void retainsUnknownScalarMetadata(String expression, Object expected) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String suffix : List.of("", " FROM mtcars WHERE model = 'Mazda RX4'", " FROM mtcars WHERE 1 = 0")) {
        String sql = "SELECT " + expression + " AS x" + suffix;
        try (var result = statement.executeQuery(sql)) {
          assertUnknownMetadata(result.getMetaData(), sql);
          if (suffix.contains("1 = 0")) {
            assertFalse(result.next(), sql);
          } else {
            assertTrue(result.next(), sql);
            Object value = result.getObject(1);
            assertEquals(expected, value, sql);
            assertTrue(Class.forName(result.getMetaData().getColumnClassName(1)).isInstance(value), sql);
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }

  /**
   * Unresolved casts do not borrow a source declaration when aliases coincide.
   *
   * @param type
   *          unresolved CAST type
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CLOB", "CHARACTER LARGE OBJECT", "NCLOB", "NATIONAL CHARACTER LARGE OBJECT"
  })
  void retainsUnknownPhysicalCastMetadata(String type) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String predicate : List.of("model = 'Mazda RX4'", "1 = 0")) {
        String sql = "SELECT CAST(first_name AS " + type + ") AS first_name FROM acme.employees WHERE 1 = 0";
        try (var result = statement.executeQuery(sql)) {
          assertUnknownMetadata(result.getMetaData(), sql);
          assertFalse(result.next(), sql);
        }
        sql = "SELECT CAST(model AS " + type + ") AS model FROM mtcars WHERE " + predicate;
        try (var result = statement.executeQuery(sql)) {
          assertUnknownMetadata(result.getMetaData(), sql);
          if (!predicate.equals("1 = 0")) {
            assertTrue(result.next(), sql);
            assertEquals("Mazda RX4", result.getObject(1), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * CLOB/VARCHAR branches temporarily use VARCHAR storage until #452 implements
   * the standard CLOB result type. NULL and non-NULL rows remain compatible.
   *
   * @param type
   *          LOB CAST type
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CLOB", "CHARACTER LARGE OBJECT", "NCLOB", "NATIONAL CHARACTER LARGE OBJECT"
  })
  void combinesClobCastsWithCharacterBranches(String type) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String value : List.of("NULL", "'x'")) {
        String character = "SELECT CAST(" + value + " AS VARCHAR) AS x";
        String lob = "SELECT CAST(" + value + " AS " + type + ") AS x";
        for (String body : List.of(character + " UNION ALL " + lob, lob + " UNION ALL " + character)) {
          for (String sql : List.of(body, "SELECT x FROM (" + body + ") d")) {
            try (var result = statement.executeQuery(sql)) {
              assertEquals(Types.VARCHAR, result.getMetaData().getColumnType(1), sql);
              assertEquals(String.class.getName(), result.getMetaData().getColumnClassName(1), sql);
              for (int row = 0; row < 2; row++) {
                assertTrue(result.next(), sql);
                assertEquals(value.equals("NULL") ? null : "x", result.getObject(1), sql);
              }
              assertFalse(result.next(), sql);
            }
          }
        }
      }
    }
  }

  /**
   * Binary declarations cannot become character or datetime inputs because their
   * declarations survive derived tables and CTEs, including empty branches.
   *
   * @param type
   *          binary CAST declaration
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "BLOB", "BINARY LARGE OBJECT", "BINARY(2)", "VARBINARY", "BINARY VARYING"
  })
  void rejectsIncompatibleBinaryCastBranches(String type) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String value : List.of("NULL", "'x'")) {
        for (String suffix : List.of("", " FROM mtcars WHERE 1 = 0")) {
          String binary = "SELECT CAST(" + value + " AS " + type + ") AS x" + suffix;
          for (String branch : List.of(binary, "SELECT x FROM (" + binary + ") d",
              "WITH c AS (" + binary + ") SELECT x FROM c")) {
            for (String other : List.of("SELECT 'a'", "SELECT CAST(" + value + " AS VARCHAR)",
                "SELECT CAST(" + value + " AS CLOB)", "SELECT DATE '2020-01-01'", "SELECT 1")) {
              // A WITH clause belongs at the start of the combined query.
              String body = branch.startsWith("WITH") ? "SELECT x FROM c" : branch;
              String prefix = branch.startsWith("WITH") ? "WITH c AS (" + binary + ") " : "";
              for (String sql : List.of(prefix + body + " UNION ALL " + other, prefix + other + " UNION ALL " + body)) {
                SQLException error = assertThrows(SQLException.class, () -> statement.executeQuery(sql), sql);
                assertEquals("42000", error.getSQLState(), sql);
              }
            }
          }
        }
      }
    }
  }

  /**
   * Retains binary metadata and values through nested materialization.
   *
   * @param type
   *          binary SQL declaration
   * @param jdbcType
   *          expected JDBC type
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "BINARY(2) | -2", "VARBINARY(4) | -3", "BINARY VARYING(4) | -3", "BLOB | 2004", "BINARY LARGE OBJECT | 2004"
  })
  void preservesBinaryDeclarations(String type, int jdbcType) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String value : List.of("NULL", "'xy'")) {
        for (String suffix : List.of("", " FROM mtcars WHERE 1 = 0")) {
          String body = "SELECT CAST(" + value + " AS " + type + ") AS x" + suffix;
          for (String sql : List.of(body, "SELECT x FROM (" + body + ") d", "WITH c AS (" + body + ") SELECT x FROM c",
              "WITH c AS (" + body + ") SELECT x FROM (SELECT x FROM c) d",
              "SELECT x FROM (" + body + " UNION ALL " + body + ") d")) {
            try (var result = statement.executeQuery(sql)) {
              assertEquals(jdbcType, result.getMetaData().getColumnType(1), sql);
              assertEquals(byte[].class.getName(), result.getMetaData().getColumnClassName(1), sql);
              if (suffix.isEmpty()) {
                assertTrue(result.next(), sql);
                if (value.equals("NULL")) {
                  assertNull(result.getObject(1), sql);
                } else {
                  assertArrayEquals(new byte[]{
                      120, 121
                  }, assertInstanceOf(byte[].class, result.getObject(1)), sql);
                }
              } else {
                assertFalse(result.next(), sql);
              }
            }
          }
        }
      }
    }
  }

  /**
   * Retains national literals and leaves unsupported literal prefixes unknown.
   *
   * @param expression
   *          SQL literal
   * @param jdbcType
   *          expected metadata type
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "N'abc' | -9", "n'abc' | -9", "'''abc''' | 12", "B'0101' | 1111"
  })
  void preservesLiteralPrefixes(String expression, int jdbcType) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String suffix : List.of("", " FROM mtcars WHERE 1 = 0")) {
        String sql = "SELECT " + expression + " AS x" + suffix;
        try (var result = statement.executeQuery(sql)) {
          assertEquals(jdbcType, result.getMetaData().getColumnType(1), sql);
        }
        if (jdbcType != Types.OTHER) {
          for (String nested : List.of("SELECT x FROM (" + sql + ") d", "WITH c AS (" + sql + ") SELECT x FROM c")) {
            try (var result = statement.executeQuery(nested)) {
              assertEquals(jdbcType, result.getMetaData().getColumnType(1), nested);
            }
          }
        }
      }
    }
  }

  /**
   * Promotes compatible binary branches without converting their values to text.
   *
   * @param left
   *          first binary declaration
   * @param right
   *          second binary declaration
   * @param jdbcType
   *          common JDBC type
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "BINARY(2) | VARBINARY(2) | -3", "VARBINARY(2) | BLOB | 2004", "BLOB | BINARY(2) | 2004"
  })
  void promotesBinaryBranches(String left, String right, int jdbcType) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      String first = "SELECT CAST('xy' AS " + left + ") AS x";
      String second = "SELECT CAST('xy' AS " + right + ") AS x";
      for (String body : List.of(first + " UNION " + second, second + " UNION " + first)) {
        for (String sql : List.of(body, "SELECT x FROM (" + body + ") d")) {
          try (var result = statement.executeQuery(sql)) {
            assertEquals(jdbcType, result.getMetaData().getColumnType(1), sql);
            assertTrue(result.next(), sql);
            assertArrayEquals(new byte[]{
                120, 121
            }, result.getBytes(1), sql);
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }

  /**
   * Applies fixed padding and explicit binary length truncation.
   *
   * @param type
   *          binary target declaration
   * @param expectedHex
   *          expected octets in hexadecimal
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "BINARY(4) | 78790000", "BINARY | 78", "VARBINARY(1) | 78", "VARBINARY(4) | 7879", "BLOB | 7879"
  })
  void appliesBinaryLengths(String type, String expectedHex) throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT CAST('xy' AS " + type + ")")) {
      assertTrue(result.next());
      assertArrayEquals(java.util.HexFormat.of().parseHex(expectedHex), result.getBytes(1));
    }
  }

  /**
   * Confirms that extracting scalar declarations preserves ARRAY restrictions.
   *
   * @param type
   *          unsupported ARRAY element type
   * @param canonical
   *          canonical LOB type name in diagnostics
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "CLOB | CLOB", "CHARACTER LARGE OBJECT | CLOB", "NCLOB | NCLOB", "NATIONAL CHARACTER LARGE OBJECT | NCLOB",
      "BLOB | BLOB", "BINARY LARGE OBJECT | BLOB"
  })
  void retainsArrayLobValidation(String type, String canonical) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      SQLException error = assertThrows(SQLException.class,
          () -> statement.executeQuery("SELECT ARRAY[CAST(NULL AS " + type + ")]"));
      assertEquals("0A000", error.getSQLState());
      assertTrue(error.getMessage().contains("ARRAY elements with declared type " + canonical + " are not supported"));
    }
  }

  /**
   * Untyped NULL exposes OTHER in plain, grouped, set, and materialized results.
   *
   * @param sql
   *          query projecting an untyped NULL column
   * @param rowCount
   *          expected number of NULL rows
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "SELECT NULL AS x | 1", "SELECT NULL AS x FROM mtcars | 32", "SELECT x FROM (SELECT NULL AS x) d | 1",
      "WITH c AS (SELECT NULL AS x) SELECT x FROM c | 1", "SELECT NULL AS x UNION ALL SELECT NULL | 2",
      "SELECT x FROM (SELECT NULL AS x UNION SELECT NULL) d | 1", "SELECT NULL AS x FROM mtcars WHERE 1 = 0 | 0",
      "WITH c AS (SELECT NULL AS x EXCEPT SELECT NULL) SELECT x FROM c | 0",
      "SELECT NULL AS x, COUNT(*) FROM mtcars | 1",
      "SELECT x FROM (SELECT NULL AS x) d JOIN (SELECT 1 AS n) e ON 1 = 1 | 1"
  })
  void exposesOtherForUntypedNull(String sql, int rowCount) throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertUnknownMetadata(result.getMetaData(), sql);
      for (int row = 0; row < rowCount; row++) {
        assertTrue(result.next(), sql);
        assertNull(result.getObject(1), sql);
        assertTrue(result.wasNull(), sql);
        assertUnknownMetadata(result.getMetaData(), sql);
      }
      assertFalse(result.next(), sql);
    }
  }

  /**
   * Checks all public type descriptors for an unresolved scalar projection.
   *
   * @param metadata
   *          JDBC result metadata
   * @param sql
   *          query used as assertion context
   * @throws SQLException
   *           if column metadata cannot be read
   */
  private static void assertUnknownMetadata(ResultSetMetaData metadata, String sql) throws SQLException {
    assertEquals(Types.OTHER, metadata.getColumnType(1), sql);
    assertEquals("OTHER", metadata.getColumnTypeName(1), sql);
    assertEquals(Object.class.getName(), metadata.getColumnClassName(1), sql);
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
