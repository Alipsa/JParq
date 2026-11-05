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
   * Supplies scalar CASTs whose declared representations are not implemented.
   *
   * @return SQL expressions and their existing evaluated values
   */
  static Stream<Arguments> unresolvedCasts() {
    return Stream.of(Arguments.of("CAST('x' AS CLOB)", "x"), Arguments.of("CAST('x' AS BLOB)", "x"),
        Arguments.of("CAST('x' AS VARBINARY)", "x"), Arguments.of("CAST('x' AS BINARY(2))", "x"),
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
      "CLOB", "BLOB", "BINARY(2)", "VARBINARY"
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
      "CLOB"
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
   * current CAST evaluation returns String values, including empty branches.
   *
   * @param type
   *          binary CAST declaration
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "BLOB", "BINARY(2)", "VARBINARY"
  })
  void rejectsIncompatibleBinaryCastBranches(String type) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String value : List.of("NULL", "'x'")) {
        for (String suffix : List.of("", " FROM mtcars WHERE 1 = 0")) {
          String binary = "SELECT CAST(" + value + " AS " + type + ") AS x" + suffix;
          for (String other : List.of("SELECT CAST(" + value + " AS VARCHAR)", "SELECT CAST(" + value + " AS CLOB)",
              "SELECT DATE '2020-01-01'", "SELECT 1")) {
            for (String sql : List.of(binary + " UNION ALL " + other, other + " UNION ALL " + binary)) {
              SQLException error = assertThrows(SQLException.class, () -> statement.executeQuery(sql), sql);
              assertEquals("42000", error.getSQLState(), sql);
            }
          }
        }
      }
    }
  }

  /**
   * Confirms that extracting scalar declarations preserves ARRAY restrictions.
   *
   * @param type
   *          unsupported ARRAY element type
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CLOB", "BLOB"
  })
  void retainsArrayLobValidation(String type) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      SQLException error = assertThrows(SQLException.class,
          () -> statement.executeQuery("SELECT ARRAY[CAST(NULL AS " + type + ")]"));
      assertEquals("0A000", error.getSQLState());
      assertTrue(error.getMessage().contains("ARRAY elements"));
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
