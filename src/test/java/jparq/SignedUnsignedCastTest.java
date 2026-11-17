package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies that SIGNED INTEGER and SIGNED_INTEGER convert like SIGNED, while
 * UNSIGNED, UNSIGNED INTEGER, and other unrecognized CAST targets fail with
 * SQLState 42000 instead of returning the input unchanged (#322).
 */
class SignedUnsignedCastTest {

  /**
   * The SIGNED, SIGNED INTEGER, and SIGNED_INTEGER spellings declare and convert
   * a thirty-two-bit integer.
   *
   * @param spelling
   *          signed integer target spelling
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SIGNED", "SIGNED INTEGER", "SIGNED_INTEGER"
  })
  void signedSpellingsConvertToInteger(String spelling) throws Exception {
    String sql = "SELECT CAST('123' AS " + spelling + ")";
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery(sql)) {
        var meta = result.getMetaData();
        assertEquals(Types.INTEGER, meta.getColumnType(1), sql);
        assertEquals("java.lang.Integer", meta.getColumnClassName(1), sql);
        assertEquals(10, meta.getPrecision(1), sql);
        assertTrue(result.next(), sql);
        assertEquals(123, result.getInt(1), sql);
        assertFalse(result.next(), sql);
      }
    }
  }

  /**
   * Signed integer casts truncate the fractional part and keep the established
   * thirty-two-bit range, so oversized values fail like SIGNED.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void signedIntegerRetainsThirtyTwoBitRange() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT CAST(mpg AS SIGNED INTEGER) FROM mtcars LIMIT 1")) {
        assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertEquals(21, result.getInt(1));
        assertFalse(result.next());
      }
      try (var result = statement.executeQuery("SELECT CAST('10.4' AS SIGNED INTEGER)")) {
        assertTrue(result.next());
        assertEquals(10, result.getInt(1));
        assertFalse(result.next());
      }
    }
    assertRejected("22003", "value out of range", "SELECT CAST('3000000000' AS SIGNED INTEGER)");
    assertRejected("22003", "value out of range", "SELECT CAST('3000000000' AS SIGNED)");
  }

  /**
   * UNSIGNED and UNSIGNED INTEGER name no JParq target, so both literal and
   * column operands fail instead of returning the input unchanged.
   *
   * @param spelling
   *          unsigned integer target spelling
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UNSIGNED", "UNSIGNED INTEGER"
  })
  void unsignedSpellingsAreRejected(String spelling) throws Exception {
    assertRejected("42000", "Invalid data type for CAST", "SELECT CAST('123' AS " + spelling + ")");
    assertRejected("42000", "Invalid data type for CAST", "SELECT CAST(cyl AS " + spelling + ") FROM mtcars");
  }

  /**
   * Other unrecognized targets fail with the same classification.
   *
   * @param target
   *          unrecognized CAST target
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "FOOBAR", "JSON", "INTERVAL DAY"
  })
  void unrecognizedTargetsAreRejected(String target) throws Exception {
    assertRejected("42000", "Invalid data type for CAST", "SELECT CAST('1' AS " + target + ")");
  }

  /**
   * A NULL operand retains the signed declaration without failing.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void nullInputKeepsSignedDeclaration() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT CAST(NULL AS SIGNED INTEGER)")) {
        var meta = result.getMetaData();
        assertEquals(Types.INTEGER, meta.getColumnType(1));
        assertEquals("java.lang.Integer", meta.getColumnClassName(1));
        assertTrue(result.next());
        assertNull(result.getObject(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Unsigned targets inside a scalar subquery fail with the same error.
   *
   * @throws Exception
   *           if connection creation fails
   */
  @Test
  void scalarSubqueryUnsignedRejected() throws Exception {
    assertRejected("42000", "Invalid data type for CAST", "SELECT (SELECT CAST('1' AS UNSIGNED))");
  }

  /**
   * Cast-style CONVERT shares the signed declaration and the unsigned rejection
   * for both argument orders.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void convertSharesSignedAndUnsignedDeclarations() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT CONVERT(SIGNED INTEGER, '123')")) {
        var meta = result.getMetaData();
        assertEquals(Types.INTEGER, meta.getColumnType(1));
        assertEquals("java.lang.Integer", meta.getColumnClassName(1));
        assertTrue(result.next());
        assertEquals(123, result.getInt(1));
        assertFalse(result.next());
      }
    }
    assertRejected("42000", "Invalid data type for CAST", "SELECT CONVERT(UNSIGNED, '123')");
    assertRejected("42000", "Invalid data type for CAST", "SELECT CONVERT(model, UNSIGNED) FROM mtcars");
  }

  /**
   * Set operations see the declared INTEGER branch, so an empty signed branch
   * rejects a character peer while a numeric peer still merges.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void setOperationSeesDeclaredSignedIntegerBranch() throws Exception {
    assertRejected("42000", "compatible column types",
        "SELECT CAST(cyl AS SIGNED INTEGER) FROM mtcars WHERE 1=0 UNION ALL SELECT 'a'");
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement
          .executeQuery("SELECT CAST(cyl AS SIGNED INTEGER) FROM mtcars LIMIT 1 UNION ALL SELECT 7")) {
        assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertEquals(6, result.getInt(1));
        assertTrue(result.next());
        assertEquals(7, result.getInt(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Executes a query and asserts the SQLState and message of its failure.
   *
   * @param sqlState
   *          expected SQLState
   * @param expectedMessage
   *          expected message fragment
   * @param sql
   *          query expected to fail
   * @throws Exception
   *           if connection creation fails
   */
  private static void assertRejected(String sqlState, String expectedMessage, String sql) throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      SQLException error = assertThrows(SQLException.class, () -> {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          while (result.next()) {
            result.getObject(1);
          }
        }
      }, sql);
      assertEquals(sqlState, error.getSQLState(), sql);
      assertTrue(error.getMessage().contains(expectedMessage), error.getMessage());
    }
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
