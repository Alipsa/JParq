package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.ArrayQueryValidation;
import se.alipsa.jparq.engine.JSqlParserSupport;
import se.alipsa.jparq.engine.SqlComparisons;
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
      "CONCAT('a', CAST('xy' AS VARBINARY))"
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
      "1", "1.5", "TRUE", "DATE '2020-01-01'", "TIME '10:00:00'", "CAST(NULL AS INTEGER)", "cyl", "cyl + 1",
      "INTERVAL '1' DAY"
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
      "BINARY(0)", "VARBINARY(0)", "BLOB(0)", "BINARY(-1)", "VARBINARY(2147483648)", "BLOB(0K)", "BLOB(8192P)",
      "BINARY(1K)", "BLOB(1Q)"
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
   * LOB multipliers retain long bounds without allocating the declared capacity.
   *
   * @param type
   *          valid binary type declaration
   * @param jdbcType
   *          expected JDBC type
   * @throws Exception
   *           if preparation or execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "BLOB(1K) | 2004", "BLOB(1M) | 2004", "BINARY LARGE OBJECT(2K) | 2004", "BINARY LARGE OBJECT(2M) | 2004",
      "BLOB(2G) | 2004", "BLOB(1T) | 2004", "BLOB(1P) | 2004", "BLOB(8191P) | 2004", "BLOB(10) | 2004",
      "VARBINARY(4) | -3", "BINARY(2) | -2"
  })
  void acceptsValidLengthsDuringPreparation(String type, int jdbcType) throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      for (String value : List.of("NULL", "'xy'")) {
        for (String suffix : List.of("", " FROM mtcars WHERE 1=0")) {
          String sql = "SELECT CAST(" + value + " AS " + type + ") AS x" + suffix;
          try (var query = connection.prepareStatement(sql); var result = query.executeQuery()) {
            assertEquals(jdbcType, result.getMetaData().getColumnType(1), sql);
            if (suffix.isEmpty()) {
              assertTrue(result.next(), sql);
              if (value.equals("NULL")) {
                assertNull(result.getBytes(1), sql);
              } else {
                assertArrayEquals(new byte[]{
                    120, 121
                }, result.getBytes(1), sql);
              }
            } else {
              assertFalse(result.next(), sql);
            }
          }
        }
      }
    }
  }

  /**
   * A K multiplier limits the value to 1024 octets, rather than decimal
   * kilobytes.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void appliesLobMultiplierCap() throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT CAST(REPEAT('x', 1025) AS BLOB(1K))")) {
      assertTrue(result.next());
      assertEquals(1024, result.getBytes(1).length);
    }
  }

  /**
   * Known binary misuse fails before empty or skipped inputs can hide it.
   *
   * @param expression
   *          expression with incompatible known operand declarations
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CHAR_LENGTH(CAST(model AS VARBINARY))", "UPPER(CAST(model AS VARBINARY))", "LOWER(CAST(model AS VARBINARY))",
      "CONCAT('x', CAST(model AS VARBINARY))", "CAST(INTERVAL '1' DAY AS VARBINARY)",
      "NULLIF(CAST(model AS VARBINARY), 'x')", "NULLIF(model, CAST('x' AS VARBINARY))",
      "NULLIF(CAST(NULL AS VARBINARY), 'x')"
  })
  void rejectsBinaryMisuseBeforeReadingRows(String expression) throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      for (String suffix : List.of("", " WHERE 1=0")) {
        String sql = "SELECT " + expression + " FROM mtcars" + suffix;
        assertEquals("42000",
            assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql).getSQLState());
      }
    }
  }

  /**
   * NULLIF retains binary values or SQL NULL for compatible declarations.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void evaluatesCompatibleBinaryNullif() throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT NULLIF(CAST('x' AS VARBINARY), CAST('y' AS VARBINARY)), "
            + "NULLIF(CAST('x' AS VARBINARY), CAST('x' AS VARBINARY))")) {
      assertTrue(result.next());
      assertArrayEquals(new byte[]{
          120
      }, assertInstanceOf(byte[].class, result.getObject(1)));
      assertNull(result.getObject(2));
      assertTrue(result.wasNull());
    }
  }

  /**
   * Candidate-list size does not increase left scalar-subquery schema resolution.
   *
   * @throws Exception
   *           if expression parsing fails
   */
  @Test
  void resolvesInLeftSubqueryIndependentlyOfListSize() throws Exception {
    var sourceSchema = SchemaBuilder.record("binary_source").fields().requiredBytes("b").endRecord();
    Integer baseline = null;
    for (int size : List.of(1, 50)) {
      var reads = new AtomicInteger();
      String candidates = String.join(", ", Collections.nCopies(size, "X'78'"));
      var expression = JSqlParserSupport.parseExpression("(SELECT b FROM binary_source) IN (" + candidates + ")");
      ArrayQueryValidation.validate(expression, column -> null, table -> {
        reads.incrementAndGet();
        return sourceSchema;
      });
      assertTrue(reads.get() > 0);
      if (baseline != null) {
        assertEquals(baseline.intValue(), reads.get());
      }
      baseline = reads.get();
    }
  }

  /**
   * Binary comparison diagnostics describe incompatible types instead of text
   * conversion.
   *
   * @param other
   *          incompatible operand
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "'x'", "1", "DATE '2020-01-01'", "INTERVAL '1' DAY"
  })
  void rejectsKnownIncompatibleComparisons(String other) throws Exception {
    for (boolean pushdown : List.of(true, false)) {
      try (var connection = DriverManager.getConnection(url() + "?predicatePushdown=" + pushdown)) {
        for (String predicate : List.of("CAST(model AS VARBINARY) = " + other, other + " < CAST(model AS VARBINARY)")) {
          for (String suffix : List.of("", " AND 1=0")) {
            String sql = "SELECT model FROM mtcars WHERE " + predicate + suffix;
            SQLException error = assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql);
            assertEquals("42000", error.getSQLState(), sql);
            assertTrue(error.getMessage().contains("Cannot compare binary and nonbinary"), error.getMessage());
          }
        }
      }
    }
  }

  /**
   * Predicate forms compare declarations before rows or CASE branches are
   * evaluated.
   *
   * @param predicate
   *          incompatible binary/nonbinary predicate
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CAST(model AS VARBINARY) IN ('x')", "CAST(model AS VARBINARY) NOT IN ('x', NULL)",
      "CAST(model AS VARBINARY) IN (SELECT model FROM mtcars)", "CAST(model AS VARBINARY) BETWEEN 'a' AND 'z'",
      "CAST(model AS VARBINARY) NOT BETWEEN X'61' AND 'z'", "CAST(model AS VARBINARY) = ANY (SELECT model FROM mtcars)",
      "CAST(model AS VARBINARY) = SOME (SELECT model FROM mtcars)",
      "CAST(model AS VARBINARY) > ALL (SELECT model FROM mtcars WHERE 1=0)",
      "CAST(model AS VARBINARY) = ANY (ARRAY['x'])", "CAST(model AS VARBINARY) LIKE 'x%'",
      "'x' LIKE CAST(model AS VARBINARY)"
  })
  void rejectsIncompatiblePredicateFormsDuringPreparation(String predicate) throws Exception {
    for (boolean pushdown : List.of(true, false)) {
      try (var connection = DriverManager.getConnection(url() + "?predicatePushdown=" + pushdown)) {
        for (String suffix : List.of("", " AND 1=0")) {
          String sql = "SELECT model FROM mtcars WHERE " + predicate + suffix;
          assertEquals("42000",
              assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql).getSQLState());
        }
      }
    }
  }

  /**
   * Simple CASE validates its switch/WHEN comparison even when no rows survive.
   *
   * @throws Exception
   *           if connection creation fails
   */
  @Test
  void rejectsIncompatibleCaseSwitchDuringPreparation() throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      for (String sql : List.of("SELECT CASE CAST(model AS VARBINARY) WHEN 'x' THEN 1 END FROM mtcars WHERE 1=0",
          "SELECT CASE model WHEN CAST('x' AS VARBINARY) THEN 1 END FROM mtcars WHERE 1=0")) {
        assertEquals("42000",
            assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql).getSQLState());
      }
    }
  }

  /**
   * Compatible binary predicates retain content comparison and NULL semantics.
   *
   * @param expression
   *          compatible binary predicate or CASE result
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "X'78' IN (X'78', X'79')", "X'78' BETWEEN X'61' AND X'7A'", "X'78' = ANY (SELECT CAST('x' AS VARBINARY))",
      "X'78' = ANY (ARRAY[X'78'])", "CASE X'78' WHEN X'78' THEN TRUE ELSE FALSE END"
  })
  void acceptsCompatibleBinaryPredicateForms(String expression) throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + expression)) {
      assertTrue(result.next());
      assertTrue(result.getBoolean(1));
    }
  }

  /**
   * Octet LIKE has a valid binary signature; its execution support remains in
   * #461.
   *
   * @throws Exception
   *           if preparing the valid signature fails
   */
  @Test
  void acceptsBinaryLikeSignatureDuringPreparation() throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var query = connection.prepareStatement("SELECT model FROM mtcars WHERE CAST(model AS VARBINARY) LIKE X'78'")) {
      assertNotNull(query);
    }
  }

  /**
   * Malformed UTF-8 raises a repertoire error rather than inserting replacement
   * characters.
   *
   * @param hex
   *          malformed UTF-8 octets
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "FF", "C328", "E282", "EDA080"
  })
  void rejectsMalformedUtf8(String hex) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String body : List.of("SELECT X'" + hex + "' AS x", "SELECT CAST(X'" + hex + "' AS VARBINARY) AS x")) {
        for (String sql : List.of("SELECT CAST(X'" + hex + "' AS VARCHAR)",
            "SELECT CAST(x AS VARCHAR) FROM (" + body + ") d")) {
          SQLException error = assertThrows(SQLException.class, () -> {
            try (var result = statement.executeQuery(sql)) {
              while (result.next()) {
                result.getObject(1);
              }
            }
          }, sql);
          assertEquals("22021", error.getSQLState(), sql);
        }
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

  /** Verifies strict buffer decoding and comparison preserve buffer state. */
  @Test
  void rejectsMalformedBuffersAndDescribesIncompatibleValues() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[]{
        0, (byte) 0xff, 0
    });
    buffer.position(1);
    buffer.limit(2);
    var decoding = assertThrows(IllegalArgumentException.class, () -> SqlText.cast(buffer));
    assertEquals("22021", assertInstanceOf(SQLException.class, decoding.getCause()).getSQLState());
    assertEquals(1, buffer.position());
    assertEquals(2, buffer.limit());
    var comparison = assertThrows(IllegalArgumentException.class, () -> SqlComparisons.typedCompare(buffer, 1));
    assertTrue(comparison.getMessage().contains("Cannot compare binary and nonbinary"));
    assertEquals(0, SqlComparisons.typedCompare(buffer, new byte[]{
        (byte) 0xff
    }));
    assertEquals(1, buffer.position());
  }

  /**
   * Returns the fixture database URL.
   *
   * @return JDBC connection URL
   */
  private static String url() {
    return "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath();
  }

  /**
   * A JDBC getter reports invalid UTF-8 elements of an ARRAY as a
   * SQLDataException (SQLState 22021) rather than an unchecked
   * IllegalArgumentException.
   *
   * @throws Exception
   *           if connection creation fails
   */
  @Test
  void getStringOnBinaryArrayThrowsSqlDataException() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT ARRAY[X'FF']")) {
        assertTrue(result.next());
        SQLException failure = assertThrows(SQLDataException.class, () -> result.getString(1));
        assertEquals("22021", failure.getSQLState());
      }
      try (var result = statement.executeQuery("SELECT ARRAY[ARRAY[X'FF']]")) {
        assertTrue(result.next());
        SQLException failure = assertThrows(SQLDataException.class, () -> result.getString(1));
        assertEquals("22021", failure.getSQLState());
      }
    }
  }
}
