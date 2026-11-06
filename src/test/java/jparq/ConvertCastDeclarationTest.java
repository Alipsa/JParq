package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies cast-style CONVERT shares CAST declarations and planning checks. */
class ConvertCastDeclarationTest {

  /**
   * Type-named correlated columns remain visible when resolving CONVERT.
   *
   * @param suffix
   *          optional empty-input filter
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "", " WHERE 1=0"
  })
  void resolvesCorrelatedTypeNamedColumn(String suffix) throws Exception {
    String sql = "SELECT (SELECT CONVERT(VARCHAR, integer) FROM mtcars LIMIT 1) AS c " + "FROM (SELECT 7 AS integer) d"
        + suffix;
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      if (suffix.isEmpty()) {
        assertTrue(result.next());
        assertEquals("7", result.getObject(1));
      }
      assertFalse(result.next());
    }
  }

  /**
   * Correlated conversion reads each outer binding, including SQL NULL.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void convertsEachCorrelatedTypeNamedBinding() throws Exception {
    String sql = "SELECT (SELECT CONVERT(VARCHAR, integer) FROM mtcars LIMIT 1) AS c "
        + "FROM (SELECT 7 AS integer UNION ALL SELECT 9 UNION ALL SELECT CAST(NULL AS INTEGER)) d";
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertTrue(result.next());
      assertEquals("7", result.getObject(1));
      assertTrue(result.next());
      assertEquals("9", result.getObject(1));
      assertTrue(result.next());
      assertNull(result.getObject(1));
      assertTrue(result.wasNull());
      assertFalse(result.next());
    }
  }

  /**
   * Function operands whose names are SQL types remain value expressions.
   *
   * @param source
   *          optional table clause
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "", " FROM mtcars LIMIT 1"
  })
  void preservesCharFunctionOperand(String source) throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT CONVERT(VARCHAR, CHAR(65)) AS c" + source)) {
      assertEquals(java.sql.Types.VARCHAR, result.getMetaData().getColumnType(1));
      assertEquals("c", result.getMetaData().getColumnLabel(1));
      assertTrue(result.next());
      assertEquals("A", result.getObject(1));
      assertFalse(result.next());
    }
  }

  /**
   * Ambiguous declarations fail preparation even when no rows will be read.
   *
   * @param source
   *          optional table or empty source clause
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "", " FROM mtcars LIMIT 1", " FROM mtcars WHERE 1=0"
  })
  void rejectsAmbiguousConversionsDuringPreparation(String source) throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      String sql = "SELECT CONVERT(VARCHAR, INTEGER)" + source;
      SQLException error = assertThrows(SQLException.class, () -> connection.prepareStatement(sql));
      assertEquals("42000", error.getSQLState(), error.getMessage());
      if (source.isEmpty()) {
        assertTrue(error.getMessage().contains("Ambiguous CONVERT arguments"), error.getMessage());
      }
    }
  }

  /**
   * Two bound type-named columns cannot silently select either argument order.
   *
   * @throws Exception
   *           if connection creation fails
   */
  @Test
  void rejectsAmbiguousBoundColumnsDuringPreparation() throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      String sql = "SELECT CONVERT(integer, varchar) FROM (SELECT 1 AS integer, 'x' AS varchar) d WHERE 1=0";
      SQLException error = assertThrows(SQLException.class, () -> connection.prepareStatement(sql));
      assertEquals("42000", error.getSQLState());
      assertTrue(error.getMessage().contains("Ambiguous CONVERT arguments"), error.getMessage());
    }
  }

  /**
   * Shared declarations preserve decimal bounds before reading any rows.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void preservesDecimalDeclarations() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String predicate : List.of("model='Mazda RX4'", "1=0")) {
        String sql = "SELECT CONVERT(DECIMAL(10,2), mpg) AS mpg FROM mtcars WHERE " + predicate;
        try (var result = statement.executeQuery(sql)) {
          assertEquals(java.sql.Types.DECIMAL, result.getMetaData().getColumnType(1), sql);
          assertEquals(10, result.getMetaData().getPrecision(1), sql);
          assertEquals(2, result.getMetaData().getScale(1), sql);
          assertEquals(java.math.BigDecimal.class.getName(), result.getMetaData().getColumnClassName(1), sql);
          if (predicate.equals("1=0")) {
            assertFalse(result.next(), sql);
          } else {
            assertTrue(result.next(), sql);
            assertEquals(new java.math.BigDecimal("21.00"), result.getObject(1), sql);
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }

  /**
   * Binary declarations survive alias collisions and materialization before row
   * reads.
   *
   * @param type
   *          binary SQL type
   * @param jdbcType
   *          expected JDBC metadata type
   * @param length
   *          expected non-null value length
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "VARBINARY | -3 | 9", "BINARY(12) | -2 | 12", "BLOB(1K) | 2004 | 9"
  })
  void preservesBinaryDeclarations(String type, int jdbcType, int length) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String value : List.of("model", "NULL")) {
        List<String> expressions = value.equals("NULL")
            ? List.of("CONVERT(" + type + ", NULL)")
            : List.of("CONVERT(" + value + ", " + type + ")", "CONVERT(" + type + ", " + value + ")");
        for (String expression : expressions) {
          for (String predicate : List.of("model='Mazda RX4'", "1=0")) {
            String body = "SELECT " + expression + " AS model FROM mtcars WHERE " + predicate;
            for (String sql : List.of(body, "SELECT model FROM (" + body + ") d",
                "WITH c AS (" + body + ") SELECT model FROM c")) {
              try (var result = statement.executeQuery(sql)) {
                assertEquals(jdbcType, result.getMetaData().getColumnType(1), sql);
                assertEquals(byte[].class.getName(), result.getMetaData().getColumnClassName(1), sql);
                if (predicate.equals("1=0")) {
                  assertFalse(result.next(), sql);
                } else {
                  assertTrue(result.next(), sql);
                  if (value.equals("NULL")) {
                    assertNull(result.getObject(1), sql);
                  } else {
                    byte[] expected = Arrays.copyOf("Mazda RX4".getBytes(StandardCharsets.UTF_8), length);
                    assertArrayEquals(expected, assertInstanceOf(byte[].class, result.getObject(1)), sql);
                  }
                  assertFalse(result.next(), sql);
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Invalid declarations and known incompatible uses fail without reading rows.
   *
   * @param expression
   *          invalid conversion or use of a binary conversion
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CONVERT(model, BINARY(0))", "CONVERT(BINARY(0), model)", "CONVERT(BINARY(0), NULL)", "CONVERT(cyl, VARBINARY)",
      "CONVERT(VARBINARY, cyl)", "CONVERT(VARBINARY, TRUE)", "CHAR_LENGTH(CONVERT(model, VARBINARY))",
      "UPPER(CONVERT(VARBINARY, model))", "CONVERT(model, VARBINARY) = 'x'", "CONVERT(VARBINARY, model) = 1",
      "NULLIF(CONVERT(model, VARBINARY), 'x')"
  })
  void rejectsInvalidConversionsDuringPreparation(String expression) throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      for (String suffix : List.of("", " WHERE 1=0")) {
        String sql = "SELECT " + expression + " FROM mtcars" + suffix;
        assertEquals("42000",
            assertThrows(SQLException.class, () -> connection.prepareStatement(sql), sql).getSQLState());
      }
    }
  }

  /**
   * WHERE comparisons validate equivalent CONVERT declarations before filtering.
   *
   * @throws Exception
   *           if connection creation fails
   */
  @Test
  void rejectsIncompatibleWhereComparisonDuringPreparation() throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      String sql = "SELECT model FROM mtcars WHERE CONVERT(model, VARBINARY) = 'x' AND 1=0";
      assertEquals("42000", assertThrows(SQLException.class, () -> connection.prepareStatement(sql)).getSQLState());
    }
  }

  /**
   * Binary CONVERT branches retain their declaration through set validation.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void validatesSetBranchesAndPreservesLabels() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      String binary = "SELECT CONVERT(VARBINARY, 'x') AS x";
      SQLException error = assertThrows(SQLException.class,
          () -> statement.executeQuery(binary + " UNION ALL SELECT 'x'"));
      assertEquals("42000", error.getSQLState());
      try (var result = statement.executeQuery("SELECT CONVERT(VARBINARY, 'x')")) {
        assertEquals("CONVERT( VARBINARY, 'x' )", result.getMetaData().getColumnLabel(1));
        assertEquals(java.sql.Types.VARBINARY, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertArrayEquals(new byte[]{
            120
        }, result.getBytes(1));
      }
    }
  }

  /**
   * Type-named source columns do not acquire an incorrect early numeric
   * declaration.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void resolvesTypeNamedColumnAfterBinding() throws Exception {
    try (var connection = DriverManager.getConnection(url());
        var statement = connection.createStatement();
        var result = statement
            .executeQuery("SELECT ARRAY[CONVERT(integer, VARCHAR), 'x'] " + "FROM (SELECT 1 AS integer) d")) {
      assertTrue(result.next());
      var array = result.getArray(1);
      try {
        assertArrayEquals(new Object[]{
            "1", "x"
        }, assertInstanceOf(Object[].class, array.getArray()));
      } finally {
        array.free();
      }
    }
  }

  /**
   * Generic projection validation does not mislabel a binary comparison as ARRAY.
   *
   * @throws Exception
   *           if connection creation fails
   */
  @Test
  void usesNeutralProjectionErrorPrefix() throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      String sql = "SELECT a.model FROM (SELECT * FROM mtcars WHERE 1=0) a JOIN mtcars b "
          + "ON CAST(a.model AS VARBINARY) = b.model";
      SQLException error = assertThrows(SQLException.class, () -> {
        try (var result = statement.executeQuery(sql)) {
          while (result.next()) {
            result.getObject(1);
          }
        }
      });
      assertEquals("42000", error.getSQLState());
      assertTrue(error.getMessage().contains("Invalid projection declaration"), error.getMessage());
      assertFalse(error.getMessage().contains("Invalid ARRAY projection declaration"), error.getMessage());
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
