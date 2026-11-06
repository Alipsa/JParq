package jparq.setops;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
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
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies declared nonnumeric types and untyped NULL across set boundaries.
 */
class SetDeclaredTypeTest {

  /**
   * Set declaration checks reject invalid branches independently of row data.
   * Earlier preparation-time checks remain tracked in issue 464.
   *
   * @param expression
   *          invalid set branch projection
   * @param diagnostic
   *          expected declaration or compatibility diagnostic
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "CONVERT(VARCHAR, TEXT) | Ambiguous CONVERT arguments",
      "CAST(model AS VARBINARY) | Set operation components must use compatible column types"
  })
  void rejectsSetDeclarationsAtExecution(String expression, String diagnostic) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath();
    try (var connection = DriverManager.getConnection(url)) {
      for (String suffix : List.of("", " WHERE 1=0")) {
        String invalid = "SELECT " + expression + " AS c FROM mtcars" + suffix;
        String text = "SELECT model FROM mtcars" + suffix;
        for (String sql : List.of(invalid + " UNION " + text, text + " UNION " + invalid)) {
          try (var statement = connection.prepareStatement(sql)) {
            SQLException error = assertThrows(SQLException.class, statement::executeQuery, sql);
            assertEquals("42000", error.getSQLState(), sql);
            assertTrue(error.getMessage().contains(diagnostic), error.getMessage());
          }
        }
      }
    }
  }

  @TempDir
  Path directory;

  /**
   * Combines physical temporal columns with literals and casts in either order.
   *
   * @param type
   *          temporal SQL type
   * @param literal
   *          temporal literal value
   * @param jdbcType
   *          expected JDBC type
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "DATE | 2026-01-02 | 91", "TIME | 10:11:12 | 92", "TIMESTAMP | 2026-01-02 10:11:12 | 93"
  })
  void combinesPhysicalTemporalBranches(String type, String literal, int jdbcType) throws Exception {
    Schema valueSchema = switch (jdbcType) {
      case Types.DATE -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
      case Types.TIME -> LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
      default -> LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    };
    var schema = SchemaBuilder.record("temporal").fields().name("x").type(valueSchema).noDefault().endRecord();
    var conf = new Configuration(false);
    var path = new org.apache.hadoop.fs.Path(directory.resolve("temporal.parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(path, conf)).withSchema(schema)
        .withConf(conf).build()) {
      var row = new GenericData.Record(schema);
      row.put("x", jdbcType == Types.TIMESTAMP ? (Object) 0L : 0);
      writer.write(row);
    }
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String expression : List.of(type + " '" + literal + "'", "CAST('" + literal + "' AS " + type + ")")) {
        for (String sql : List.of("SELECT x FROM temporal UNION ALL SELECT " + expression,
            "SELECT " + expression + " UNION ALL SELECT x FROM temporal")) {
          try (var result = statement.executeQuery(sql)) {
            assertEquals(jdbcType, result.getMetaData().getColumnType(1), sql);
            assertTrue(result.next(), sql);
            assertNotNull(result.getObject(1), sql);
            assertTrue(result.next(), sql);
            assertNotNull(result.getObject(1), sql);
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }

  /**
   * Checks declarations before reading rows, including empty and NULL results.
   *
   * @param expression
   *          literal or CAST projection
   * @param type
   *          expected JDBC type
   * @param className
   *          expected value class
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "'''a''' | 12 | java.lang.String", "CAST(cyl AS VARCHAR) | 12 | java.lang.String",
      "CAST(model AS CHAR(3)) | 1 | java.lang.String", "CAST(NULL AS VARCHAR) | 12 | java.lang.String",
      "DATE '2026-01-02' | 91 | java.sql.Date", "TIME '10:11:12' | 92 | java.sql.Time",
      "TIMESTAMP '2026-01-02 10:11:12' | 93 | java.sql.Timestamp", "CAST(NULL AS DATE) | 91 | java.sql.Date",
      "CAST(NULL AS TIME) | 92 | java.sql.Time", "CAST(NULL AS TIMESTAMP) | 93 | java.sql.Timestamp",
      "CAST(123 AS NATIONAL CHARACTER(5)) | -15 | java.lang.String",
      "CAST(123 AS NATIONAL CHAR(5)) | -15 | java.lang.String",
      "CAST(123 AS NATIONAL CHARACTER VARYING(5)) | -9 | java.lang.String",
      "CAST('abcdef' AS NATIONAL CHARACTER(5)) | -15 | java.lang.String",
      "CAST('abcdef' AS NATIONAL CHAR(5)) | -15 | java.lang.String",
      "CAST('abcdef' AS NATIONAL CHARACTER VARYING(5)) | -9 | java.lang.String"
  })
  void retainsProjectionMetadata(String expression, int type, String className) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String predicate : List.of("model = 'Mazda RX4'", "1 = 0")) {
        String body = "SELECT " + expression + " AS x FROM mtcars WHERE " + predicate;
        for (String sql : List.of(body, "SELECT x FROM (" + body + ") d", "WITH c AS (" + body + ") SELECT x FROM c")) {
          try (var result = statement.executeQuery(sql)) {
            assertEquals(type, result.getMetaData().getColumnType(1), sql);
            assertEquals(className, result.getMetaData().getColumnClassName(1), sql);
            if (predicate.equals("1 = 0")) {
              assertFalse(result.next(), sql);
            } else {
              assertTrue(result.next(), sql);
              Object value = result.getObject(1);
              if (expression.contains("NULL")) {
                assertNull(value, sql);
              } else {
                assertEquals(className, value.getClass().getName(), sql);
                if (expression.contains("NATIONAL")) {
                  assertEquals(expression.contains("123") ? "123" : "abcde", value, sql);
                }
              }
              assertFalse(result.next(), sql);
            }
          }
        }
      }
    }
  }

  /**
   * Combines character declarations with physical VARCHAR columns in both orders.
   *
   * @param expression
   *          character expression
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "'a'", "CAST(cyl AS VARCHAR)", "CAST(model AS CHAR(3))", "CAST(NULL AS VARCHAR)"
  })
  void combinesCharacterBranches(String expression) throws Exception {
    for (boolean pushdown : List.of(true, false)) {
      try (
          var connection = DriverManager
              .getConnection(url() + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
          var statement = connection.createStatement()) {
        String branch = "SELECT " + expression + " AS x FROM mtcars WHERE model = 'Mazda RX4'";
        String physical = "SELECT model FROM mtcars WHERE model = 'Mazda RX4'";
        for (String sql : List.of(branch + " UNION ALL " + physical, physical + " UNION ALL " + branch)) {
          try (var result = statement.executeQuery(sql)) {
            assertEquals(Types.VARCHAR, result.getMetaData().getColumnType(1), sql);
            assertEquals(String.class.getName(), result.getMetaData().getColumnClassName(1), sql);
            String value = switch (expression) {
              case "'a'" -> "a";
              case "CAST(cyl AS VARCHAR)" -> "6";
              case "CAST(model AS CHAR(3))" -> "Maz";
              default -> null;
            };
            boolean physicalFirst = sql.startsWith(physical);
            assertTrue(result.next(), sql);
            assertEquals(physicalFirst ? "Mazda RX4" : value, result.getObject(1), sql);
            assertTrue(result.next(), sql);
            assertEquals(physicalFirst ? value : "Mazda RX4", result.getObject(1), sql);
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }

  /**
   * Retains national and varying character declarations when combining casts.
   *
   * @param left
   *          first character type
   * @param right
   *          second character type
   * @param expected
   *          expected common JDBC type
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "CHAR, VARCHAR, 12", "CHAR, NCHAR, -15", "CHAR, NVARCHAR, -9", "NCHAR, VARCHAR, -9"
  })
  void promotesCharacterDeclarations(String left, String right, int expected) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String body : List.of("SELECT CAST('a' AS " + left + ") AS x UNION ALL SELECT CAST('b' AS " + right + ")",
          "SELECT CAST('b' AS " + right + ") AS x UNION ALL SELECT CAST('a' AS " + left + ")")) {
        for (String sql : List.of(body, "SELECT x FROM (" + body + ") d")) {
          try (var result = statement.executeQuery(sql)) {
            assertEquals(expected, result.getMetaData().getColumnType(1), sql);
            assertTrue(result.next(), sql);
            assertEquals(body.startsWith("SELECT CAST('a'") ? "a" : "b", result.getObject(1), sql);
            assertTrue(result.next(), sql);
            assertEquals(body.startsWith("SELECT CAST('a'") ? "b" : "a", result.getObject(1), sql);
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }

  /**
   * National fixed-character long forms use NCHAR's default length and enforce
   * the same numeric truncation rule.
   *
   * @param type
   *          fixed national character spelling
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "NCHAR", "NATIONAL CHARACTER", "NATIONAL CHAR"
  })
  void retainsNationalCharacterDefaultLength(String type) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT CAST('ab' AS " + type + ")")) {
        assertEquals(Types.NCHAR, result.getMetaData().getColumnType(1));
        assertTrue(result.next());
        assertEquals("a", result.getObject(1));
        assertFalse(result.next());
      }
      SQLException error = assertThrows(SQLException.class, () -> {
        try (var result = statement.executeQuery("SELECT CAST(12 AS " + type + ")")) {
          assertTrue(result.next());
          result.getObject(1);
        }
      });
      assertEquals("22001", error.getSQLState());
    }
  }

  /**
   * Preserves untyped NULL through nested materialization, including empty sets.
   *
   * @param body
   *          NULL-only query
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT NULL AS x UNION SELECT NULL AS x", "SELECT NULL AS x INTERSECT SELECT NULL AS x",
      "SELECT NULL AS x EXCEPT SELECT NULL AS x",
      "SELECT NULL AS x FROM mtcars WHERE 1 = 0 UNION SELECT NULL AS x FROM mtcars WHERE 1 = 0", "SELECT NULL AS x"
  })
  void preservesUntypedNull(String body) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String source : List.of("SELECT x FROM (" + body + ") d", "WITH c AS (" + body + ") SELECT x FROM c",
          "WITH c AS (" + body + "), d AS (SELECT x FROM c) SELECT x FROM d")) {
        for (String sql : List.of(source + " UNION ALL SELECT 1 AS x",
            "SELECT 1 AS x UNION ALL SELECT x FROM (" + source + ") n")) {
          try (var result = statement.executeQuery(sql)) {
            assertEquals(Types.INTEGER, result.getMetaData().getColumnType(1), sql);
            boolean foundOne = false;
            int nullCount = 0;
            while (result.next()) {
              Object value = result.getObject(1);
              if (value != null) {
                assertEquals(1, value, sql);
                foundOne = true;
              } else {
                nullCount++;
              }
            }
            assertTrue(foundOne, sql);
            assertEquals(body.contains("EXCEPT") || body.contains("1 = 0") ? 0 : 1, nullCount, sql);
          }
        }
      }
    }
  }

  /**
   * A declared nonnumeric NULL value remains incompatible with numeric branches.
   *
   * @param cast
   *          typed NULL declaration
   * @throws Exception
   *           if connection creation fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "VARCHAR", "CHAR(3)", "DATE", "TIME", "TIMESTAMP", "CLOB", "BLOB", "VARBINARY", "BINARY(2)"
  })
  void rejectsIncompatibleTypedNull(String cast) throws Exception {
    try (var connection = DriverManager.getConnection(url()); var statement = connection.createStatement()) {
      for (String suffix : List.of("", " FROM mtcars WHERE model = 'Mazda RX4'", " FROM mtcars WHERE 1 = 0")) {
        String body = "SELECT CAST(NULL AS " + cast + ") AS x" + suffix;
        for (String branch : List.of(body, "SELECT x FROM (" + body + ") d",
            "WITH c AS (" + body + ") SELECT x FROM c")) {
          for (String sql : List.of(branch + " UNION ALL SELECT 1",
              "SELECT 1 UNION ALL SELECT x FROM (" + branch + ") r")) {
            SQLException error = assertThrows(SQLException.class, () -> statement.executeQuery(sql), sql);
            assertEquals("42000", error.getSQLState(), sql);
          }
        }
      }
    }
  }

  /**
   * Returns the shared fixture database URL.
   *
   * @return JDBC connection URL
   */
  private String url() {
    return "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath();
  }
}
