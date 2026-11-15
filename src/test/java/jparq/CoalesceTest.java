package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import se.alipsa.jparq.JParqSql;

/** Tests for the COALESCE function. */
public class CoalesceTest {

  static JParqSql jparqSql;
  private static Path peopleDirectory;

  @BeforeAll
  static void setup() throws IOException {
    Path people = createPeopleParquetWithNulls();
    peopleDirectory = people.getParent().toAbsolutePath();
    jparqSql = new JParqSql("jdbc:jparq:" + peopleDirectory);
  }

  static Path createPeopleParquetWithNulls() throws IOException {
    String schemaJson = """
        {
          "type": "record",
          "name": "Person",
          "fields": [
            {"name": "name", "type": ["null", "string"], "default": null},
            {"name": "age",  "type": ["null", "int"],    "default": null}
          ]
        }
        """;
    Schema schema = new Schema.Parser().parse(schemaJson);

    Path dir = Files.createTempDirectory("jparq-coalesce-");
    Path file = dir.resolve("people.parquet");

    Configuration conf = new Configuration(false);
    org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(file.toUri());
    OutputFile out = HadoopOutputFile.fromPath(hPath, conf);

    try (var writer = AvroParquetWriter.<GenericRecord>builder(out).withSchema(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build()) {

      GenericRecord r1 = new GenericData.Record(schema);
      r1.put("name", "Alice");
      r1.put("age", 30);
      writer.write(r1);

      GenericRecord r2 = new GenericData.Record(schema);
      r2.put("name", "Bob");
      r2.put("age", null);
      writer.write(r2);

      GenericRecord r3 = new GenericData.Record(schema);
      r3.put("name", null);
      r3.put("age", 25);
      writer.write(r3);
    }
    return file;
  }

  @Test
  void testCoalesceInSelectList() {
    String sql = "SELECT name, age, COALESCE(name, 'Unknown') AS display_name, "
        + "COALESCE(age, 99) AS display_age FROM people";
    jparqSql.query(sql, rs -> {
      List<String> displayNames = new ArrayList<>();
      List<Integer> displayAges = new ArrayList<>();
      try {
        while (rs.next()) {
          String name = rs.getString("name");
          boolean nameWasNull = rs.wasNull();
          Number age = (Number) rs.getObject("age");
          boolean ageWasNull = rs.wasNull();

          String displayName = rs.getString("display_name");
          int displayAge = rs.getInt("display_age");

          if (nameWasNull) {
            assertEquals("Unknown", displayName, "Null name should fall back to Unknown");
          } else {
            assertEquals(name, displayName, "COALESCE should preserve existing names");
          }

          if (ageWasNull) {
            assertEquals(99, displayAge, "Null age should fall back to 99");
          } else {
            assertEquals(age.intValue(), displayAge, "COALESCE should preserve existing ages");
          }

          displayNames.add(displayName);
          displayAges.add(displayAge);
        }
        assertEquals(List.of("Alice", "Bob", "Unknown"), displayNames, "Display names should match expected order");
        assertEquals(List.of(30, 99, 25), displayAges, "Display ages should match expected order");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testCoalesceInWhereClause() {
    String sql = "SELECT COALESCE(name, 'Unknown') AS display_name FROM people "
        + "WHERE COALESCE(name, 'Unknown') = 'Unknown'";
    jparqSql.query(sql, rs -> {
      List<String> displayNames = new ArrayList<>();
      try {
        while (rs.next()) {
          displayNames.add(rs.getString("display_name"));
        }
        assertEquals(List.of("Unknown"), displayNames, "Only the row with null name should remain");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  /**
   * Supplies common numeric types and selected/fallback values for COALESCE.
   *
   * @return COALESCE expression, equivalent CASE, input rows, JDBC type,
   *         precision, scale, and expected values
   */
  static Stream<Arguments> numericCommonTypes() {
    String integers = "(VALUES (1), (CAST(NULL AS INTEGER))) t(id)";
    return Stream.of(
        Arguments.of("COALESCE(id, 1.5)", "CASE WHEN id IS NOT NULL THEN id ELSE 1.5 END", integers, Types.DECIMAL, 11,
            1, List.of(new BigDecimal("1.0"), new BigDecimal("1.5"))),
        Arguments.of("COALESCE(id, CAST(7 AS BIGINT))", "CASE WHEN id IS NOT NULL THEN id ELSE CAST(7 AS BIGINT) END",
            integers, Types.BIGINT, 19, 0, List.of(1L, 7L)),
        Arguments.of("COALESCE(CAST(id AS REAL), 1)",
            "CASE WHEN CAST(id AS REAL) IS NOT NULL THEN CAST(id AS REAL) ELSE 1 END",
            "(VALUES (1.25), (CAST(NULL AS DECIMAL(3,2)))) t(id)", Types.DOUBLE, -1, 0, List.of(1.25, 1.0)),
        Arguments.of("COALESCE(NULL, id, 2.5)", "CASE WHEN id IS NOT NULL THEN id ELSE 2.5 END", integers,
            Types.DECIMAL, 11, 1, List.of(new BigDecimal("1.0"), new BigDecimal("2.5"))),
        Arguments.of("COALESCE(COALESCE(id, 1), 2.5)",
            "CASE WHEN id IS NOT NULL THEN id WHEN 1 IS NOT NULL THEN 1 ELSE 2.5 END", integers, Types.DECIMAL, 11, 1,
            List.of(new BigDecimal("1.0"), new BigDecimal("1.0"))));
  }

  /**
   * Matches numeric CASE promotion for plain and derived-table COALESCE,
   * including empty inputs and selection of either argument.
   *
   * @param expression
   *          COALESCE expression
   * @param equivalentCase
   *          explicit equivalent CASE expression
   * @param input
   *          typed input rows
   * @param type
   *          expected JDBC type
   * @param precision
   *          expected precision, or -1 for approximate types
   * @param scale
   *          expected scale
   * @param values
   *          expected selected and fallback values
   * @throws SQLException
   *           if query execution or metadata access fails
   */
  @ParameterizedTest
  @MethodSource("numericCommonTypes")
  void numericCommonTypesMatchCase(String expression, String equivalentCase, String input, int type, int precision,
      int scale, List<Object> values) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + peopleDirectory);
        var statement = connection.createStatement()) {
      for (boolean empty : List.of(false, true)) {
        String query = "SELECT " + expression + " AS v, " + equivalentCase + " AS expected FROM " + input
            + (empty ? " WHERE 1 = 0" : "");
        for (boolean derived : List.of(false, true)) {
          try (var result = statement.executeQuery(derived ? "SELECT v, expected FROM (" + query + ") d" : query)) {
            assertNumericDeclaration(result.getMetaData(), type, values.getFirst().getClass(), precision, scale);
            int rows = 0;
            while (result.next()) {
              assertEquals(values.get(rows), result.getObject(1));
              assertEquals(result.getObject(2), result.getObject(1));
              rows++;
            }
            assertEquals(empty ? 0 : values.size(), rows);
          }
        }
      }
    }
  }

  /**
   * Combines equal numeric values from differently typed COALESCE arguments for
   * DISTINCT and GROUP BY, preserving the common decimal value.
   *
   * @throws SQLException
   *           if query execution or metadata access fails
   */
  @Test
  void distinctAndGroupingUsePromotedNumericValues() throws SQLException {
    String input = """
        FROM (VALUES (CAST(1 AS INTEGER), CAST(NULL AS DECIMAL(3,1))),
                     (CAST(NULL AS INTEGER), CAST(1.0 AS DECIMAL(3,1)))) t(x, y)
        """;
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + peopleDirectory);
        var statement = connection.createStatement()) {
      for (boolean grouped : List.of(false, true)) {
        String query = grouped
            ? "SELECT COALESCE(x, y) AS v, COUNT(*) AS n " + input + " GROUP BY COALESCE(x, y)"
            : "SELECT DISTINCT COALESCE(x, y) AS v " + input;
        for (boolean derived : List.of(false, true)) {
          try (var result = statement.executeQuery(derived ? "SELECT * FROM (" + query + ") d" : query)) {
            assertNumericDeclaration(result.getMetaData(), Types.DECIMAL, BigDecimal.class, 11, 1);
            assertTrue(result.next());
            assertEquals(new BigDecimal("1.0"), result.getObject(1));
            if (grouped) {
              assertEquals(2L, result.getObject(2));
            }
            assertFalse(result.next(), "Equal promoted values must produce one result row");
          }
        }
      }
    }
  }

  /**
   * Promotes grouped Parquet aggregate results to the COALESCE common type.
   *
   * @throws SQLException
   *           if query execution or metadata access fails
   */
  @Test
  void groupedNumericCoalesce() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + peopleDirectory);
        var statement = connection.createStatement()) {
      String query = "SELECT COALESCE(MAX(age), 1.5) AS v FROM people GROUP BY name";
      for (boolean derived : List.of(false, true)) {
        try (var result = statement
            .executeQuery((derived ? "SELECT v FROM (" + query + ") d" : query) + " ORDER BY v")) {
          assertNumericDeclaration(result.getMetaData(), Types.DECIMAL, BigDecimal.class, 11, 1);
          for (String value : List.of("1.5", "25.0", "30.0")) {
            assertTrue(result.next());
            assertEquals(new BigDecimal(value), result.getObject(1));
          }
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Checks #524's character CAST and JSON rendering against promoted values and
   * the equivalent CASE expression.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void promotedNumericCoalesceFormatting() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + peopleDirectory);
        var statement = connection.createStatement()) {
      String query = """
          SELECT CAST(COALESCE(id, 1.5) AS VARCHAR(20)) AS text_value,
                 JSON_ARRAY(COALESCE(id, 1.5)) AS json_value,
                 JSON_ARRAY(CASE WHEN id IS NOT NULL THEN id ELSE 1.5 END) AS case_value
          FROM (VALUES (1), (CAST(NULL AS INTEGER))) t(id)
          """;
      for (boolean derived : List.of(false, true)) {
        try (var result = statement.executeQuery(derived ? "SELECT * FROM (" + query + ") d" : query)) {
          for (String value : List.of("1.0", "1.5")) {
            assertTrue(result.next());
            assertEquals(value, result.getString(1));
            assertEquals("[" + value + "]", result.getString(2));
            assertEquals(result.getString(3), result.getString(2));
          }
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Checks a promoted numeric column before reading its values.
   *
   * @param metadata
   *          result metadata
   * @param type
   *          expected JDBC type
   * @param valueClass
   *          expected Java class
   * @param precision
   *          expected precision, or -1 to omit the check
   * @param scale
   *          expected scale
   * @throws SQLException
   *           if metadata access fails
   */
  private static void assertNumericDeclaration(ResultSetMetaData metadata, int type, Class<?> valueClass, int precision,
      int scale) throws SQLException {
    assertEquals(type, metadata.getColumnType(1));
    assertEquals(valueClass.getName(), metadata.getColumnClassName(1));
    assertEquals(scale, metadata.getScale(1));
    if (precision >= 0) {
      assertEquals(precision, metadata.getPrecision(1));
    }
  }
}
