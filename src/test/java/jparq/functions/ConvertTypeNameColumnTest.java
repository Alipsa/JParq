package jparq.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Resolves cast-style CONVERT against physical columns whose names spell SQL
 * type names, so column extraction agrees with evaluation.
 */
class ConvertTypeNameColumnTest {

  private static final Schema CONV_SCHEMA = SchemaBuilder.record("conv").fields().requiredInt("id").requiredLong("int8")
      .requiredInt("int4").requiredDouble("float8").requiredBoolean("bool").requiredString("text")
      .requiredString("note").endRecord();

  private static final Schema AMBIGUOUS_SCHEMA = SchemaBuilder.record("amb").fields().requiredInt("id")
      .requiredString("varchar").requiredLong("int8").endRecord();

  @TempDir
  static Path directory;

  /**
   * Writes fixture tables containing columns named after SQL type names.
   *
   * @throws Exception
   *           if writing the fixture fails
   */
  @BeforeAll
  static void writeFixture() throws Exception {
    var configuration = new Configuration(false);
    List<Map<String, Object>> convRows = List.of(
        Map.of("id", 1, "int8", 10L, "int4", 100, "float8", 1.5d, "bool", true, "text", "ten", "note", "first"),
        Map.of("id", 2, "int8", 20L, "int4", 200, "float8", 2.5d, "bool", false, "text", "twenty", "note", "second"));
    writeTable("conv.parquet", CONV_SCHEMA, configuration, convRows);
    List<Map<String, Object>> ambiguousRows = List.of(Map.of("id", 1, "varchar", "ten", "int8", 10L));
    writeTable("amb.parquet", AMBIGUOUS_SCHEMA, configuration, ambiguousRows);
  }

  private static void writeTable(String name, Schema schema, Configuration configuration,
      List<Map<String, Object>> rows) throws Exception {
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve(name).toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (Map<String, Object> row : rows) {
        var record = new GenericData.Record(schema);
        row.forEach(record::put);
        writer.write(record);
      }
    }
  }

  /**
   * Converts physical columns named after SQL type names in both argument orders.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void convertsTypeNamedColumns(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown="
        + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String column : List.of("int8", "int4", "float8")) {
        for (String type : List.of("VARCHAR", "DOUBLE")) {
          assertConvertMatchesCast(statement, column, type);
        }
      }
      for (String column : List.of("bool", "text")) {
        assertConvertMatchesCast(statement, column, "VARCHAR");
      }
      // The regression from issue 324: the type position must not be read as a
      // column when the operand is a physical column named after a type.
      String sql = "SELECT CONVERT(VARCHAR, int8) AS v, CONVERT(DOUBLE, int8) AS d, CONVERT(VARCHAR, note) AS n "
          + "FROM conv ORDER BY id";
      try (var result = statement.executeQuery(sql)) {
        assertTrue(result.next(), sql);
        assertEquals("10", result.getObject(1), sql);
        assertEquals(10.0d, result.getObject(2), sql);
        assertEquals("first", result.getObject(3), sql);
        assertTrue(result.next(), sql);
        assertEquals("20", result.getObject(1), sql);
        assertEquals(20.0d, result.getObject(2), sql);
        assertEquals("second", result.getObject(3), sql);
        assertFalse(result.next(), sql);
      }
    }
  }

  /**
   * Compares both CONVERT argument orders against the equivalent CAST.
   *
   * @param statement
   *          statement used for the comparison queries
   * @param column
   *          physical column named after a SQL type
   * @param type
   *          cast target type
   * @throws Exception
   *           if query execution fails
   */
  private static void assertConvertMatchesCast(Statement statement, String column, String type) throws Exception {
    for (String arguments : List.of(type + ", " + column, column + ", " + type)) {
      String sql = "SELECT CONVERT(" + arguments + ") AS converted, CAST(" + column + " AS " + type
          + ") AS casted, id FROM conv ORDER BY id";
      try (var result = statement.executeQuery(sql)) {
        for (int row = 1; row <= 2; row++) {
          assertTrue(result.next(), sql);
          assertEquals(result.getObject(2), result.getObject(1),
              sql + " row " + row + ": CONVERT should behave like CAST");
        }
        assertFalse(result.next(), sql);
      }
    }
  }

  /**
   * Extracts type-named columns referenced through cast-style CONVERT in WHERE,
   * GROUP BY, and ORDER BY clauses.
   *
   * @param predicatePushdown
   *          whether predicate pushdown is enabled
   * @param projectionPushdown
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void extractsTypeNamedColumnsFromClauses(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicatePushdown + "&projectionPushdown="
        + projectionPushdown;
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("SELECT id FROM conv WHERE CONVERT(VARCHAR, int8) = '10' ORDER BY id")) {
        assertTrue(result.next());
        assertEquals(1, result.getObject(1));
        assertFalse(result.next());
      }
      try (var result = statement.executeQuery("SELECT CONVERT(VARCHAR, int8) AS v, COUNT(*) AS c FROM conv "
          + "GROUP BY CONVERT(VARCHAR, int8) ORDER BY v")) {
        assertTrue(result.next());
        assertEquals("10", result.getObject(1));
        assertEquals(1L, result.getObject(2));
        assertTrue(result.next());
        assertEquals("20", result.getObject(1));
        assertEquals(1L, result.getObject(2));
        assertFalse(result.next());
      }
      try (var result = statement.executeQuery("SELECT id FROM conv ORDER BY CONVERT(VARCHAR, int8) DESC")) {
        assertTrue(result.next());
        assertEquals(2, result.getObject(1));
        assertTrue(result.next());
        assertEquals(1, result.getObject(1));
        assertFalse(result.next());
      }
      try (var result = statement.executeQuery(
          "SELECT a.note FROM conv a JOIN amb b ON a.text = b.varchar AND a.int8 = CONVERT(BIGINT, b.int8) "
              + "ORDER BY a.id")) {
        assertTrue(result.next());
        assertEquals("first", result.getObject(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Reports ambiguous CONVERT arguments instead of a phantom missing column when
   * both type positions name physical columns.
   *
   * @throws Exception
   *           if connection creation fails
   */
  @Test
  void rejectsAmbiguousTypeNamedColumnsDuringPreparation() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      SQLException error = assertThrows(SQLException.class,
          () -> connection.prepareStatement("SELECT CONVERT(VARCHAR, int8) FROM amb"));
      assertEquals("42000", error.getSQLState());
      assertTrue(error.getMessage().contains("Ambiguous CONVERT arguments"), error.getMessage());
    }
  }
}
