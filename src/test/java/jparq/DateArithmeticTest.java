package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** JDBC regressions for date arithmetic and materialized day counts. */
class DateArithmeticTest {

  @TempDir
  Path directory;

  /**
   * Writes a physical DATE column, including a NULL value.
   *
   * @throws Exception
   *           if the fixture cannot be written
   */
  @BeforeEach
  void writeDates() throws Exception {
    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema schema = SchemaBuilder.record("dates").fields().requiredInt("id").name("d").type().unionOf().nullType().and()
        .type(date).endUnion().nullDefault().endRecord();
    Configuration configuration = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("small.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int id = 0; id < 2; id++) {
        var record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("d", id == 0 ? (int) LocalDate.of(2024, 3, 1).toEpochDay() : null);
        writer.write(record);
      }
    }
  }

  /**
   * Rejects unsupported operands at JDBC boundaries with a classified error.
   *
   * @param expression
   *          unsupported arithmetic to execute
   * @throws Exception
   *           if opening the connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "d + 1", "DATE '2024-01-01' + 1", "1 + d", "d - 1", "1 - d", "d * 2", "d / 2", "d + d"
  })
  void unsupportedArithmeticRaisesSqlException(String expression) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT " + expression + " AS x FROM small WHERE id = 0",
          "SELECT t.x FROM (SELECT " + expression + " AS x FROM small WHERE id = 0) t",
          "SELECT id FROM small WHERE id = 0 AND " + expression + " IS NOT NULL")) {
        var failure = assertThrows(SQLException.class, () -> {
          try (var result = statement.executeQuery(sql)) {
            result.next();
            result.getObject(1);
          }
        }, sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("Unsupported temporal arithmetic"), failure::getMessage);
      }
    }
  }

  /**
   * Preserves signed calendar-day counts through derived tables and CTEs.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void dateDifferencesRemainNumeric() throws Exception {
    String projection = "SELECT d - DATE '2024-02-28' AS forward_days, "
        + "DATE '2024-02-28' - d AS backward_days, d - d AS zero_days FROM small";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of(projection, "SELECT * FROM (" + projection + ") t",
          "WITH t AS (" + projection + ") SELECT * FROM t")) {
        try (var result = statement.executeQuery(sql)) {
          assertTrue(result.next());
          assertEquals(2L, result.getObject(1));
          assertEquals(-2L, result.getObject(2));
          assertEquals(0L, result.getObject(3));
          assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1));
          assertTrue(result.next());
          assertNull(result.getObject(1));
          assertTrue(result.wasNull());
          assertNull(result.getObject(2));
          assertNull(result.getObject(3));
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Reports day-count metadata before reading rows, including empty results and
   * aliases that shadow a source date.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void dayCountMetadataDoesNotDependOnRows() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT d - d AS d FROM small WHERE id = 99",
          "SELECT (d) - CAST('2024-03-01' AS DATE) AS d FROM small WHERE id = 99",
          "SELECT * FROM (SELECT d - d AS d FROM small WHERE id = 99) t",
          "SELECT DATE '2024-03-01' - DATE '2024-02-28' AS d")) {
        try (var result = statement.executeQuery(sql)) {
          assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1), sql);
          assertEquals(Long.class.getName(), result.getMetaData().getColumnClassName(1), sql);
          if (result.next()) {
            assertEquals(2L, result.getObject(1));
          }
        }
      }
    }
  }

  /**
   * Preserves explicit interval offsets and NULL propagation.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void intervalOffsetsAndNullsStillWork() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT d + INTERVAL '1 day', d - INTERVAL '1 day', "
            + "INTERVAL '1 day' + d, d + NULL, d + 1 FROM small WHERE id = 1")) {
      assertTrue(result.next());
      for (int column = 1; column <= 5; column++) {
        assertNull(result.getObject(column));
      }
    }
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(
            "SELECT d + INTERVAL '1 day', d - INTERVAL '1 day', " + "INTERVAL '1 day' + d FROM small WHERE id = 0")) {
      assertTrue(result.next());
      assertEquals(Date.valueOf("2024-03-02"), result.getObject(1));
      assertEquals(Date.valueOf("2024-02-29"), result.getObject(2));
      assertEquals(Date.valueOf("2024-03-02"), result.getObject(3));
    }
  }
}
