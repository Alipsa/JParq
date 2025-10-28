package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
import se.alipsa.jparq.JParqSql;

/** Tests for the COALESCE function. */
public class CoalesceTest {

  static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws IOException {
    Path people = createPeopleParquetWithNulls();
    jparqSql = new JParqSql("jdbc:jparq:" + people.getParent().toAbsolutePath());
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
}
