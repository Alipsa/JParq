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

/** Tests for IS NULL and IS NOT NULL operations. */
public class IsNullTest {

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

    Path dir = Files.createTempDirectory("jparq-nulltest-");
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
    return file; // parent dir is your JDBC “database” path
  }

  @Test
  void testIsNull() {

    jparqSql.query("SELECT name, age FROM people WHERE age IS NULL", rs -> {
      List<String> names = new ArrayList<>();
      try {
        while (rs.next()) {
          names.add(rs.getString("name"));
        }
        assertEquals(List.of("Bob"), names, "Only Bob should have null age");
      } catch (SQLException e) {
        System.err.println(String.join("\n", names));
        fail(e);
      }
    });
  }

  @Test
  void testIsNotNull() {

    jparqSql.query("SELECT name, age FROM people WHERE name IS NOT NULL", rs -> {
      List<String> names = new ArrayList<>();
      try {
        while (rs.next()) {
          names.add(rs.getString("name"));
        }
        assertEquals(List.of("Alice", "Bob"), names, "Both Alice and Bob have non-null names");
      } catch (SQLException e) {
        System.err.println(String.join("\n", names));
        fail(e);
      }
    });
  }
}
