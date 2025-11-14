package se.alipsa.jparq;

import java.nio.file.Path;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JParqDatabaseMetaDataHintParsingTest {

  @Test
  void preservesEmptyEntriesInColumnTypeHints() {
    List<String> hints = JParqDatabaseMetaData.parseColumnTypeHints("java.lang.String,,java.lang.Integer");
    Assertions.assertEquals(3, hints.size());
    Assertions.assertEquals("java.lang.String", hints.get(0));
    Assertions.assertEquals("", hints.get(1));
    Assertions.assertEquals("java.lang.Integer", hints.get(2));
  }

  @Test
  void trimsWhitespaceAroundHints() {
    List<String> hints = JParqDatabaseMetaData.parseColumnTypeHints("  java.lang.Long  ,  java.lang.Double ");
    Assertions.assertEquals(List.of("java.lang.Long", "java.lang.Double"), hints);
  }

  @Test
  void handlesNullOrBlankInputs() {
    Assertions.assertTrue(JParqDatabaseMetaData.parseColumnTypeHints(null).isEmpty());
    Assertions.assertTrue(JParqDatabaseMetaData.parseColumnTypeHints("   ").isEmpty());
  }

  @Test
  void readsColumnTypeHintsFromParquetFooter(@TempDir Path tempDir) throws Exception {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);
    Path file = tempDir.resolve("hinted.parquet");
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());

    Schema schema = SchemaBuilder.record("HintedRecord").fields().requiredString("value").endRecord();
    Map<String, String> metadata = Map.of("matrix.columnTypes", "java.lang.Integer");
    try (ParquetWriter<GenericRecord> writer =
        AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(hadoopPath, conf)).withConf(conf)
        .withSchema(schema).withExtraMetaData(metadata).build()) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("value", "42");
      writer.write(record);
    }

    try (JParqConnection connection = new JParqConnection("jdbc:jparq:" + tempDir.toUri().toString(),
        new Properties())) {
      DatabaseMetaData metaData = connection.getMetaData();
      try (ResultSet columns = metaData.getColumns(null, null, "hinted", null)) {
        Assertions.assertTrue(columns.next(), "No column metadata was returned");
        Assertions.assertEquals(Types.INTEGER, columns.getInt("DATA_TYPE"));
      }
    }
  }
}
