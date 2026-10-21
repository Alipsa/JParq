package jparq.window;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
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

/**
 * Checks window partitions for array and binary contents, NULLs, and empty
 * arrays.
 */
class WindowStructuredPartitionTest {

  /**
   * Keeps interleaved content groups together even when array text is identical.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @param directory
   *          dataset directory
   * @throws Exception
   *           if writing or querying fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void partitionsByContent(boolean predicate, boolean projection, @TempDir Path directory) throws Exception {
    Schema schema = SchemaBuilder.record("partitions").fields().requiredInt("id").name("tags").type().nullable().array()
        .items().stringType().noDefault().optionalBytes("bin").endRecord();
    Configuration configuration = new Configuration(false);
    configuration.setBoolean("parquet.avro.write-old-list-structure", true);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("parts.parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int id = 1; id <= 8; id++) {
        var record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("tags", id <= 4 ? (id % 2 == 1 ? List.of("a, b") : List.of("a", "b")) : id <= 6 ? null : List.of());
        record.put("bin", id == 5 || id == 6 ? null : ByteBuffer.wrap(new byte[]{
            (byte) 0xff, (byte) (id <= 4 ? id % 2 : 2)
        }));
        writer.write(record);
      }
    }
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection;
    try (var connection = DriverManager.getConnection(url)) {
      for (String column : List.of("tags", "bin", "tags, bin")) {
        String sql = "SELECT id, tags, bin, COUNT(*) OVER (PARTITION BY " + column + ") AS n, "
            + "ROW_NUMBER() OVER (PARTITION BY " + column + " ORDER BY id) AS rn, " + "RANK() OVER (PARTITION BY "
            + column + " ORDER BY id) AS rk FROM parts ORDER BY id";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          for (int id = 1; id <= 8; id++) {
            assertTrue(result.next(), sql);
            assertEquals(id, result.getInt(1), sql);
            if (id <= 4) {
              assertArrayEquals(id % 2 == 1 ? new Object[]{
                  "a, b"
              } : new Object[]{
                  "a", "b"
              }, assertInstanceOf(Object[].class, result.getObject(2)), sql);
              assertArrayEquals(new byte[]{
                  (byte) 0xff, (byte) (id % 2)
              }, assertInstanceOf(byte[].class, result.getObject(3)), sql);
            } else if (id <= 6) {
              assertNull(result.getObject(2), sql);
              assertNull(result.getObject(3), sql);
            } else {
              assertArrayEquals(new Object[0], assertInstanceOf(Object[].class, result.getObject(2)), sql);
              assertArrayEquals(new byte[]{
                  (byte) 0xff, 2
              }, assertInstanceOf(byte[].class, result.getObject(3)), sql);
            }
            int position = id <= 4 ? (id <= 2 ? 1 : 2) : (id % 2 == 1 ? 1 : 2);
            assertEquals(2, result.getInt(4), sql);
            assertEquals(position, result.getInt(5), sql);
            assertEquals(position, result.getInt(6), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
    }
  }
}
