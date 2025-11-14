package jparq.derived;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests verifying support for {@code LATERAL} derived tables.
 */
class LateralTest {

  private static Path tempDirectory;
  private static JParqSql jparqSql;
  private static Schema customerSchema;
  private static Schema orderSchema;

  @BeforeAll
  static void setUp() throws IOException {
    tempDirectory = Files.createTempDirectory("jparq-lateral-test");
    customerSchema = buildCustomerSchema();
    orderSchema = buildOrderSchema();
    writeCustomers();
    writeOrders();
    jparqSql = new JParqSql("jdbc:jparq:" + tempDirectory.toAbsolutePath());
  }

  @AfterAll
  static void tearDown() throws IOException {
    if (tempDirectory == null) {
      return;
    }
    try (Stream<Path> paths = Files.walk(tempDirectory)) {
      paths.sorted((left, right) -> right.compareTo(left)).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (IOException e) {
          // Best-effort cleanup to avoid masking test results.
        }
      });
    }
  }

  @Test
  void lateralTopOrderPerCustomer() {
    String sql = """
        SELECT
            c.name,
            latest.order_amount
        FROM
            customers c
            JOIN LATERAL (
                SELECT o.amount AS order_amount
                FROM orders o
                WHERE o.customer_id = c.id
                ORDER BY o.amount DESC
                LIMIT 1
            ) latest ON TRUE
        ORDER BY c.id
        """;

    List<String> results = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          results.add(rs.getString("name") + ":" + rs.getDouble("order_amount"));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to evaluate lateral top order query", e);
      }
    });

    Assertions.assertEquals(List.of("Alice:150.0", "Bob:200.0"), results);
  }

  @Test
  void leftJoinLateralIncludesCustomersWithoutOrders() {
    String sql = """
        SELECT
            c.name,
            latest.order_amount
        FROM
            customers c
            LEFT JOIN LATERAL (
                SELECT o.amount AS order_amount
                FROM orders o
                WHERE o.customer_id = c.id
                ORDER BY o.amount DESC
                LIMIT 1
            ) latest ON TRUE
        ORDER BY c.id
        """;

    List<String> results = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          Double amount = (Double) rs.getObject("order_amount");
          results.add(rs.getString("name") + ":" + (amount == null ? "null" : amount.toString()));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to evaluate lateral left join", e);
      }
    });

    Assertions.assertEquals(List.of("Alice:150.0", "Bob:200.0", "Carol:null"), results);
  }

  @Test
  void crossJoinLateralAggregatesPerCustomer() {
    String sql = """
        SELECT
            c.name,
            stats.order_count
        FROM
            customers c
            CROSS JOIN LATERAL (
                SELECT COUNT(*) AS order_count
                FROM orders o
                WHERE o.customer_id = c.id
            ) stats
        ORDER BY c.id
        """;

    List<String> counts = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          counts.add(rs.getString("name") + ":" + rs.getLong("order_count"));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to evaluate lateral aggregation", e);
      }
    });

    Assertions.assertEquals(List.of("Alice:2", "Bob:1", "Carol:0"), counts);
  }

  private static Schema buildCustomerSchema() {
    return SchemaBuilder.record("customer").namespace("jparq.derived").fields().requiredInt("id").requiredString("name")
        .endRecord();
  }

  private static Schema buildOrderSchema() {
    return SchemaBuilder.record("order").namespace("jparq.derived").fields().requiredInt("id")
        .requiredInt("customer_id").requiredDouble("amount").endRecord();
  }

  private static void writeCustomers() throws IOException {
    var outputPath = new org.apache.hadoop.fs.Path(tempDirectory.resolve("customers.parquet").toUri());
    Configuration conf = new Configuration(false);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(outputPath, conf))
        .withSchema(customerSchema).withConf(conf).withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
      writer.write(buildCustomer(1, "Alice"));
      writer.write(buildCustomer(2, "Bob"));
      writer.write(buildCustomer(3, "Carol"));
    }
  }

  private static void writeOrders() throws IOException {
    var outputPath = new org.apache.hadoop.fs.Path(tempDirectory.resolve("orders.parquet").toUri());
    Configuration conf = new Configuration(false);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(outputPath, conf))
        .withSchema(orderSchema).withConf(conf).withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
      writer.write(buildOrder(101, 1, 120.0));
      writer.write(buildOrder(102, 1, 150.0));
      writer.write(buildOrder(201, 2, 200.0));
    }
  }

  private static GenericRecord buildCustomer(int id, String name) {
    GenericData.Record record = new GenericData.Record(customerSchema);
    record.put("id", id);
    record.put("name", name);
    return record;
  }

  private static GenericRecord buildOrder(int id, int customerId, double amount) {
    GenericData.Record record = new GenericData.Record(orderSchema);
    record.put("id", id);
    record.put("customer_id", customerId);
    record.put("amount", amount);
    return record;
  }
}
