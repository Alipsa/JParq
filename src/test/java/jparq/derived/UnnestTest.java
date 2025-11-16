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
 * Integration tests verifying support for the standard {@code UNNEST} table
 * function.
 */
class UnnestTest {

  private static Path tempDirectory;
  private static JParqSql jparqSql;
  private static Schema productSchema;

  @BeforeAll
  static void setUp() throws IOException {
    tempDirectory = Files.createTempDirectory("jparq-unnest-test");
    productSchema = buildSchema();
    writeProducts();
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
          // Ignore cleanup failures to avoid masking test results.
        }
      });
    }
  }

  @Test
  void unnestSimpleArray() {
    String sql = """
        SELECT
            p.name,
            tags.tag
        FROM
            products p,
            UNNEST(p.tags) AS tags(tag)
        ORDER BY
            p.name,
            tags.tag
        """;

    List<String> results = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          results.add(rs.getString("name") + ":" + rs.getString("tag"));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to process query results", e);
      }
    });

    Assertions.assertEquals(List.of("Alpha:Comfort", "Alpha:Sport", "Beta:Family"), results);
  }

  @Test
  void unnestJoinFiltersOnCondition() {
    String sql = """
        SELECT
            p.name,
            tags.tag
        FROM
            products p
            JOIN UNNEST(p.tags) AS tags(tag) ON tags.tag = 'Sport'
        ORDER BY
            p.name,
            tags.tag
        """;

    List<String> filteredTags = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          filteredTags.add(rs.getString("name") + ":" + rs.getString("tag"));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to read filtered UNNEST results", e);
      }
    });

    Assertions.assertEquals(List.of("Alpha:Sport"), filteredTags);
  }

  @Test
  void unnestWithOrdinality() {
    String sql = """
        SELECT
            p.name,
            ord.tag_position
        FROM
            products p,
            UNNEST(p.tags) WITH ORDINALITY AS ord(tag_value, tag_position)
        WHERE
            p.name = 'Alpha'
        ORDER BY
            ord.tag_position
        """;

    List<Integer> positions = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          positions.add(rs.getInt("tag_position"));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to read ordinality results", e);
      }
    });

    Assertions.assertEquals(List.of(1, 2), positions);
  }

  @Test
  void unnestArrayOfRecords() {
    String sql = """
        SELECT
            p.name,
            reviews.user,
            reviews.rating
        FROM
            products p,
            UNNEST(p.reviews) AS reviews(rating, user)
        WHERE
            reviews.rating = 5
        ORDER BY
            p.name,
            reviews.user
        """;

    List<String> results = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          results.add(rs.getString("name") + ":" + rs.getString("user") + ":" + rs.getInt("rating"));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to process review results", e);
      }
    });

    Assertions.assertEquals(List.of("Alpha:Alex:5", "Gamma:Grace:5", "Gamma:Henry:5"), results);
  }

  @Test
  void unnestSupportsTableSample() {
    String sql = """
        SELECT
            tags.tag
        FROM
            products p,
            UNNEST(p.tags) AS tags(tag)
            TABLESAMPLE SYSTEM (50 PERCENT)
            REPEATABLE (5150)
        ORDER BY
            tags.tag
        """;

    List<String> first = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          first.add(rs.getString("tag"));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to read sampled UNNEST results", e);
      }
    });

    List<String> second = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          second.add(rs.getString("tag"));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to read sampled UNNEST results", e);
      }
    });

    Assertions.assertEquals(first, second, "REPEATABLE seed should stabilise UNNEST sampling");
    Assertions.assertFalse(first.isEmpty(), "Sampling should retain at least one tag");
    Assertions.assertTrue(first.size() <= 3, "Sampling should not exceed original UNNEST rows");
  }

  private static Schema buildSchema() {
    Schema reviewSchema = SchemaBuilder.record("review").namespace("jparq.derived").fields().requiredInt("rating")
        .requiredString("user").endRecord();

    return SchemaBuilder.record("product").namespace("jparq.derived").fields().requiredInt("id").requiredString("name")
        .name("tags").type().array().items().stringType().noDefault().name("reviews").type().array().items(reviewSchema)
        .noDefault().endRecord();
  }

  private static void writeProducts() throws IOException {
    Schema reviewSchema = productSchema.getField("reviews").schema().getElementType();
    org.apache.hadoop.fs.Path parquetPath = new org.apache.hadoop.fs.Path(
        tempDirectory.resolve("products.parquet").toUri());
    Configuration conf = new Configuration(false);

    var outputFile = HadoopOutputFile.fromPath(parquetPath, conf);

    try (var writer = AvroParquetWriter.<GenericRecord>builder(outputFile).withSchema(productSchema).withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
      writer.write(buildProduct(1, "Alpha", List.of("Comfort", "Sport"),
          List.of(review(reviewSchema, 5, "Alex"), review(reviewSchema, 4, "Bert"))));
      writer.write(buildProduct(2, "Beta", List.of("Family"), List.of(review(reviewSchema, 3, "Chris"))));
      writer.write(buildProduct(3, "Gamma", List.of(),
          List.of(review(reviewSchema, 5, "Grace"), review(reviewSchema, 5, "Henry"))));
    }
  }

  private static GenericRecord buildProduct(int id, String name, List<String> tags, List<GenericRecord> reviews) {
    GenericData.Record record = new GenericData.Record(productSchema);
    record.put("id", id);
    record.put("name", name);
    record.put("tags", buildStringArray(productSchema.getField("tags").schema(), tags));
    record.put("reviews", buildReviewArray(productSchema.getField("reviews").schema(), reviews));
    return record;
  }

  private static GenericData.Array<String> buildStringArray(Schema arraySchema, List<String> values) {
    GenericData.Array<String> array = new GenericData.Array<>(values.size(), arraySchema);
    for (String value : values) {
      array.add(value);
    }
    return array;
  }

  private static GenericData.Array<GenericRecord> buildReviewArray(Schema arraySchema, List<GenericRecord> values) {
    GenericData.Array<GenericRecord> array = new GenericData.Array<>(values.size(), arraySchema);
    for (GenericRecord value : values) {
      array.add(value);
    }
    return array;
  }

  private static GenericRecord review(Schema reviewSchema, int rating, String user) {
    GenericData.Record record = new GenericData.Record(reviewSchema);
    record.put("rating", rating);
    record.put("user", user);
    return record;
  }
}
