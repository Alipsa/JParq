package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Random;
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
import se.alipsa.jparq.engine.function.NumericFunctions;

/** Tests for SQL numeric functions. */
public class NumericFunctionsTest {

  private static final double EPSILON = 1e-9;

  static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws IOException {
    Path numbers = createNumbersParquet();
    jparqSql = new JParqSql("jdbc:jparq:" + numbers.getParent().toAbsolutePath());
  }

  @Test
  void testBasicNumericFunctions() {
    String sql = "SELECT ABS(val) AS abs_val, CEILING(val) AS ceil_val, FLOOR(val) AS floor_val, "
        + "ROUND(3.125, 2) AS round_pos_scale, ROUND(1250, -2) AS round_neg_scale, "
        + "TRUNCATE(3.875, 2) AS trunc_pos_scale, TRUNC(-3.875, 2) AS trunc_neg_number, "
        + "TRUNCATE(987.5, -2) AS trunc_neg_scale, TRUNC(val2) AS trunc_default, MOD(val2, 2) AS mod_val, "
        + "SQRT(16) AS sqrt_val FROM numbers";

    jparqSql.query(sql, rs -> {
      try {
        assertTrue(rs.next());

        assertEquals(3.75, rs.getDouble("abs_val"), EPSILON, "ABS should return magnitude");
        assertEquals(-3.0, rs.getDouble("ceil_val"), EPSILON, "CEILING should round toward +inf");
        assertEquals(-4.0, rs.getDouble("floor_val"), EPSILON, "FLOOR should round toward -inf");

        assertEquals(3.13, rs.getDouble("round_pos_scale"), EPSILON, "ROUND should respect positive scale");
        assertEquals(1300.0, rs.getDouble("round_neg_scale"), EPSILON, "ROUND should support negative scale");

        assertEquals(3.87, rs.getDouble("trunc_pos_scale"), EPSILON, "TRUNCATE should drop fractional digits");
        assertEquals(-3.87, rs.getDouble("trunc_neg_number"), EPSILON, "TRUNC should truncate toward zero");
        assertEquals(900.0, rs.getDouble("trunc_neg_scale"), EPSILON, "TRUNCATE should support negative scale");
        assertEquals(2.0, rs.getDouble("trunc_default"), EPSILON, "TRUNC should default to zero decimals");

        assertEquals(0.5, rs.getDouble("mod_val"), EPSILON, "MOD should produce remainder");
        assertEquals(4.0, rs.getDouble("sqrt_val"), EPSILON, "SQRT should compute square root");

        assertFalse(rs.next());
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testPowerAndLogFunctions() {
    String sql = "SELECT POWER(2, 3) AS pow_val, POW(4, 0.5) AS pow_alias, EXP(1) AS exp_val, "
        + "LOG(2.718281828459045) AS ln_val, LOG(10, 1000) AS log_base, LOG10(100) AS log10_val FROM numbers";

    jparqSql.query(sql, rs -> {
      try {
        assertTrue(rs.next());

        assertEquals(8.0, rs.getDouble("pow_val"), EPSILON, "POWER should raise to exponent");
        assertEquals(2.0, rs.getDouble("pow_alias"), EPSILON, "POW should be supported as alias");
        assertEquals(Math.E, rs.getDouble("exp_val"), 1e-6, "EXP(1) should equal e");
        assertEquals(1.0, rs.getDouble("ln_val"), 1e-6, "LOG should default to natural log");
        assertEquals(3.0, rs.getDouble("log_base"), EPSILON, "LOG with base should be supported");
        assertEquals(2.0, rs.getDouble("log10_val"), EPSILON, "LOG10 should compute base-10 logarithm");

        assertFalse(rs.next());
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testRandomAndSignFunctions() {
    String sql = "SELECT RAND() AS random_val, RAND(42) AS seeded_val, SIGN(val) AS neg_sign, "
        + "SIGN(0) AS zero_sign, SIGN(val2) AS pos_sign FROM numbers";

    double expectedSeeded = new Random(42).nextDouble();

    jparqSql.query(sql, rs -> {
      try {
        assertTrue(rs.next());

        double randomVal = rs.getDouble("random_val");
        assertTrue(randomVal >= 0.0 && randomVal < 1.0, "RAND() should return value in [0,1)");
        assertEquals(expectedSeeded, rs.getDouble("seeded_val"), EPSILON, "RAND(seed) should be deterministic");

        assertEquals(-1, rs.getInt("neg_sign"), "SIGN of negative value should be -1");
        assertEquals(0, rs.getInt("zero_sign"), "SIGN of zero should be 0");
        assertEquals(1, rs.getInt("pos_sign"), "SIGN of positive value should be 1");

        assertFalse(rs.next());
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testTrigonometricFunctions() {
    String sql = """
        SELECT SIN(angle) AS sin_val, COS(angle) AS cos_val, TAN(angle) AS tan_val,
        ASIN(unit) AS asin_val, ACOS(unit) AS acos_val, ATAN(unit) AS atan_val,
        ATAN2(y, x) AS atan2_val FROM numbers
        """;

    jparqSql.query(sql, rs -> {
      try {
        assertTrue(rs.next());

        assertEquals(Math.sqrt(0.5), rs.getDouble("sin_val"), 1e-9, "SIN should operate on radians");
        assertEquals(Math.sqrt(0.5), rs.getDouble("cos_val"), 1e-9, "COS should operate on radians");
        assertEquals(1.0, rs.getDouble("tan_val"), 1e-9, "TAN of pi/4 should be 1");

        assertEquals(Math.asin(0.5), rs.getDouble("asin_val"), EPSILON, "ASIN should invert SIN");
        assertEquals(Math.acos(0.5), rs.getDouble("acos_val"), EPSILON, "ACOS should invert COS");
        assertEquals(Math.atan(0.5), rs.getDouble("atan_val"), EPSILON, "ATAN should compute arctangent");
        assertEquals(Math.atan2(4.0, 3.0), rs.getDouble("atan2_val"), EPSILON, "ATAN2 should use y,x arguments");

        assertFalse(rs.next());
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void testAngleConversionFunctions() {
    String sql = "SELECT DEGREES(angle) AS deg_val, RADIANS(45) AS rad_val FROM numbers";

    jparqSql.query(sql, rs -> {
      try {
        assertTrue(rs.next());

        assertEquals(45.0, rs.getDouble("deg_val"), EPSILON, "DEGREES should convert from radians");
        assertEquals(Math.PI / 4, rs.getDouble("rad_val"), EPSILON, "RADIANS should convert to radians");

        assertFalse(rs.next());
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  private static Path createNumbersParquet() throws IOException {
    String schemaJson = """
        {
          "type": "record",
          "name": "Numbers",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "val", "type": "double"},
            {"name": "val2", "type": "double"},
            {"name": "unit", "type": "double"},
            {"name": "angle", "type": "double"},
            {"name": "x", "type": "double"},
            {"name": "y", "type": "double"}
          ]
        }
        """;

    Schema schema = new Schema.Parser().parse(schemaJson);

    Path dir = Files.createTempDirectory("jparq-numeric-");
    Path file = dir.resolve("numbers.parquet");

    Configuration conf = new Configuration(false);
    org.apache.hadoop.fs.Path hPath = new org.apache.hadoop.fs.Path(file.toUri());
    OutputFile out = HadoopOutputFile.fromPath(hPath, conf);

    try (var writer = AvroParquetWriter.<GenericRecord>builder(out).withSchema(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build()) {

      GenericRecord record = new GenericData.Record(schema);
      record.put("id", 1);
      record.put("val", -3.75);
      record.put("val2", 2.5);
      record.put("unit", 0.5);
      record.put("angle", Math.PI / 4);
      record.put("x", 3.0);
      record.put("y", 4.0);
      writer.write(record);
    }

    return file;
  }

  @Test
  public void testToBigDecimal() {
    assertEquals(BigDecimal.valueOf(1.23), NumericFunctions.toBigDecimal("1.23"));
    assertEquals(BigDecimal.valueOf(1), NumericFunctions.toBigDecimal(1));
    assertEquals(BigDecimal.valueOf(7), NumericFunctions.toBigDecimal((byte) 7));
    assertEquals(BigDecimal.valueOf(1.33), NumericFunctions.toBigDecimal(1.33));
    assertEquals(BigDecimal.valueOf(9999), NumericFunctions.toBigDecimal(BigInteger.valueOf(9999)));
    assertNull(NumericFunctions.toBigDecimal(null));
  }
}
