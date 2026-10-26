package jparq.setops;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers decimal bounds, literal declarations, and mixed numeric
 * multiplicities.
 */
class SetNumericBoundaryTest {

  @TempDir
  Path directory;

  /**
   * Widens computed decimal bounds despite aliases hiding the source metadata.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void arithmeticAliasesUseDeclaredDecimalBounds(boolean pushdown) throws Exception {
    var decimal = LogicalTypes.decimal(5, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    var schema = SchemaBuilder.record("p").fields().name("dec").type(decimal).noDefault().endRecord();
    var conf = new Configuration(false);
    var path = new org.apache.hadoop.fs.Path(directory.resolve("p.parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(path, conf)).withSchema(schema)
        .withConf(conf).build()) {
      for (int i = 0; i < 2; i++) {
        var row = new GenericData.Record(schema);
        row.put("dec", ByteBuffer.wrap(new BigDecimal("999.99").unscaledValue().toByteArray()));
        writer.write(row);
      }
    }
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown;
    try (var connection = DriverManager.getConnection(url)) {
      for (String operator : List.of("UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL")) {
        String sql = "SELECT dec * 1.00 AS dec FROM p " + operator + " SELECT dec FROM p";
        try (var statement = connection.prepareStatement(sql)) {
          for (int execution = 0; execution < 2; execution++) {
            try (var result = statement.executeQuery()) {
              assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1), sql);
              assertEquals(8, result.getMetaData().getPrecision(1), sql);
              assertEquals(4, result.getMetaData().getScale(1), sql);
              int count = 0;
              while (result.next()) {
                assertEquals(new BigDecimal("999.9900"), result.getObject(1), sql);
                count++;
              }
              int expected = operator.startsWith("EXCEPT")
                  ? 0
                  : operator.equals("UNION ALL") ? 4 : operator.equals("INTERSECT ALL") ? 2 : 1;
              assertEquals(expected, count, sql);
            }
          }
        }
      }
      for (boolean square : List.of(false, true)) {
        String sql = square
            ? "SELECT dec * dec AS dec FROM p UNION ALL SELECT dec FROM p"
            : "SELECT dec * 10 AS dec FROM p UNION ALL SELECT CAST(1 AS DECIMAL(4,3)) AS dec";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1), sql);
          assertEquals(square ? 10 : 8, result.getMetaData().getPrecision(1), sql);
          assertEquals(square ? 4 : 3, result.getMetaData().getScale(1), sql);
          for (int i = 0; i < (square ? 4 : 3); i++) {
            assertTrue(result.next(), sql);
            String expected = square ? i < 2 ? "999980.0001" : "999.9900" : i < 2 ? "9999.900" : "1.000";
            assertEquals(new BigDecimal(expected), result.getObject(1), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Preserves OTHER for wholly untyped NULL results, including empty sets.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void untypedNullResultsRetainOtherMetadata() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (boolean empty : List.of(false, true)) {
        String filter = empty ? " WHERE FALSE" : "";
        String sql = "SELECT NULL AS x" + filter + " UNION SELECT NULL AS x" + filter;
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals(Types.OTHER, result.getMetaData().getColumnType(1));
          assertEquals("OTHER", result.getMetaData().getColumnTypeName(1));
          if (!empty) {
            assertTrue(result.next());
            assertEquals(null, result.getObject(1));
          }
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Keeps nonnumeric CAST metadata unchanged while numeric promotion is scoped.
   *
   * @param empty
   *          whether the branches are empty
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void nonnumericCastsRetainExistingMetadata(boolean empty) throws Exception {
    String filter = empty ? " WHERE FALSE" : "";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String type : List.of("BOOLEAN", "VARCHAR", "CHAR", "DATE", "TIME", "TIMESTAMP")) {
        String body = "SELECT CAST(NULL AS " + type + ") AS x" + filter;
        try (var statement = connection.createStatement();
            var result = statement.executeQuery(body + " UNION ALL " + body)) {
          // Declared metadata for the remaining CAST families is tracked in #288.
          assertEquals(type.equals("BOOLEAN") ? Types.BOOLEAN : Types.OTHER, result.getMetaData().getColumnType(1),
              type);
          for (int row = 0; row < (empty ? 0 : 2); row++) {
            assertTrue(result.next());
            assertEquals(null, result.getObject(1));
          }
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Preserves declared bounds and exact values through widening and empty sets.
   *
   * @param empty
   *          whether branches are empty
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void decimalBoundaries(boolean empty) throws Exception {
    String filter = empty ? " WHERE FALSE" : "";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String sql : List.of("SELECT CAST(99999999.99 AS DECIMAL(10,2)) AS n" + filter
          + " UNION ALL SELECT CAST(0.0001 AS DECIMAL(10,4))" + filter,
          "SELECT 9223372036854775808 AS n" + filter + " UNION ALL SELECT 0.0001" + filter)) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          var meta = result.getMetaData();
          assertEquals(Types.DECIMAL, meta.getColumnType(1), sql);
          assertEquals(sql.contains("CAST") ? 12 : 23, meta.getPrecision(1), sql);
          assertEquals(4, meta.getScale(1), sql);
          if (!empty) {
            assertTrue(result.next());
            assertEquals(new BigDecimal(sql.contains("CAST") ? "99999999.9900" : "9223372036854775808.0000"),
                result.getObject(1), sql);
            assertTrue(result.next());
            assertEquals(new BigDecimal("0.0001"), result.getObject(1), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
      String fractional = "SELECT 0.1 AS n" + filter + " UNION ALL SELECT 0.2" + filter;
      try (var statement = connection.createStatement(); var result = statement.executeQuery(fractional)) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
        assertEquals(1, result.getMetaData().getPrecision(1));
        assertEquals(1, result.getMetaData().getScale(1));
        if (!empty) {
          assertTrue(result.next());
          assertEquals(new BigDecimal("0.1"), result.getObject(1));
          assertTrue(result.next());
          assertEquals(new BigDecimal("0.2"), result.getObject(1));
        }
        assertFalse(result.next());
      }
      String widenedCast = "WITH x AS (SELECT CAST(1.25 AS DECIMAL(6,2)) AS n) "
          + "SELECT CAST(n AS DECIMAL(10,4)) AS n FROM x" + filter + " UNION ALL SELECT CAST(2 AS INTEGER)" + filter;
      try (var statement = connection.createStatement(); var result = statement.executeQuery(widenedCast)) {
        assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
        assertEquals(14, result.getMetaData().getPrecision(1));
        assertEquals(4, result.getMetaData().getScale(1));
        if (!empty) {
          assertTrue(result.next());
          assertEquals(new BigDecimal("1.2500"), result.getObject(1));
          assertTrue(result.next());
          assertEquals(new BigDecimal("2.0000"), result.getObject(1));
        }
        assertFalse(result.next());
      }
      String sql = "SELECT CAST(NULL AS INTEGER) AS n" + filter + " UNION ALL SELECT -1" + filter;
      try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
        assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1));
        if (!empty) {
          assertTrue(result.next());
          assertEquals(null, result.getObject(1));
          assertTrue(result.next());
          assertEquals(-1L, result.getObject(1));
        }
        assertFalse(result.next());
      }
    }
  }

  /**
   * Applies approximate conversion before equality, where DOUBLE loses a digit.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void approximateComparisonUsesConvertedValues() throws Exception {
    String left = "SELECT 9007199254740993 AS n";
    String right = "SELECT CAST(9007199254740992 AS DOUBLE)";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String operator : List.of("UNION", "INTERSECT", "EXCEPT")) {
        try (var statement = connection.createStatement();
            var result = statement.executeQuery(left + " " + operator + " " + right)) {
          assertEquals(Types.DOUBLE, result.getMetaData().getColumnType(1));
          if (!operator.equals("EXCEPT")) {
            assertTrue(result.next());
            assertEquals(9007199254740992.0, result.getObject(1));
          }
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Counts duplicates only after branches share a numeric representation.
   *
   * @param intersect
   *          whether INTERSECT ALL rather than EXCEPT ALL is tested
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void mixedNumericMultiplicity(boolean intersect) throws Exception {
    String sql = "(SELECT CAST(1 AS INTEGER) AS n UNION ALL SELECT CAST(1 AS INTEGER) "
        + "UNION ALL SELECT CAST(2 AS INTEGER)) " + (intersect ? "INTERSECT ALL" : "EXCEPT ALL")
        + " SELECT CAST(1 AS DECIMAL(10,3)) ORDER BY n";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
      assertEquals(13, result.getMetaData().getPrecision(1));
      assertEquals(3, result.getMetaData().getScale(1));
      assertTrue(result.next());
      assertEquals(new BigDecimal("1.000"), result.getObject(1));
      if (!intersect) {
        assertTrue(result.next());
        assertEquals(new BigDecimal("2.000"), result.getObject(1));
      }
      assertFalse(result.next());
    }
  }
}
