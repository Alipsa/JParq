package jparq.setops;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies common numeric types, values, and set multiplicities. */
class SetNumericPromotionTest {

  @TempDir
  Path directory;

  /**
   * Writes fixed numeric declarations, including a long beyond DOUBLE precision.
   *
   * @throws Exception
   *           if fixture creation fails
   */
  @BeforeEach
  void fixture() throws Exception {
    var schema = SchemaBuilder.record("numbers").fields().requiredInt("i").requiredLong("l").requiredDouble("d")
        .endRecord();
    var conf = new Configuration(false);
    var path = new org.apache.hadoop.fs.Path(directory.resolve("numbers.parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(path, conf)).withSchema(schema)
        .withConf(conf).build()) {
      for (int i = 1; i <= 2; i++) {
        var row = new GenericData.Record(schema);
        row.put("i", i);
        row.put("l", 9007199254740992L + i);
        row.put("d", i + 0.5);
        writer.write(row);
      }
    }
  }

  /**
   * Checks numeric families in both branch orders, including empty declarations.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void commonTypes(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      for (String[] pair : List.of(new String[]{
          "i", "l"
      }, new String[]{
          "i", "d"
      }, new String[]{
          "CAST(i AS DECIMAL(10,2))", "l"
      }, new String[]{
          "CAST(i AS DECIMAL(10,2))", "CAST(i AS DECIMAL(12,4))"
      }, new String[]{
          "CAST(i AS DECIMAL(10,2))", "d"
      })) {
        for (boolean reverse : List.of(false, true)) {
          String left = pair[reverse ? 1 : 0];
          String right = pair[reverse ? 0 : 1];
          int type = Arrays.asList(pair).contains("d")
              ? Types.DOUBLE
              : pair[0].contains("DECIMAL") ? Types.DECIMAL : Types.BIGINT;
          for (boolean empty : List.of(false, true)) {
            String filter = empty ? " WHERE i < 0" : "";
            String sql = "SELECT " + left + " AS n FROM numbers" + filter + " UNION ALL SELECT " + right
                + " FROM numbers" + filter;
            try (var statement = connection.prepareStatement(sql)) {
              for (int execution = 0; execution < 2; execution++) {
                try (var result = statement.executeQuery()) {
                  var meta = result.getMetaData();
                  assertEquals(type, meta.getColumnType(1), sql);
                  Class<?> expected = type == Types.DOUBLE
                      ? Double.class
                      : type == Types.DECIMAL ? BigDecimal.class : Long.class;
                  assertEquals(expected.getName(), meta.getColumnClassName(1), sql);
                  if (type == Types.DECIMAL) {
                    assertEquals(pair[1].equals("l") ? 21 : 12, meta.getPrecision(1), sql);
                    assertEquals(pair[1].equals("l") ? 2 : 4, meta.getScale(1), sql);
                  }
                  int count = 0;
                  while (result.next()) {
                    assertInstanceOf(expected, result.getObject(1), sql);
                    if (type == Types.BIGINT && result.getLong(1) > 2) {
                      assertEquals(9007199254740993L + count % 2, result.getLong(1), sql);
                    }
                    count++;
                  }
                  assertEquals(empty ? 0 : 4, count, sql);
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Checks numeric equality after coercion for every set operator.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void setSemantics(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      for (String operator : List.of("UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL")) {
        String sql = "SELECT i AS n FROM numbers " + operator
            + " SELECT CAST(i AS DECIMAL(10,2)) FROM numbers ORDER BY n DESC";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertEquals(Types.DECIMAL, result.getMetaData().getColumnType(1));
          List<BigDecimal> actual = new ArrayList<>();
          while (result.next()) {
            actual.add(assertInstanceOf(BigDecimal.class, result.getObject(1)));
          }
          var expected = operator.startsWith("EXCEPT")
              ? List.of()
              : operator.equals("UNION ALL")
                  ? List.of(new BigDecimal("2.00"), new BigDecimal("2.00"), new BigDecimal("1.00"),
                      new BigDecimal("1.00"))
                  : List.of(new BigDecimal("2.00"), new BigDecimal("1.00"));
          assertEquals(expected, actual, sql);
        }
      }
    }
  }

  /**
   * Checks literal inference, NULLs, materialization, and correlated derived
   * sets.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void nestedAndLiteralTypes(boolean pushdown) throws Exception {
    try (var connection = DriverManager.getConnection(url(pushdown))) {
      for (String body : List.of("SELECT i AS n FROM numbers UNION SELECT 2",
          "SELECT 1 AS n UNION SELECT CAST(2 AS INTEGER)", "SELECT NULL AS n UNION ALL SELECT l FROM numbers")) {
        for (String sql : List.of(body, "SELECT n FROM (" + body + ") x", "WITH x AS (" + body + ") SELECT n FROM x")) {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1), sql);
            while (result.next()) {
              Object value = result.getObject(1);
              if (value != null) {
                assertInstanceOf(Long.class, value, sql);
              }
            }
          }
        }
      }
      String sql = "SELECT COUNT(*) FROM numbers m WHERE EXISTS (SELECT 1 FROM "
          + "(SELECT i FROM numbers WHERE i = m.i UNION SELECT 99) x WHERE x.i = 1)";
      try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
        assertTrue(result.next());
        assertEquals(1, result.getInt(1));
      }
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT x.n FROM numbers m CROSS JOIN LATERAL "
              + "(SELECT i AS n FROM numbers WHERE i = m.i UNION SELECT 99) x ORDER BY m.i, x.n")) {
        assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1));
        for (long expected : List.of(1L, 99L, 2L, 99L)) {
          assertTrue(result.next());
          assertEquals(expected, result.getObject(1));
        }
        assertFalse(result.next());
      }
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT i AS n, CAST(i AS DECIMAL(10,2)) AS v FROM numbers WHERE i = 1 "
              + "UNION ALL SELECT l, d FROM numbers WHERE i = 2")) {
        assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1));
        assertEquals(Types.DOUBLE, result.getMetaData().getColumnType(2));
        assertTrue(result.next());
        assertEquals(1L, result.getObject(1));
        assertEquals(1.0, result.getObject(2));
        assertTrue(result.next());
        assertEquals(9007199254740994L, result.getObject(1));
        assertEquals(2.5, result.getObject(2));
        assertFalse(result.next());
      }
      for (String projection : List.of("CAST(i AS DOUBLE)", "1")) {
        String aliased = "SELECT " + projection + " AS i FROM numbers UNION ALL SELECT i FROM numbers";
        boolean approximate = projection.contains("DOUBLE");
        try (var statement = connection.createStatement(); var result = statement.executeQuery(aliased)) {
          assertEquals(approximate ? Types.DOUBLE : Types.BIGINT, result.getMetaData().getColumnType(1), aliased);
          for (int i = 0; i < 4; i++) {
            assertTrue(result.next());
            long expected = i >= 2 || approximate ? i % 2 + 1 : 1;
            if (approximate) {
              assertEquals((double) expected, result.getObject(1), aliased);
            } else {
              assertEquals(expected, result.getObject(1), aliased);
            }
          }
          assertFalse(result.next());
        }
      }
      try (var statement = connection.createStatement()) {
        for (String branch : List.of("'text'", "'text' WHERE FALSE", "CAST(NULL AS VARCHAR)")) {
          assertThrows(SQLException.class,
              () -> statement.executeQuery("SELECT i FROM numbers UNION SELECT " + branch));
        }
      }
    }
  }

  /**
   * Builds a connection URL with both pushdown settings controlled.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @return fixture connection URL
   */
  private String url(boolean pushdown) {
    return "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown;
  }
}
