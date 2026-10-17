package jparq.hf;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
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
import org.junit.jupiter.params.provider.MethodSource;
import se.alipsa.jparq.JParqConnection;

/**
 * End-to-end checks that only literal operands are pushed into Parquet
 * predicates: comparisons against functions, arithmetic, subqueries and Boolean
 * expressions must be evaluated by the Java engine.
 */
class LiteralOperandPushdownTest {

  @TempDir
  Path directory;

  /** Query paired with its fixed expected ids and its pushdown classification. */
  private record Case(String where, List<Long> expected, boolean pushed) {
    @Override
    public String toString() {
      return where;
    }
  }

  /**
   * Creates tables {@code t(id, n, dbl, s, active)} and {@code t2(x, s)}.
   *
   * @throws Exception
   *           if a fixture cannot be written
   */
  @BeforeEach
  void createFixture() throws Exception {
    Schema schema = SchemaBuilder.record("t").fields().requiredInt("id").optionalInt("n").requiredDouble("dbl")
        .requiredString("s").requiredBoolean("active").endRecord();
    String[] strings = {
        "a1", "b2", "a3", "b4"
    };
    try (var writer = writer("t", schema)) {
      for (int id = 1; id <= 4; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("n", id == 2 ? null : Integer.valueOf(id));
        row.put("dbl", id * 1.5);
        row.put("s", strings[id - 1]);
        row.put("active", id % 2 == 1);
        writer.write(row);
      }
    }
    Schema second = SchemaBuilder.record("t2").fields().requiredInt("x").requiredString("s").endRecord();
    try (var writer = writer("t2", second)) {
      for (int x = 1; x <= 2; x++) {
        var row = new GenericData.Record(second);
        row.put("x", x);
        row.put("s", "a" + x);
        writer.write(row);
      }
    }
  }

  private org.apache.parquet.hadoop.ParquetWriter<GenericRecord> writer(String name, Schema schema) throws Exception {
    var file = new org.apache.hadoop.fs.Path(directory.resolve(name + ".parquet").toUri());
    return AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, new Configuration(false)))
        .withSchema(schema).withConf(new Configuration(false)).build();
  }

  private List<Long> ids(String where, boolean pushdown) throws Exception {
    List<Long> rows = new ArrayList<>();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT id FROM t WHERE " + where + " ORDER BY id")) {
      while (result.next()) {
        rows.add(result.getLong(1));
      }
    }
    return rows;
  }

  /**
   * Supplies every comparison from the plan's "non-literal operand" row together
   * with a few literal-only controls.
   *
   * @return the cases
   */
  static Stream<Case> cases() {
    return Stream.of(
        // STRING compared with a function, a scalar subquery and a function in an IN
        // list
        new Case("s = LOWER(s)", List.of(1L, 2L, 3L, 4L), false), new Case("s = CONCAT('a', '1')", List.of(1L), false),
        new Case("s = (SELECT MIN(s) FROM t2)", List.of(1L), false),
        new Case("s IN (LOWER('A1'), 'b2')", List.of(1L, 2L), false),
        // BOOLEAN compared with a comparison and with NOT col
        new Case("active = (n > 1)", List.of(3L), false), new Case("active = NOT active", List.of(), false),
        new Case("active <> (id > 2)", List.of(1L, 4L), false),
        // INT compared with arithmetic, a function, a parenthesized literal, a subquery
        new Case("id = n + 0", List.of(1L, 3L, 4L), false),
        new Case("id > (SELECT MIN(x) FROM t2)", List.of(2L, 3L, 4L), false),
        new Case("id = ABS(-2)", List.of(2L), false), new Case("id = 1 + 1", List.of(2L), false),
        new Case("id = (2)", List.of(2L), true), new Case("2 = (id)", List.of(2L), true),
        new Case("id = ((2))", List.of(2L), true),
        // an expression as a BETWEEN bound
        new Case("n BETWEEN id - 1 AND 5", List.of(1L, 3L, 4L), false),
        new Case("n BETWEEN (1) AND (3)", List.of(1L, 3L), true),
        // literal-only controls
        new Case("s = 'b2'", List.of(2L), true), new Case("active = TRUE", List.of(1L, 3L), true),
        new Case("id > -1", List.of(1L, 2L, 3L, 4L), true));
  }

  /**
   * Compares pushdown on and off against the fixed expected rows.
   *
   * @param testCase
   *          the where clause and its expected ids
   * @throws Exception
   *           if a query fails
   */
  @ParameterizedTest
  @MethodSource("cases")
  void resultsAreIndependentOfPushdown(Case testCase) throws Exception {
    assertEquals(testCase.expected(), ids(testCase.where(), true), testCase.where() + " with pushdown");
    assertEquals(testCase.expected(), ids(testCase.where(), false), testCase.where() + " without pushdown");
  }

  /**
   * Checks that non-literal comparisons stay residuals while literal ones,
   * including a parenthesized literal, are pushed.
   *
   * @param testCase
   *          the where clause and whether it must be pushed
   * @throws Exception
   *           if planning fails
   */
  @ParameterizedTest
  @MethodSource("cases")
  void onlyLiteralComparisonsAreAttached(Case testCase) throws Exception {
    try (var connection = (JParqConnection) DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement("SELECT id FROM t WHERE " + testCase.where())) {
      var info = statement.getPushdownInfo();
      assertEquals(testCase.pushed(), info.parquetPredicateAttached(), testCase.where() + " " + info);
      assertEquals(!testCase.pushed(), info.residualFilterPresent(), testCase.where() + " " + info);
    }
  }

  /**
   * Verifies the exact predicate and residual for a parenthesized literal and a
   * partially pushed conjunction.
   *
   * @throws Exception
   *           if planning fails
   */
  @Test
  void parenthesizedLiteralIsPushedAndRejectedPartsAreResiduals() throws Exception {
    try (var connection = (JParqConnection) DriverManager.getConnection("jdbc:jparq:" + directory)) {
      try (var statement = connection.prepareStatement("SELECT id FROM t WHERE id = (2)")) {
        var info = statement.getPushdownInfo();
        assertEquals("eq(id, 2)", info.parquetPredicateSummary());
        assertNull(info.residualExpressionSummary());
      }
      try (var statement = connection
          .prepareStatement("SELECT id FROM t WHERE id = (2) AND s = LOWER(s) AND active = (n > 1)")) {
        var info = statement.getPushdownInfo();
        assertEquals("eq(id, 2)", info.parquetPredicateSummary());
        assertEquals("s = LOWER(s) AND active = (n > 1)", info.residualExpressionSummary());
      }
    }
  }
}
