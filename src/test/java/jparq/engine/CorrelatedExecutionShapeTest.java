package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.avro.LogicalTypes;
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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.ParquetTable;
import se.alipsa.jparq.engine.SqlParser;

/** Exercises execution-local correlation shapes without timing assertions. */
class CorrelatedExecutionShapeTest {
  @TempDir
  Path directory;

  /**
   * Writes changing and null correlation keys plus exact logical values.
   *
   * @throws Exception
   *           if a fixture cannot be written
   */
  @BeforeEach
  void createFixtures() throws Exception {
    var schema = SchemaBuilder.record("row").fields().requiredInt("id").optionalInt("g").requiredString("Mixed")
        .name("amount").type(LogicalTypes.decimal(20, 2).addToSchema(Schema.create(Schema.Type.BYTES))).noDefault()
        .name("day").type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).noDefault().endRecord();
    for (String name : List.of("big", "small")) {
      var configuration = new Configuration(false);
      var file = new org.apache.hadoop.fs.Path(directory.resolve(name + ".parquet").toUri());
      try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, configuration))
          .withSchema(schema).withConf(configuration).build()) {
        for (int id = 0; id < ("big".equals(name) ? 40 : 5); id++) {
          var row = new GenericData.Record(schema);
          row.put("id", id);
          row.put("g", id == 0 ? null : Integer.valueOf(id % 5));
          row.put("Mixed", "value ' " + id);
          row.put("amount", ByteBuffer
              .wrap(new BigDecimal("9007199254740993.25").add(BigDecimal.valueOf(id)).unscaledValue().toByteArray()));
          row.put("day", 20000 + id);
          writer.write(row);
        }
      }
    }
  }

  /**
   * Rejects user-written private binding calls in WHERE with SQLException.
   */
  @Test
  void reservedBindingCallsInWhereRaiseSqlExceptions() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String call : List.of("__JPARQ_CORRELATED_VALUE(1, 2)", "__JPARQ_CORRELATED_VALUE('x', 0)")) {
        var failure = assertThrows(java.sql.SQLException.class, () -> {
          try (var result = statement.executeQuery("SELECT o.id FROM big o WHERE " + call + " = 1")) {
            result.next();
          }
        });
        Throwable root = failure;
        while (root.getCause() != null) {
          root = root.getCause();
        }
        assertInstanceOf(IllegalArgumentException.class, root);
        assertEquals("Invalid correlated binding: reserved for internal query execution", root.getMessage());
      }
    }
  }

  /**
   * Checks every evaluator path with changing keys in all pushdown modes.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void rowDependentResultsAndScopesRemainCorrect(boolean predicate, boolean projection) throws Exception {
    String url = "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection;
    List<Integer> matching = IntStream.range(0, 40).filter(id -> id % 5 != 0).boxed().toList();
    try (var connection = DriverManager.getConnection(url); var statement = connection.createStatement()) {
      for (String condition : List.of("EXISTS (SELECT 1 FROM small s WHERE s.g = b.g)",
          "b.g IN (SELECT s.g FROM small s WHERE s.id <= b.id)", "b.g = ANY (SELECT s.g FROM small s WHERE s.g = b.g)",
          "b.g = SOME (SELECT s.g FROM small s WHERE s.g = b.g)",
          "b.g = ALL (SELECT s.g FROM small s WHERE s.g = b.g) AND b.g <> 0",
          "EXISTS (SELECT s.g FROM small s GROUP BY s.g HAVING s.g = b.g)",
          "(SELECT COUNT(*) FROM small s WHERE s.g = b.g) = 1",
          "EXISTS (SELECT 1 FROM (SELECT g FROM small WHERE g = b.g) s)",
          "EXISTS (WITH c AS (SELECT g FROM small WHERE g = b.g) SELECT 1 FROM c)",
          "EXISTS (SELECT 1 FROM small s JOIN small q ON q.id = s.id WHERE s.g = b.g)",
          "EXISTS (SELECT 1 FROM small s WHERE s.g = g AND s.g = b.g)",
          "EXISTS (SELECT 1 FROM small s WHERE EXISTS (SELECT 1 FROM small q WHERE q.g = s.g AND q.g = b.g))")) {
        String sql = "SELECT b.id FROM big b WHERE " + condition + " ORDER BY b.id";
        try (var result = statement.executeQuery(sql)) {
          var actual = new ArrayList<Integer>();
          while (result.next()) {
            actual.add(result.getInt(1));
            assertSame(result, statement.getResultSet());
          }
          assertEquals(matching, actual, sql);
        }
      }
      var scopedQueries = Map.of("SELECT 1 FROM c s WHERE s.g = b.g", 2,
          "WITH c AS (SELECT g FROM small WHERE id = 1) SELECT 1 FROM c s WHERE s.g = b.g", 1,
          "SELECT 1 FROM small s WHERE id = 1 AND s.g = b.g", 1);
      for (var entry : scopedQueries.entrySet()) {
        String body = entry.getKey();
        String sql = "WITH c AS (SELECT g FROM small WHERE id = 2) SELECT b.id FROM big b WHERE EXISTS (" + body
            + ") ORDER BY b.id";
        try (var result = statement.executeQuery(sql)) {
          int group = entry.getValue();
          var expected = IntStream.range(0, 40).filter(id -> id % 5 == group).boxed().toList();
          var actual = new ArrayList<Integer>();
          while (result.next()) {
            actual.add(result.getInt(1));
          }
          assertEquals(expected, actual, sql);
        }
      }
    }
  }

  /**
   * Verifies quoted table qualifiers and logical values survive typed binding.
   *
   * @throws Exception
   *           if execution fails
   */
  @Test
  void typedValuesAreBoundWithoutLiteralRoundTrips() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?caseSensitive=true");
        var statement = connection.createStatement()) {
      for (String column : List.of("Mixed", "amount", "day")) {
        String sql = "SELECT \"b\".id FROM big \"b\" WHERE EXISTS (SELECT 1 FROM small s WHERE s." + column
            + " = \"b\"." + column + ") ORDER BY \"b\".id";
        try (var result = statement.executeQuery(sql)) {
          for (int id = 0; id < 5; id++) {
            assertTrue(result.next(), sql);
            assertEquals(id, result.getInt(1), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Evaluates ARRAY and grouped HAVING subqueries with their own typed bindings.
   *
   * @throws Exception
   *           if execution fails
   */
  @Test
  void arraysAndGroupedHavingUseCurrentOuterValues() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      try (var result = statement.executeQuery(
          "SELECT b.id, ARRAY(SELECT s.id FROM small s WHERE s.g = b.g) " + "AS ids FROM big b ORDER BY b.id")) {
        for (int id = 0; id < 40; id++) {
          assertTrue(result.next());
          assertEquals(id % 5 == 0 ? List.of() : List.of(id % 5), result.getObject(2));
        }
        assertFalse(result.next());
      }
      try (var result = statement.executeQuery("SELECT b.g, COUNT(*) FROM big b GROUP BY b.g "
          + "HAVING EXISTS (SELECT 1 FROM small s WHERE s.g = b.g) ORDER BY b.g")) {
        for (int group = 1; group <= 4; group++) {
          assertTrue(result.next());
          assertEquals(group, result.getInt(1));
          assertEquals(8, result.getLong(2));
        }
        assertFalse(result.next());
      }
    }
  }

  /**
   * Preserves the Java class and value when a scalar query projects an outer
   * reference, including NULL and exact decimal and date values.
   *
   * @throws Exception
   *           if execution fails
   */
  @Test
  void scalarOuterProjectionsRetainOriginalTypes() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String column : List.of("g", "amount", "day")) {
        try (var result = statement.executeQuery("SELECT b.id, b." + column + ", (SELECT b." + column
            + " FROM small s WHERE s.id = 1) AS bound_value FROM big b ORDER BY b.id")) {
          for (int id = 0; id < 40; id++) {
            assertTrue(result.next());
            Object expected = result.getObject(2);
            Object actual = result.getObject(3);
            assertEquals(expected, actual);
            if (expected != null) {
              assertEquals(expected.getClass(), actual.getClass());
            }
          }
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Rebuilds Parquet predicates for each typed key instead of retaining the first
   * binding in a cached physical plan.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void pushedPredicatesUseEachCurrentBinding(boolean pushdown) throws Exception {
    var predicates = new ArrayList<String>();
    try (var tables = mockStatic(ParquetTable.class, CALLS_REAL_METHODS)) {
      tables.when(() -> ParquetTable.validate(any(), any())).thenAnswer(invocation -> {
        ParquetTable table = (ParquetTable) invocation.callRealMethod();
        se.alipsa.jparq.JParqConnection.TableLocation location = invocation.getArgument(0);
        if (!"small".equals(location.tableName())) {
          return table;
        }
        var observed = spy(table);
        doAnswer(open -> {
          org.apache.parquet.filter2.predicate.FilterPredicate predicate = open.getArgument(1);
          predicates.add(predicate == null ? "none" : predicate.toString());
          return table.open(open.getArgument(0), predicate);
        }).when(observed).open(any(Configuration.class),
            nullable(org.apache.parquet.filter2.predicate.FilterPredicate.class));
        return observed;
      });
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown);
          var statement = connection.createStatement();
          var result = statement
              .executeQuery("SELECT b.id FROM big b WHERE EXISTS (SELECT 1 FROM small s WHERE s.g = b.g)")) {
        int rows = 0;
        while (result.next()) {
          rows++;
        }
        assertEquals(32, rows);
      }
    }
    assertFalse(predicates.isEmpty());
    if (pushdown) {
      for (int group = 0; group < 5; group++) {
        assertTrue(predicates.contains("eq(g, " + group + ")"), predicates.toString());
      }
    } else {
      assertTrue(predicates.stream().allMatch("none"::equals));
    }
  }

  /**
   * Keeps row-varying literal LATERAL inputs on their existing per-call path.
   *
   * @throws Exception
   *           if execution fails
   */
  @Test
  void literalLateralInputsRemainRowDependent() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement("SELECT b.id, x.v FROM big b JOIN LATERAL "
            + "(SELECT b.id AS v FROM small s WHERE s.id = 1) x ON TRUE ORDER BY b.id");
        var result = statement.executeQuery()) {
      for (int id = 0; id < 40; id++) {
        assertTrue(result.next());
        assertEquals(id, result.getInt(1));
        assertEquals(String.valueOf(id), result.getString(2));
      }
      assertFalse(result.next());
    }
  }

  /**
   * Gets a new binding and cancellation scope when an outer statement executes
   * again after cancellation during iteration.
   *
   * @throws Exception
   *           if execution fails
   */
  @Test
  void reexecutionAfterCancellationGetsFreshShapes() throws Exception {
    String sql = "SELECT b.id, (SELECT s.g FROM small s WHERE s.id = b.g) AS scalar_value " + "FROM big b";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement(sql)) {
      try (var result = statement.executeQuery()) {
        assertTrue(result.next());
        assertNull(result.getObject(2));
        statement.cancel();
        assertEquals("57014", assertThrows(java.sql.SQLException.class, result::next).getSQLState());
      }
      try (var result = statement.executeQuery()) {
        for (int id = 0; id < 40; id++) {
          assertTrue(result.next());
          assertSame(result, statement.getResultSet());
          assertEquals(id, result.getInt(1));
          assertEquals(id % 5 == 0 ? null : Integer.valueOf(id % 5), result.getObject(2));
        }
        assertFalse(result.next());
      }
    }
  }

  /**
   * Counts real parser and footer-validation calls as the outer row count grows.
   *
   * @param outerRows
   *          number of outer rows evaluated
   * @param prepared
   *          whether the owning statement is prepared
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "10,true", "40,true", "10,false", "40,false"
  })
  void preparationWorkIsIndependentOfOuterRowCount(int outerRows, boolean prepared) throws Exception {
    String sql = "SELECT b.id FROM big b WHERE b.id < " + outerRows
        + " AND EXISTS (SELECT 1 FROM small s WHERE s.g = b.g)";
    try (var parser = mockStatic(SqlParser.class, CALLS_REAL_METHODS);
        var tables = mockStatic(ParquetTable.class, CALLS_REAL_METHODS);
        var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = prepared ? connection.prepareStatement(sql) : connection.createStatement();
        var result = prepared ? ((java.sql.PreparedStatement) statement).executeQuery() : statement.executeQuery(sql)) {
      int count = 0;
      while (result.next()) {
        count++;
      }
      assertEquals(outerRows * 4 / 5, count);
      assertAll(() -> parser.verify(() -> SqlParser.parseQuery(anyString()), times(2)),
          () -> tables.verify(() -> ParquetTable.validate(any(), any()), times(2)));
    }
  }

  /**
   * Keeps parse and schema-validation work constant for materialized nested
   * scopes and joins as outer inputs grow.
   *
   * @param inner
   *          correlated or uncorrelated nested query
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT 1 FROM (SELECT g FROM small WHERE g = b.g) s",
      "WITH c AS (SELECT g FROM small WHERE g = b.g) SELECT 1 FROM c",
      "SELECT 1 FROM small s JOIN small q ON q.id = s.id WHERE s.g = b.g",
      "SELECT s.g FROM small s WHERE s.g = b.g GROUP BY s.g", "SELECT 1 FROM small"
  })
  void nestedMaterializationsReuseParsedAndValidatedShapes(String inner) throws Exception {
    List<Integer> previousCounts = null;
    for (int outerRows : List.of(10, 40)) {
      var parses = new AtomicInteger();
      var validations = new AtomicInteger();
      try (var parser = mockStatic(SqlParser.class, CALLS_REAL_METHODS);
          var tables = mockStatic(ParquetTable.class, CALLS_REAL_METHODS)) {
        parser.when(() -> SqlParser.parseQuery(anyString())).thenAnswer(invocation -> {
          parses.incrementAndGet();
          return invocation.callRealMethod();
        });
        tables.when(() -> ParquetTable.validate(any(), any())).thenAnswer(invocation -> {
          validations.incrementAndGet();
          return invocation.callRealMethod();
        });
        try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
            var statement = connection.createStatement();
            var result = statement
                .executeQuery("SELECT b.id FROM big b WHERE b.id < " + outerRows + " AND EXISTS (" + inner + ")")) {
          int count = 0;
          while (result.next()) {
            count++;
          }
          assertEquals(inner.equals("SELECT 1 FROM small") ? outerRows : outerRows * 4 / 5, count);
        }
      }
      var counts = List.of(parses.get(), validations.get());
      if (previousCounts != null) {
        assertEquals(previousCounts, counts, inner);
      }
      previousCounts = counts;
    }
  }

}
