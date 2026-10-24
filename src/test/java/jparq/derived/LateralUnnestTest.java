package jparq.derived;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.ParquetTable;
import se.alipsa.jparq.engine.SqlParser;

/** Regression coverage for typed outer arrays inside LATERAL derived tables. */
class LateralUnnestTest {
  @TempDir
  Path directory;

  /**
   * Writes empty and null arrays before populated rows to exercise schema
   * discovery.
   */
  @BeforeEach
  void writeArrays() throws Exception {
    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema dates = Schema.createArray(date);
    Schema nested = Schema.createArray(Schema.createArray(Schema.create(Schema.Type.INT)));
    Schema schema = SchemaBuilder.record("arrays").fields().requiredInt("id").name("dates")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL), dates)).noDefault().name("nested")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL), nested)).noDefault().endRecord();
    Configuration conf = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("arr.parquet").toUri()),
        conf);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(conf).build()) {
      for (int id = 0; id < 4; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("dates", id == 0 ? List.of() : id == 1 ? null : List.of(20000 + id, 21000 + id));
        row.put("nested", id == 0 ? List.of() : id == 1 ? null : List.of(List.of(id, id + 1), List.of(id + 2)));
        writer.write(row);
      }
    }
  }

  /**
   * Both join syntaxes bind the current row and retain DATE metadata and values.
   */
  @Test
  void lateralDatesUseCurrentOuterRow() throws SQLException {
    for (String join : List.of(", LATERAL", " CROSS JOIN LATERAL")) {
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
          var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT a.id, x.t FROM arr a" + join
              + " (SELECT u.t FROM UNNEST(a.dates) AS u(t)) x ORDER BY a.id, x.t")) {
        assertEquals(Types.DATE, result.getMetaData().getColumnType(2));
        List<String> rows = new ArrayList<>();
        while (result.next()) {
          assertInstanceOf(Date.class, result.getObject(2));
          rows.add(result.getInt(1) + ":" + result.getDate(2).toLocalDate().toEpochDay());
        }
        assertEquals(List.of("2:20002", "2:21002", "3:20003", "3:21003"), rows);
      }
    }
  }

  /**
   * Nested arrays retain ARRAY metadata and structured values.
   */
  @Test
  void lateralNestedArraysRetainElements() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT a.id, x.t FROM arr a CROSS JOIN LATERAL "
            + "(SELECT t FROM UNNEST(a.nested) AS u(t)) x ORDER BY a.id")) {
      assertEquals(Types.ARRAY, result.getMetaData().getColumnType(2));
      List<String> rows = new ArrayList<>();
      while (result.next()) {
        rows.add(result.getInt(1) + ":" + Arrays.toString((Object[]) result.getObject(2)));
      }
      assertEquals(List.of("2:[2, 3]", "2:[4]", "3:[3, 4]", "3:[5]"), rows);
    }
  }

  /** Table-function arguments and other outer expressions bind together. */
  @Test
  void lateralUnnestCanFilterOnAnotherOuterColumn() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT a.id, x.t FROM arr a CROSS JOIN LATERAL "
            + "(SELECT u.t FROM UNNEST(a.dates) AS u(t) WHERE a.id = 3) x ORDER BY x.t")) {
      assertTrue(result.next());
      assertEquals(3, result.getInt(1));
      assertEquals(20003, result.getDate(2).toLocalDate().toEpochDay());
      assertTrue(result.next());
      assertEquals(21003, result.getDate(2).toLocalDate().toEpochDay());
      assertFalse(result.next());
    }
  }

  /** An empty outer input still exposes the array element's declared type. */
  @Test
  void emptyOuterInputRetainsDateMetadata() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT x.t FROM arr a CROSS JOIN LATERAL "
            + "(SELECT u.t FROM UNNEST(a.dates) AS u(t)) x WHERE a.id < 0")) {
      assertEquals(Types.DATE, result.getMetaData().getColumnType(1));
      assertFalse(result.next());
    }
  }

  /**
   * Nested LATERAL shapes and validated inputs stay bounded as outer rows grow.
   *
   * @param scalar
   *          whether the containing query is a correlated scalar subquery
   * @throws Exception
   *           if JDBC execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void nestedLateralsReuseShapes(boolean scalar) throws Exception {
    List<Integer> previousCounts = null;
    for (int outerRows : List.of(2, 4)) {
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
        String inner = "FROM arr b CROSS JOIN LATERAL (SELECT u.t FROM UNNEST(b.dates) AS u(t)) y "
            + "WHERE b.id = a.id";
        String sql = scalar
            ? "SELECT a.id, (SELECT MAX(y.t) " + inner + ") FROM arr a WHERE a.id < " + outerRows + " ORDER BY a.id"
            : "SELECT a.id, x.t FROM arr a CROSS JOIN LATERAL (SELECT y.t " + inner + ") x WHERE a.id < " + outerRows
                + " ORDER BY a.id, x.t";
        try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
            var statement = connection.prepareStatement(sql)) {
          for (int execution = 0; execution < 2; execution++) {
            try (var result = statement.executeQuery()) {
              int count = 0;
              while (result.next()) {
                int id = result.getInt(1);
                if (scalar && id < 2) {
                  assertNull(result.getObject(2));
                } else {
                  assertEquals((scalar || count % 2 == 1 ? 21000 : 20000) + id,
                      result.getDate(2).toLocalDate().toEpochDay());
                }
                count++;
              }
              assertEquals(scalar ? outerRows : Math.max(0, outerRows - 2) * 2, count);
            }
          }
        }
      }
      var counts = List.of(parses.get(), validations.get());
      if (previousCounts != null) {
        assertEquals(previousCounts, counts, "Nested LATERAL work grew with outer row count");
      }
      previousCounts = counts;
    }
  }

  /**
   * Direct-column metadata uses the same qualifier resolution as value binding.
   */
  @Test
  void qualifiersPreserveDateSchemasAndValues() throws SQLException {
    for (String qualifier : List.of("arr", "a", "A", "\"Outer A\"")) {
      String source = "arr".equals(qualifier) ? "arr" : "arr " + qualifier;
      try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
          var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT x.t FROM " + source + " CROSS JOIN LATERAL "
              + "(SELECT u.t FROM UNNEST(" + qualifier + ".dates) AS u(t)) x ORDER BY x.t")) {
        assertEquals(Types.DATE, result.getMetaData().getColumnType(1), qualifier);
        List<Long> days = new ArrayList<>();
        while (result.next()) {
          days.add(result.getDate(1).toLocalDate().toEpochDay());
        }
        assertEquals(List.of(20002L, 20003L, 21002L, 21003L), days, qualifier);
      }
    }
  }

  /** Standalone NULL arrays produce no rows and retain unknown-type metadata. */
  @Test
  void standaloneNullUnnestReturnsNoRows() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT * FROM UNNEST(NULL) AS u(x)")) {
      assertEquals("x", result.getMetaData().getColumnLabel(1));
      assertEquals(Types.VARCHAR, result.getMetaData().getColumnType(1));
      assertFalse(result.next());
    }
  }

  /** LEFT JOIN retains outer rows whose arrays are null or empty. */
  @Test
  void leftLateralUnnestPreservesEmptyAndNullArrays() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT a.id, x.n FROM arr a LEFT JOIN LATERAL "
            + "(SELECT u.t, u.n FROM UNNEST(a.dates) WITH ORDINALITY AS u(t, n)) x ON TRUE " + "ORDER BY a.id, x.n")) {
      List<String> rows = new ArrayList<>();
      while (result.next()) {
        rows.add(result.getInt(1) + ":" + result.getObject(2));
      }
      assertEquals(List.of("0:null", "1:null", "2:1", "2:2", "3:1", "3:2"), rows);
    }
  }
}
