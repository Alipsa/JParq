package jparq;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.JParqResultSetMetaData;

/**
 * Verifies that every ARRAY value exposed through {@code getObject()} uses the
 * uniform {@code Object[]} representation, regardless of whether the array
 * comes from a physical column, a computed constructor, or a mixture of both in
 * one result. Regression coverage for issue 234.
 */
class ArrayResultRepresentationTest {

  /**
   * Element declarations, including unknown declarations, are resolved once per
   * column.
   */
  @Test
  void arraySchemasAreCachedPerColumn(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory); var statement = connection.createStatement()) {
      try (var original = statement.executeQuery("SELECT ARRAY[1,2], ARRAY[NULL] FROM s");
          var result = org.mockito.Mockito.spy(original)) {
        int count = 0;
        while (result.next()) {
          assertEquals(Types.INTEGER, result.getArray(1).getBaseType());
          assertEquals(Types.INTEGER, result.getArray(1).getBaseType());
          assertEquals(Types.OTHER, result.getArray(2).getBaseType());
          assertEquals(Types.OTHER, result.getArray(2).getBaseType());
          count++;
        }
        assertEquals(2, count);
        org.mockito.Mockito.verify(result, org.mockito.Mockito.times(2)).getMetaData();
      }
    }
  }

  /** Computed arrays retain declared types even when every element is null. */
  @Test
  void typedNullArrayElements(@TempDir Path directory) throws Exception {
    try (var connection = connect(directory); var statement = connection.createStatement()) {
      var declarations = Map.ofEntries(Map.entry("INTEGER", Types.INTEGER), Map.entry("DATE", Types.DATE),
          Map.entry("VARCHAR", Types.VARCHAR), Map.entry("VARCHAR(12)", Types.VARCHAR),
          Map.entry("CHARACTER VARYING(12)", Types.VARCHAR), Map.entry("CHAR(4)", Types.CHAR),
          Map.entry("CHARACTER(4)", Types.CHAR), Map.entry("NCHAR(4)", Types.NCHAR),
          Map.entry("NVARCHAR(4)", Types.NVARCHAR), Map.entry("BOOLEAN", Types.BOOLEAN),
          Map.entry("BOOL", Types.BOOLEAN), Map.entry("TIMESTAMP", Types.TIMESTAMP),
          Map.entry("TIMESTAMP(9)", Types.TIMESTAMP), Map.entry("TIME", Types.TIME),
          Map.entry("TIME WITHOUT TIME ZONE", Types.TIME), Map.entry("TIMESTAMP WITHOUT TIME ZONE", Types.TIMESTAMP),
          Map.entry("TIME WITH TIME ZONE", Types.TIME_WITH_TIMEZONE),
          Map.entry("TIMESTAMP(6) WITH TIME ZONE", Types.TIMESTAMP_WITH_TIMEZONE), Map.entry("BINARY(4)", Types.BINARY),
          Map.entry("VARBINARY(4)", Types.VARBINARY));
      for (var declaration : declarations.entrySet()) {
        String array = "ARRAY[CAST(NULL AS " + declaration.getKey() + ")]";
        for (String query : List.of("SELECT " + array + " AS a", "SELECT a FROM (SELECT " + array + " AS a) t",
            "WITH t AS (SELECT " + array + " AS a) SELECT a FROM t", "SELECT (SELECT " + array + ") AS a")) {
          try (var result = statement.executeQuery(query)) {
            assertTrue(result.next(), query);
            assertEquals(Types.ARRAY, result.getMetaData().getColumnType(1), query);
            assertEquals(declaration.getValue(), result.getArray(1).getBaseType(), query);
            assertEquals(JDBCType.valueOf(declaration.getValue()).getName(), result.getArray("a").getBaseTypeName(),
                query);
            assertArrayEquals(new Object[]{
                null
            }, (Object[]) result.getArray(1).getArray(), query);
          }
        }
      }
      try (var result = statement.executeQuery("SELECT ARRAY[1,2.5], ARRAY[ARRAY[CAST(NULL AS INTEGER)]]")) {
        assertTrue(result.next());
        assertEquals(Types.DECIMAL, result.getArray(1).getBaseType());
        try (var elements = result.getArray(2).getResultSet()) {
          assertTrue(elements.next());
          assertEquals(Types.INTEGER, elements.getArray(2).getBaseType());
        }
      }
    }
  }

  /**
   * Incompatible typed elements fail at the JDBC planning boundary, including
   * empty inputs.
   */
  @Test
  void incompatibleTypedArrayElementsFailDuringPlanning(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory); var statement = connection.createStatement()) {
      for (String expression : List.of("ARRAY[CAST(NULL AS VARCHAR), 1]", "ARRAY[CAST(NULL AS BOOLEAN), 1]",
          "ARRAY[CAST(NULL AS DATE), CURRENT_TIMESTAMP]", "ARRAY[CAST(NULL AS VARCHAR), id]")) {
        for (String query : List.of("SELECT " + expression + " AS v FROM s",
            "SELECT " + expression + " AS v FROM s WHERE 1 = 0",
            "SELECT v FROM (SELECT " + expression + " AS v FROM s) t",
            "WITH t AS (SELECT " + expression + " AS v FROM s) SELECT v FROM t",
            "SELECT CASE WHEN TRUE THEN " + expression + " ELSE NULL END FROM s",
            "SELECT CASE WHEN TRUE THEN 7 ELSE " + expression + " END FROM s")) {
          var error = assertThrows(java.sql.SQLSyntaxErrorException.class, () -> statement.executeQuery(query), query);
          assertEquals("42000", error.getSQLState(), query);
          assertTrue(error.getMessage().contains("ARRAY elements must share a common type"), query);
        }
      }
      for (String query : List.of("SELECT ARRAY[CAST(NULL AS VARCHAR), a.id] FROM s a",
          "SELECT ARRAY[CAST(NULL AS VARCHAR), a.id] FROM s a CROSS JOIN s b",
          "SELECT ARRAY[CAST(NULL AS VARCHAR), a.id] FROM s a CROSS JOIN s b WHERE 1 = 0")) {
        var error = assertThrows(java.sql.SQLSyntaxErrorException.class, () -> statement.executeQuery(query), query);
        assertEquals("42000", error.getSQLState(), query);
        assertTrue(error.getMessage().contains("ARRAY elements must share a common type"), query);
      }
      for (String expression : List.of("ARRAY[CAST(NULL AS VARCHAR), 1]", "ARRAY[CAST(NULL AS BOOLEAN), 1]")) {
        var error = assertThrows(java.sql.SQLSyntaxErrorException.class,
            () -> connection.prepareStatement("SELECT " + expression + " AS v"));
        assertEquals("42000", error.getSQLState());
      }
    }
  }

  /**
   * Defensive array getters translate declaration failures to classified JDBC
   * exceptions.
   */
  @Test
  void arrayGetterClassifiesDeclarationFailures() throws Exception {
    var expression = net.sf.jsqlparser.parser.CCJSqlParserUtil.parseExpression("ARRAY[CAST(NULL AS VARCHAR), 1]");
    var metadata = new JParqResultSetMetaData(null, List.of("v"), null, null, null, null, null, List.of(expression));
    try (var original = JParqResultSet.materializedResult("array", List.of("v"), List.of(Types.ARRAY),
        List.of(List.of((Object) new Object[]{
            1
        }))); var result = org.mockito.Mockito.spy(original)) {
      org.mockito.Mockito.doReturn(metadata).when(result).getMetaData();
      assertTrue(result.next());
      var error = assertThrows(java.sql.SQLSyntaxErrorException.class, () -> result.getArray(1));
      assertEquals("42000", error.getSQLState());
      assertEquals("42000",
          assertThrows(java.sql.SQLSyntaxErrorException.class, () -> result.getArray("v")).getSQLState());
    }
  }

  /** Known cast declarations preserve sibling values and temporal promotion. */
  @Test
  void castArrayElementsRetainCommonTypes(@TempDir Path directory) throws Exception {
    var expressions = Map.of("ARRAY[CAST(NULL AS DATE), DATE '2026-01-02']", Types.DATE,
        "ARRAY[CAST(NULL AS VARCHAR(12)), 'value']", Types.VARCHAR, "ARRAY[CAST(NULL AS CHAR(4)), 'value']",
        Types.VARCHAR, "ARRAY[CAST(NULL AS BINARY(4)), CAST(NULL AS VARBINARY(4))]", Types.VARBINARY,
        "ARRAY[CAST(NULL AS BOOLEAN), TRUE]", Types.BOOLEAN,
        "ARRAY[CAST(NULL AS TIMESTAMP(9)), TIMESTAMP '2026-01-02 03:04:05.123456789']", Types.TIMESTAMP,
        "ARRAY[CAST(NULL AS TIME), CURRENT_TIME]", Types.TIME_WITH_TIMEZONE,
        "ARRAY[CAST(NULL AS TIMESTAMP), CURRENT_TIMESTAMP]", Types.TIMESTAMP_WITH_TIMEZONE);
    try (var connection = connect(directory); var statement = connection.createStatement()) {
      for (var expression : expressions.entrySet()) {
        for (String query : List.of("SELECT " + expression.getKey() + " AS a",
            "SELECT a FROM (SELECT " + expression.getKey() + " AS a) t",
            "WITH t AS (SELECT " + expression.getKey() + " AS a) SELECT a FROM t")) {
          try (var result = statement.executeQuery(query)) {
            assertTrue(result.next(), query);
            var array = result.getArray(1);
            assertEquals(expression.getValue(), array.getBaseType(), query);
            Object[] elements = (Object[]) array.getArray();
            assertNull(elements[0], query);
            if (expression.getValue() == Types.VARBINARY) {
              assertNull(elements[1], query);
            } else {
              assertTrue(elements[1] != null, query);
            }
            if (expression.getValue() == Types.TIMESTAMP) {
              assertEquals(java.sql.Timestamp.valueOf("2026-01-02 03:04:05.123456789"), elements[1], query);
            }
          }
        }
      }
    }
  }

  /**
   * Both JDBC array accessors preserve non-null, empty, null, and computed
   * results.
   */
  @Test
  void jdbcArrayAccess(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory); var statement = connection.createStatement()) {
      for (String sql : List.of("SELECT ints AS a FROM s WHERE id = 1", "SELECT ARRAY[1,2] AS a",
          "SELECT a FROM (SELECT ARRAY[1,2] AS a) t", "WITH t AS (SELECT ARRAY[1,2] AS a) SELECT a FROM t",
          "SELECT ints AS a FROM s WHERE id = 1 UNION ALL SELECT ARRAY[1,2]")) {
        try (var result = statement.executeQuery(sql)) {
          while (result.next()) {
            Object[] expected = (Object[]) result.getObject(1);
            assertArrayEquals(expected, (Object[]) result.getArray(1).getArray(), sql);
            assertFalse(result.wasNull());
            assertArrayEquals(expected, (Object[]) result.getArray("a").getArray(), sql);
            assertFalse(result.wasNull());
          }
        }
      }
      try (var result = statement.executeQuery("SELECT ints AS a FROM s ORDER BY id")) {
        assertTrue(result.next());
        final var retained = result.getArray(1);
        assertTrue(result.next());
        assertNull(result.getArray(1));
        assertTrue(result.wasNull());
        assertNull(result.getArray("a"));
        assertTrue(result.wasNull());
        result.close();
        assertArrayEquals(new Object[]{
            1, 2
        }, (Object[]) retained.getArray());
        assertThrows(java.sql.SQLException.class, () -> result.getArray(1));
        assertThrows(java.sql.SQLException.class, () -> result.getArray("a"));
      }
      try (var result = statement.executeQuery("SELECT ARRAY[] AS a, 1 AS scalar")) {
        assertTrue(result.next());
        assertArrayEquals(new Object[0], (Object[]) result.getArray("a").getArray());
        assertFalse(result.wasNull());
        assertThrows(java.sql.SQLDataException.class, () -> result.getArray("scalar"));
        assertThrows(java.sql.SQLException.class, () -> result.getArray(0));
        assertThrows(java.sql.SQLException.class, () -> result.getArray(3));
        assertThrows(java.sql.SQLException.class, () -> result.getArray("missing"));
      }
    }
  }

  @Test
  void computedArrayReturnsObjectArray(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ARRAY[4,6]")) {
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          4, 6
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  @Test
  void physicalArrayColumnReturnsObjectArray(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ints FROM s WHERE id = 1")) {
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          1, 2
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  @Test
  void unionAllMixingPhysicalAndComputedArraysReturnsOneRepresentation(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ints FROM s WHERE id = 1 UNION ALL SELECT ARRAY['x']")) {
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          1, 2
      }, (Object[]) result.getObject(1));
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          "x"
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  @Test
  void nestedComputedArraysUseObjectArrays(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ARRAY[ARRAY[1,2],ARRAY[3]]")) {
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          new Object[]{
              1, 2
          }, new Object[]{
              3
          }
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  @Test
  void derivedTableAndCteArraysUseObjectArrays(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT a FROM (SELECT ARRAY[4,6] AS a) t")) {
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          4, 6
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("WITH t AS (SELECT ARRAY[4,6] AS a) SELECT a FROM t")) {
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          4, 6
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  @Test
  void columnClassNameReportsObjectArrayForEveryArrayColumn(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ARRAY[4,6], ints FROM s WHERE id = 1")) {
      ResultSetMetaData metaData = result.getMetaData();
      assertEquals(Types.ARRAY, metaData.getColumnType(1));
      assertEquals(Object[].class.getName(), metaData.getColumnClassName(1));
      assertEquals(Types.ARRAY, metaData.getColumnType(2));
      assertEquals(Object[].class.getName(), metaData.getColumnClassName(2));
    }
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ints FROM s WHERE id = 1 UNION ALL SELECT ARRAY['x']")) {
      ResultSetMetaData metaData = result.getMetaData();
      assertEquals(Types.ARRAY, metaData.getColumnType(1));
      assertEquals(Object[].class.getName(), metaData.getColumnClassName(1));
    }
  }

  @Test
  void getStringRendersArraysReadably(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ARRAY[4,6], ARRAY[[1,2],[3]]")) {
      assertTrue(result.next());
      assertEquals("[4, 6]", result.getString(1));
      assertEquals("[[1, 2], [3]]", result.getString(2));
      assertFalse(result.next());
    }
  }

  @Test
  void nullArrayValuesRemainNull(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT NULL, CAST(NULL AS ARRAY<INT>)")) {
      assertTrue(result.next());
      assertNull(result.getObject(1));
      Object value = result.getObject(2);
      assertTrue(value == null || value instanceof Object[], "expected null or Object[] but got " + value);
      assertFalse(result.next());
    }
  }

  @Test
  void arraysCompareByContentWithScalarSubqueryArrays(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory); var statement = connection.createStatement()) {
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] = (SELECT ARRAY[1,2]) ORDER BY id", 1, 2);
      assertIds(statement, "SELECT id FROM s WHERE (SELECT ARRAY[1,2]) = ARRAY[1,2] ORDER BY id", 1, 2);
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] IN (SELECT ARRAY[1,2]) ORDER BY id", 1, 2);
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[s.id] IN (SELECT ARRAY[i.id] FROM s i) ORDER BY id", 1, 2);
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] <> (SELECT ARRAY[1,2]) ORDER BY id");
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] NOT IN (SELECT ARRAY[1,2]) ORDER BY id");
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] = (SELECT ARRAY[1,3]) ORDER BY id");
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] < (SELECT ARRAY[1,3]) ORDER BY id", 1, 2);
    }
  }

  @Test
  void storedArraysCompareByContentWithComputedArrays(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory); var statement = connection.createStatement()) {
      assertIds(statement, "SELECT id FROM s WHERE ints = ARRAY[1,2] ORDER BY id", 1);
      assertIds(statement, "SELECT id FROM s WHERE ints = (SELECT ints FROM s WHERE id = 1) ORDER BY id", 1);
      assertIds(statement, "SELECT id FROM s WHERE ints = ints ORDER BY id", 1);
      assertIds(statement, "SELECT id FROM s WHERE ARRAY[1,2] <> ints ORDER BY id");
      assertIds(statement, "SELECT id FROM s WHERE ints <> ints ORDER BY id");
    }
  }

  @Test
  void setOperationOrderBySortsArraysByContent(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory); var statement = connection.createStatement()) {
      assertArrayColumn(statement, "SELECT ARRAY[2] UNION ALL SELECT ARRAY[1] UNION ALL SELECT ARRAY[3] ORDER BY 1",
          new Object[]{
              1
          }, new Object[]{
              2
          }, new Object[]{
              3
          });
      assertArrayColumn(statement,
          "SELECT ARRAY[2] UNION ALL SELECT ARRAY[1] UNION ALL SELECT ARRAY[3] ORDER BY 1 DESC", new Object[]{
              3
          }, new Object[]{
              2
          }, new Object[]{
              1
          });
      assertArrayColumn(statement, "SELECT ARRAY[ARRAY[2]] UNION ALL SELECT ARRAY[ARRAY[1]] ORDER BY 1", new Object[]{
          new Object[]{
              1
          }
      }, new Object[]{
          new Object[]{
              2
          }
      });
    }
  }

  @Test
  void scalarSubqueryArrayReportsArrayTypeAndClass(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT (SELECT ARRAY[1,2]) FROM s WHERE id = 1")) {
      var metaData = result.getMetaData();
      assertEquals(Types.ARRAY, metaData.getColumnType(1));
      assertEquals(Object[].class.getName(), metaData.getColumnClassName(1));
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          1, 2
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT (SELECT (ARRAY[1,2])) FROM s WHERE id = 1")) {
      var metaData = result.getMetaData();
      assertEquals(Types.ARRAY, metaData.getColumnType(1));
      assertEquals(Object[].class.getName(), metaData.getColumnClassName(1));
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          1, 2
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  @Test
  void scalarSubqueryMetadataDoesNotResolveInnerColumnsAgainstOuterSchema(@TempDir Path directory) throws Exception {
    writeDataset(directory);
    // s.b is BOOLEAN while t2.b is INT; the subquery type must not inherit the
    // outer column's type
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT (SELECT b FROM t2) AS x FROM s WHERE id = 1")) {
      var metaData = result.getMetaData();
      assertEquals(Types.OTHER, metaData.getColumnType(1));
      assertEquals(Object.class.getName(), metaData.getColumnClassName(1));
      assertTrue(result.next());
      assertEquals(42, result.getObject(1));
      assertFalse(result.next());
    }
  }

  @Test
  void scalarSubqueryOverStoredArrayReportsOtherUntilColumnSchemasAreResolved(@TempDir Path directory)
      throws Exception {
    writeDataset(directory);
    try (var connection = connect(directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT (SELECT ints FROM s WHERE id = 1) FROM s WHERE id = 1")) {
      var metaData = result.getMetaData();
      assertEquals(Types.OTHER, metaData.getColumnType(1));
      assertEquals(Object.class.getName(), metaData.getColumnClassName(1));
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          1, 2
      }, (Object[]) result.getObject(1));
      assertFalse(result.next());
    }
  }

  private static void assertIds(java.sql.Statement statement, String sql, int... expected)
      throws java.sql.SQLException {
    List<Integer> ids = new java.util.ArrayList<>();
    try (var result = statement.executeQuery(sql)) {
      while (result.next()) {
        ids.add(result.getInt(1));
      }
    }
    assertEquals(java.util.Arrays.stream(expected).boxed().toList(), ids, sql);
  }

  private static void assertArrayColumn(java.sql.Statement statement, String sql, Object[]... expected)
      throws java.sql.SQLException {
    List<Object> actual = new java.util.ArrayList<>();
    try (var result = statement.executeQuery(sql)) {
      while (result.next()) {
        actual.add(result.getObject(1));
      }
    }
    assertEquals(expected.length, actual.size(), sql);
    for (int i = 0; i < expected.length; i++) {
      assertArrayEquals(expected[i], (Object[]) actual.get(i), sql + " row " + (i + 1));
    }
  }

  private static Connection connect(Path directory) throws Exception {
    Class.forName("se.alipsa.jparq.JParqDriver");
    return DriverManager.getConnection("jdbc:jparq:" + directory.toAbsolutePath());
  }

  /**
   * Writes a minimal dataset with one physical array column plus a second table
   * whose column names shadow the first table's with different types.
   *
   * @param directory
   *          dataset directory
   * @throws IOException
   *           if writing fails
   */
  private static void writeDataset(Path directory) throws IOException {
    Schema schema = SchemaBuilder.record("array_row").fields().requiredInt("id").name("ints")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createArray(Schema.create(Schema.Type.INT))))
        .noDefault().name("b").type(Schema.create(Schema.Type.BOOLEAN)).withDefault(false).endRecord();
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("s.parquet").toUri()),
        new org.apache.hadoop.conf.Configuration());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).build()) {
      for (int id = 1; id <= 2; id++) {
        var record = new GenericData.Record(schema);
        record.put("id", id);
        record.put("ints",
            id == 1
                ? new GenericData.Array<>(schema.getField("ints").schema().getTypes().get(1), List.of(1, 2))
                : null);
        record.put("b", id == 1);
        writer.write(record);
      }
    }
    Schema t2Schema = SchemaBuilder.record("t2_row").fields().requiredInt("b").endRecord();
    var t2Output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve("t2.parquet").toUri()),
        new org.apache.hadoop.conf.Configuration());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(t2Output).withSchema(t2Schema).build()) {
      var record = new GenericData.Record(t2Schema);
      record.put("b", 42);
      writer.write(record);
    }
  }
}
