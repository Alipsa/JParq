package jparq;

import static jparq.support.QueryTestSupport.rootCause;
import static jparq.support.QueryTestSupport.rows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.engine.InMemoryRecordReader;
import se.alipsa.jparq.engine.JSqlParserSupport;
import se.alipsa.jparq.engine.SqlParser;

/** Verifies expression discovery inside SQL functions with named operands. */
class NamedArgumentTraversalTest {
  @TempDir
  Path directory;

  /**
   * Evaluates windows nested in named operands, including nested scalar calls.
   *
   * @param expression
   *          the projection containing a window
   * @param expected
   *          the ordered result values
   * @throws SQLException
   *           if execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "SUBSTRING(name FROM ROW_NUMBER() OVER (ORDER BY id)) | abc,bc,c",
      "SUBSTRING(name FROM 1 FOR ROW_NUMBER() OVER (ORDER BY id)) | a,ab,abc",
      "POSITION(CAST(ROW_NUMBER() OVER (ORDER BY id) AS VARCHAR) IN '123') | 1,2,3",
      "POSITION('a' IN SUBSTRING(name FROM ROW_NUMBER() OVER (ORDER BY id))) | 1,0,0",
      "OVERLAY(name PLACING 'X' FROM ROW_NUMBER() OVER (ORDER BY id) FOR 1) | Xbc,aXc,abX",
      "OVERLAY(name PLACING 'X' FROM 1 FOR ROW_NUMBER() OVER (ORDER BY id)) | Xbc,Xc,X",
      "OVERLAY(name PLACING CAST(ROW_NUMBER() OVER (ORDER BY id) AS VARCHAR) FROM 1 FOR 1) | 1bc,2bc,3bc",
      "TRIM('x' FROM CAST(ROW_NUMBER() OVER (ORDER BY id) AS VARCHAR)) | 1,2,3",
      "TRIM(BOTH FROM CAST(ROW_NUMBER() OVER (ORDER BY id) AS VARCHAR)) | 1,2,3",
      "TRIM(LEADING FROM CAST(ROW_NUMBER() OVER (ORDER BY id) AS VARCHAR)) | 1,2,3",
      "TRIM(TRAILING FROM CAST(ROW_NUMBER() OVER (ORDER BY id) AS VARCHAR)) | 1,2,3",
      "TRIM(FROM CAST(ROW_NUMBER() OVER (ORDER BY id) AS VARCHAR)) | 1,2,3"
  })
  void plansWindowsInNamedArguments(String expression, String expected) throws SQLException {
    String sql = "SELECT " + expression
        + " AS p FROM (VALUES (3, 'abc'), (1, 'abc'), (2, 'abc')) names(id, name) ORDER BY id";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (boolean prepared : List.of(false, true)) {
        assertEquals(List.of(expected.split(",")),
            rows(connection, sql, prepared).stream().map(row -> row.getFirst().toString()).toList());
      }
    }
  }

  /**
   * Rejects unresolved columns during preparation regardless of evaluation.
   *
   * @param sql
   *          a table-free query with a missing column
   * @throws SQLException
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT POSITION('x' IN name)", "SELECT POSITION(name IN 'abc')", "SELECT SUBSTRING(name FROM 1)",
      "SELECT SUBSTRING('abc' FROM name)", "SELECT SUBSTRING('abc' FROM 1 FOR name)",
      "SELECT OVERLAY(name PLACING 'x' FROM 1)", "SELECT OVERLAY('abc' PLACING name FROM 1)",
      "SELECT OVERLAY('abc' PLACING 'x' FROM name)", "SELECT OVERLAY('abc' PLACING 'x' FROM 1 FOR name)",
      "SELECT TRIM('x' FROM name)", "SELECT TRIM(BOTH FROM name)", "SELECT TRIM(LEADING FROM name)",
      "SELECT TRIM(TRAILING FROM name)", "SELECT TRIM(FROM name)",
      "SELECT CASE WHEN FALSE THEN POSITION('x' IN name) ELSE 1 END", "SELECT 1 WHERE POSITION('x' IN name) > 0",
      "SELECT 1 ORDER BY SUBSTRING(name FROM 1)", "SELECT COUNT(*) GROUP BY SUBSTRING(name FROM 1)",
      "SELECT COUNT(*) HAVING POSITION('x' IN name) > 0"
  })
  void rejectsTableFreeColumnsInNamedArguments(String sql) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      var error = assertThrows(SQLException.class, () -> connection.prepareStatement(sql));
      // SQLState classification for table-free missing columns is tracked in issue
      // #490.
      assertEquals("Missing column without FROM: name", rootCause(error).getMessage());
    }
  }

  /**
   * Retains physical outer columns used only by subqueries in named arguments.
   *
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if fixture creation or execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void retainsCorrelatedSubqueryDependencies(boolean projection) throws Exception {
    var schema = SchemaBuilder.record("NameRow").fields().requiredInt("id").requiredString("name").endRecord();
    var configuration = new Configuration(false);
    var file = new org.apache.hadoop.fs.Path(directory.resolve("names.parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, configuration))
        .withSchema(schema).withConf(configuration).build()) {
      for (int id = 1; id <= 3; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("name", "abc".substring(id - 1));
        writer.write(row);
      }
    }
    String sql = "SELECT id, SUBSTRING((SELECT name FROM (VALUES (0)) v(x)) FROM 1) AS p " + "FROM names ORDER BY id";
    try (
        var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?projectionPushdown=" + projection)) {
      for (boolean prepared : List.of(false, true)) {
        assertEquals(List.of(List.of(1, "abc"), List.of(2, "bc"), List.of(3, "c")), rows(connection, sql, prepared));
      }
    }
  }

  /**
   * Keeps columns inside scalar subqueries in their own query scope.
   *
   * @throws SQLException
   *           if preparation or execution fails
   */
  @Test
  void tableFreeNamedArgumentsPreserveNestedScopes() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertEquals(List.of(List.of("bc", 2, "aXc")),
          rows(connection,
              "SELECT SUBSTRING((SELECT name FROM (VALUES ('abc')) v(name)) FROM 2), "
                  + "POSITION('b' IN (SELECT name FROM (VALUES ('abc')) v(name))), "
                  + "OVERLAY('abc' PLACING (SELECT name FROM (VALUES ('X')) v(name)) FROM 2)",
              true));
    }
  }

  /**
   * Validates predicate subqueries even when CASE prevents their execution.
   *
   * @throws SQLException
   *           if connection setup fails
   */
  @Test
  void validatesUnevaluatedPredicateSubqueriesInNamedArguments() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      var error = assertThrows(SQLException.class,
          () -> connection.prepareStatement("SELECT SUBSTRING('abc' FROM CASE WHEN FALSE THEN "
              + "CASE WHEN 1 IN (SELECT 1, 2) THEN 1 ELSE 2 END ELSE 1 END)"));
      assertEquals("42000", error.getSQLState());
      assertTrue(error.getMessage().contains("exactly one column"), error.getMessage());
    }
  }

  /**
   * Rejects unresolved columns hidden in standalone UNNEST operands.
   *
   * @param expression
   *          an array element containing an unresolved column
   * @throws SQLException
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "POSITION('x' IN name)", "SUBSTRING(name FROM 1)", "OVERLAY('abc' PLACING name FROM 1)", "TRIM('x' FROM name)"
  })
  void validatesNamedArgumentsInUnnest(String expression) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      var error = assertThrows(SQLException.class,
          () -> statement.executeQuery("SELECT * FROM UNNEST(ARRAY[" + expression + "]) t(x)"));
      assertEquals("42000", error.getSQLState());
      assertTrue(error.getMessage().contains("Column not found in UNNEST argument"), error.getMessage());
    }
  }

  /**
   * Keeps predicates with named operands dependent on both join participants.
   *
   * @param pushdown
   *          whether predicate pushdown is enabled
   * @throws Exception
   *           if fixture creation or execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void parquetJoinsPreserveMixedNamedArgumentPredicates(boolean pushdown) throws Exception {
    var leftSchema = SchemaBuilder.record("Names").fields().requiredInt("id").requiredString("name").endRecord();
    var rightSchema = SchemaBuilder.record("Needles").fields().requiredString("needle").endRecord();
    writeRows("names", leftSchema, List.of(List.of(1, "abc"), List.of(2, "def")));
    writeRows("needles", rightSchema, List.of(List.of("b"), List.of("e")));
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown)) {
      String sql = "SELECT a.id, b.needle FROM names a CROSS JOIN needles b "
          + "WHERE a.id = 1 OR POSITION(b.needle IN a.name) > 0 ORDER BY a.id, b.needle";
      for (boolean prepared : List.of(false, true)) {
        assertEquals(List.of(List.of(1, "b"), List.of(1, "e"), List.of(2, "e")), rows(connection, sql, prepared));
      }
    }
  }

  /**
   * Drops residual predicates whose named operands reference an unavailable join
   * side.
   *
   * @throws Exception
   *           if parsing or result-set evaluation fails
   */
  @Test
  void resultSetPrunesUnavailableNamedArgumentQualifiers() throws Exception {
    var schema = SchemaBuilder.record("Input").fields().requiredInt("id").requiredString("name").endRecord();
    var row = new GenericData.Record(schema);
    row.put("id", 2);
    row.put("name", "abc");
    var select = SqlParser.parseSelect("SELECT id FROM a");
    var residual = JSqlParserSupport.parseExpression("a.id = 1 OR POSITION('x' IN b.name) > 0");
    try (var result = new JParqResultSet(new InMemoryRecordReader(List.of(row), schema, "a"), select, "a", "PUBLIC",
        null, residual, List.of("id"), List.of("id"), null)) {
      assertTrue(result.next());
      assertEquals(2, result.getInt(1));
      assertFalse(result.next());
    }
  }

  /**
   * Evaluates standard TRIM forms without an explicit trim character.
   *
   * @param expression
   *          standard TRIM expression
   * @param expected
   *          expected string value
   * @throws SQLException
   *           if preparation or execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "TRIM(BOTH FROM '  x  ') | x", "TRIM(FROM '  x  ') | x", "TRIM(LEADING FROM '  x') | x",
      "TRIM(TRAILING FROM 'x  ') | x", "TRIM(BOTH FROM CAST(NULL AS VARCHAR)) | <null>", "TRIM(FROM MIN(name)) | x"
  })
  void standardTrimFormsExecute(String expression, String expected) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (boolean prepared : List.of(false, true)) {
        var actual = rows(connection, "SELECT " + expression + " FROM (VALUES ('  x  ')) v(name)", prepared);
        assertEquals(1, actual.size());
        assertEquals("<null>".equals(expected) ? null : expected, actual.getFirst().getFirst());
      }
    }
  }

  /**
   * Reports the leftmost unresolved TRIM operand first.
   */
  @Test
  void trimOperandsAreValidatedInSourceOrder() {
    var error = assertThrows(IllegalArgumentException.class, () -> SqlParser.parseSelect("SELECT TRIM(a FROM b)"));
    assertEquals("Missing column without FROM: a", rootCause(error).getMessage());
  }

  /**
   * Writes physical join inputs so join predicate planning receives resolved
   * schemas.
   *
   * @param table
   *          physical table name
   * @param schema
   *          record schema
   * @param values
   *          ordered field values for each row
   * @throws Exception
   *           if fixture creation fails
   */
  private void writeRows(String table, org.apache.avro.Schema schema, List<List<Object>> values) throws Exception {
    var configuration = new Configuration(false);
    var file = new org.apache.hadoop.fs.Path(directory.resolve(table + ".parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, configuration))
        .withSchema(schema).withConf(configuration).build()) {
      for (var fields : values) {
        var row = new GenericData.Record(schema);
        for (int i = 0; i < fields.size(); i++) {
          row.put(i, fields.get(i));
        }
        writer.write(row);
      }
    }
  }

}
