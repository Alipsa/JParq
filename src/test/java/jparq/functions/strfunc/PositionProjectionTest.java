package jparq.functions.strfunc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Regression coverage for named function arguments required by projection
 * pushdown.
 */
class PositionProjectionTest {
  @TempDir
  Path directory;

  /**
   * Writes string inputs and a lookup table for correlated subqueries.
   *
   * @throws Exception
   *           if fixture creation fails
   */
  @BeforeEach
  void writeFixture() throws Exception {
    writeTable("names", new String[]{
        "ax", "xb", "bxx", "", null
    });
    writeTable("other", new String[]{
        "x", "y"
    });
  }

  /**
   * Writes names with an id and a search substring.
   *
   * @param table
   *          the table filename without its extension
   * @param names
   *          string values, including nulls
   * @throws Exception
   *           if fixture creation fails
   */
  private void writeTable(String table, String[] names) throws Exception {
    var schema = SchemaBuilder.record("names").fields().requiredInt("id").optionalString("name")
        .requiredString("needle").endRecord();
    var conf = new Configuration(false);
    var file = new org.apache.hadoop.fs.Path(directory.resolve(table + ".parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, conf)).withSchema(schema)
        .withConf(conf).build()) {
      for (int i = 0; i < names.length; i++) {
        var record = new GenericData.Record(schema);
        record.put("id", i + 1);
        record.put("name", names[i]);
        record.put("needle", "x");
        writer.write(record);
      }
    }
  }

  /**
   * Keeps named arguments available to filters even when only id is projected.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void filtersWithNamedArguments(boolean predicate, boolean projection) throws Exception {
    try (var connection = connect(predicate, projection); var statement = connection.createStatement()) {
      for (String expression : List.of("POSITION('x' IN name)", "POSITION(needle IN name)",
          "POSITION(n.needle IN n.name)", "POSITION('x' IN LOWER(n.name))", "LOCATE('x', name)",
          "POSITION('x' IN SUBSTRING(name FROM 1 FOR 3))")) {
        String query = "SELECT id FROM names n WHERE " + expression + " = 2 ORDER BY id";
        assertEquals(List.of(1, 3), ids(statement, query), query);
      }
      assertEquals(List.of(1, 3),
          ids(statement, "SELECT id FROM names WHERE SUBSTRING(name FROM 2 FOR 1) = 'x' ORDER BY id"));
      assertEquals(List.of(1),
          ids(statement, "SELECT id FROM names WHERE OVERLAY(name PLACING 'Q' FROM 1 FOR 1) = 'Qx' ORDER BY id"));
    }
  }

  /**
   * Retains columns used only by a named function argument in ORDER BY.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void ordersWithPosition(boolean predicate, boolean projection) throws Exception {
    try (var connection = connect(predicate, projection); var statement = connection.createStatement()) {
      assertEquals(List.of(4, 2, 1, 3, 5), ids(statement, "SELECT id FROM names ORDER BY POSITION('x' IN name), id"));
    }
  }

  /**
   * Retains outer columns used by a correlated subquery in a named argument.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void correlatesPositionOperand(boolean predicate, boolean projection) throws Exception {
    try (var connection = connect(predicate, projection); var statement = connection.createStatement()) {
      assertEquals(List.of(1, 3), ids(statement, """
          SELECT id FROM names n
          WHERE POSITION((SELECT o.needle FROM other o WHERE o.name = n.needle) IN n.name) = 2
          ORDER BY id
          """));
    }
  }

  /**
   * Opens a connection with the requested pushdown options.
   *
   * @param predicate
   *          whether predicate pushdown is enabled
   * @param projection
   *          whether projection pushdown is enabled
   * @return a connection to the fixture directory
   * @throws SQLException
   *           if opening the connection fails
   */
  private Connection connect(boolean predicate, boolean projection) throws SQLException {
    return DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection);
  }

  /**
   * Reads ids in query order.
   *
   * @param statement
   *          the statement used to execute the query
   * @param query
   *          SQL returning ids in its first column
   * @return the ids returned by the query
   * @throws SQLException
   *           if query execution or iteration fails
   */
  private static List<Integer> ids(Statement statement, String query) throws SQLException {
    try (var result = statement.executeQuery(query)) {
      List<Integer> ids = new ArrayList<>();
      while (result.next()) {
        ids.add(result.getInt(1));
      }
      return ids;
    }
  }
}
