package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
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
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Regression coverage for SQL apostrophe escapes in values and pushed filters.
 */
class EscapedStringLiteralTest {
  @TempDir
  Path directory;

  /**
   * Writes strings with single and repeated apostrophes.
   *
   * @throws Exception
   *           if writing the fixture fails
   */
  @BeforeEach
  void writeFixture() throws Exception {
    var schema = SchemaBuilder.record("names").fields().requiredInt("id").requiredString("name").endRecord();
    var file = new org.apache.hadoop.fs.Path(directory.resolve("names.parquet").toUri());
    var conf = new Configuration(false);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, conf)).withSchema(schema)
        .withConf(conf).build()) {
      List<String> names = List.of("O'Brien", "O''Brien", "Other", "'", "''");
      for (int i = 0; i < names.size(); i++) {
        var record = new GenericData.Record(schema);
        record.put("id", i + 1);
        record.put("name", names.get(i));
        writer.write(record);
      }
    }
  }

  /**
   * Checks projections, functions, materialization, and correlation decode once.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void evaluatesEscapedValues(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery("""
          SELECT 'it''s' AS s, LENGTH('it''s') AS n,
                 CASE WHEN 'it''s' = 'it' || CHAR(39) || 's' THEN 'equal' ELSE 'different' END AS cmp,
                 '' AS empty_value, '''' AS quote_value, '''''' AS two_quotes,
                 'a''''b' AS consecutive, N'O''Brien' AS national_value,
                 $$it''s$$ AS dollar_value, 'a\\b''c' AS backslash_value,
                 'a''--b/*c*/' AS comment_value
          """)) {
        assertTrue(result.next());
        assertEquals("it's", result.getString(1));
        assertEquals(4, result.getInt(2));
        assertEquals("equal", result.getString(3));
        assertEquals("", result.getString(4));
        assertEquals("'", result.getString(5));
        assertEquals("''", result.getString(6));
        assertEquals("a''b", result.getString(7));
        assertEquals("O'Brien", result.getString(8));
        assertEquals("it''s", result.getString(9));
        assertEquals("a\\b'c", result.getString(10));
        assertEquals("a'--b/*c*/", result.getString(11));
        assertFalse(result.next());
      }
      assertEquals(List.of(1), ids(connection, """
          WITH escaped AS (SELECT 'O''Brien' AS name)
          SELECT n.id FROM names n JOIN (SELECT name FROM escaped) e ON n.name = e.name ORDER BY n.id
          """));
      assertEquals(List.of(1), ids(connection, """
          SELECT n.id FROM names n WHERE EXISTS (
            SELECT 1 FROM names b WHERE b.id = n.id AND n.name = 'O''Brien') ORDER BY n.id
          """));
      try (var result = statement.executeQuery("""
          SELECT (SELECT n.name) AS name FROM names n WHERE n.id = 2
          """)) {
        assertTrue(result.next());
        assertEquals("O''Brien", result.getString(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Verifies every literal filter path against physical strings with pushdown on
   * and off.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void filtersPhysicalStrings(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown)) {
      for (String predicate : List.of("name = 'O''Brien'", "'O''Brien' = name", "name IN ('O''Brien')",
          "name BETWEEN 'O''Brien' AND 'O''Brien'", "name LIKE 'O''B%'", "name = ANY(ARRAY['O''Brien'])")) {
        assertEquals(List.of(1), ids(connection, "SELECT id FROM names WHERE " + predicate + " ORDER BY id"),
            predicate);
      }
      assertEquals(List.of(2), ids(connection, "SELECT id FROM names WHERE name = 'O''''Brien' ORDER BY id"));
      assertEquals(List.of(2, 3, 4, 5),
          ids(connection, "SELECT id FROM names WHERE NOT (name = 'O''Brien') ORDER BY id"));
      assertEquals(List.of(4, 5), ids(connection, "SELECT id FROM names WHERE name LIKE '''%' ORDER BY id"));
      assertEquals(List.of(2), ids(connection, "SELECT id FROM names WHERE name = $$O''Brien$$ ORDER BY id"));
    }
  }

  /**
   * Checks JDBC parameter rendering and rebinding preserve literal apostrophes.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if querying fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void bindsStringsWithoutDoubleDecoding(boolean pushdown) throws Exception {
    try (var connection = connect(pushdown);
        var statement = connection.prepareStatement("SELECT id, ? AS supplied FROM names WHERE name = ?")) {
      List<String> values = List.of("O'Brien", "O''Brien", "Other", "'", "''");
      for (int i = 0; i < values.size(); i++) {
        statement.setString(1, values.get(i));
        statement.setString(2, values.get(i));
        try (var result = statement.executeQuery()) {
          assertTrue(result.next(), values.get(i));
          assertEquals(i + 1, result.getInt(1));
          assertEquals(values.get(i), result.getString(2));
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Opens the fixture database with matching pushdown settings.
   *
   * @param pushdown
   *          whether pushdown is enabled
   * @return the connection
   * @throws Exception
   *           if connection setup fails
   */
  private Connection connect(boolean pushdown) throws Exception {
    return DriverManager.getConnection(
        "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
  }

  /**
   * Collects the first output column as row identifiers.
   *
   * @param connection
   *          the fixture connection
   * @param sql
   *          the query
   * @return selected identifiers
   * @throws Exception
   *           if querying fails
   */
  private List<Integer> ids(Connection connection, String sql) throws Exception {
    List<Integer> ids = new ArrayList<>();
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      while (result.next()) {
        ids.add(result.getInt(1));
      }
    }
    return ids;
  }
}
