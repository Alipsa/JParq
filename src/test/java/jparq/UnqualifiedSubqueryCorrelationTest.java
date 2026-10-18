package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Verifies inner-first binding and unqualified outer fallback in subqueries.
 */
class UnqualifiedSubqueryCorrelationTest {
  @TempDir
  Path base;

  /**
   * Writes two integer columns using identical values for deterministic matches.
   *
   * @param table
   *          fixture table name
   * @param first
   *          first field name
   * @param second
   *          second field name
   * @param count
   *          number of records
   * @throws Exception
   *           if writing fails
   */
  private void write(String table, String first, String second, int count) throws Exception {
    Schema schema = org.apache.avro.SchemaBuilder.record("Row").fields().requiredInt(first).requiredInt(second)
        .endRecord();
    var conf = new Configuration(false);
    var file = new org.apache.hadoop.fs.Path(base.resolve(table + ".parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, conf)).withSchema(schema)
        .withConf(conf).build()) {
      for (int id = 0; id < count; id++) {
        var row = new GenericData.Record(schema);
        row.put(first, id);
        row.put(second, id % 5);
        writer.write(row);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void havingRestoresOuterFallbackWithoutReplacingInnerColumns(boolean predicate, boolean projection) throws Exception {
    write("big", "id", "g", 10);
    write("other", "k", "m", 5);
    write("small", "id", "g", 5);
    try (var conn = connect(predicate, projection)) {
      String grouped = "SELECT g, COUNT(*) FROM big GROUP BY g HAVING ";
      assertEquals(Set.of(0, 1, 2, 3, 4), ids(conn, grouped + "EXISTS (SELECT 1 FROM other o WHERE o.k = g)"));
      assertEquals(Set.of(0, 1, 2, 3, 4), ids(conn, grouped + "g IN (SELECT o.k FROM other o WHERE o.m = g)"));
      assertEquals(Set.of(1, 2, 3, 4), ids(conn, grouped + "(SELECT MAX(o.k) FROM other o WHERE o.m = g) > 0"));
      assertTrue(ids(conn, grouped + "EXISTS (SELECT 1 FROM small s WHERE s.g = 4 AND g = 0)").isEmpty());
      assertEquals(Set.of(0, 1, 2, 3), ids(conn, grouped + "g < (SELECT MAX(g) FROM small)"));
      assertEquals(Set.of(0, 1, 2, 3, 4),
          ids(conn, grouped + "EXISTS (SELECT 1 FROM (SELECT k AS g FROM other) d WHERE g = 4)"));
      assertEquals(Set.of(0, 1, 2, 3, 4),
          ids(conn, grouped + "EXISTS (WITH d AS (SELECT k AS g FROM other) SELECT 1 FROM d WHERE g = 4)"));
    }
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void whereResolvesUnqualifiedCorrelationsAndKeepsLocalNames(boolean predicate, boolean projection) throws Exception {
    write("big", "id", "g", 10);
    write("other", "k", "m", 5);
    write("small", "id", "g", 5);
    try (var conn = connect(predicate, projection)) {
      String select = "SELECT b.id FROM big b WHERE ";
      Set<Integer> matches = Set.of(0, 1, 2, 3, 4);
      assertEquals(matches, ids(conn, select + "EXISTS (SELECT 1 FROM other o WHERE o.k = id)"));
      assertEquals(matches, ids(conn, select + "b.id IN (SELECT o.k FROM other o WHERE o.m = g)"));
      assertEquals(matches, ids(conn, "WITH b AS (SELECT id, g FROM big) SELECT id FROM b "
          + "WHERE id IN (SELECT o.k FROM other o WHERE o.m = g)"));
      assertEquals(matches, ids(conn,
          "SELECT b.id FROM (SELECT id, g FROM big) b " + "WHERE b.id IN (SELECT o.k FROM other o WHERE o.m = g)"));
      assertEquals(matches, ids(conn, select + "b.id = ANY (SELECT o.k FROM other o WHERE o.m = g)"));
      assertEquals(Set.of(1, 2, 3, 4, 6, 7, 8, 9),
          ids(conn, select + "(SELECT MAX(o.k) FROM other o WHERE o.m = g) > 0"));
      assertTrue(ids(conn, select + "EXISTS (SELECT 1 FROM small s WHERE s.g = 4 AND g = 0)").isEmpty());
      assertEquals(Set.of(0, 1, 2, 3), ids(conn, select + "b.id IN (SELECT id FROM small WHERE g < 4)"));
    }
  }

  /**
   * Opens a connection with independent pushdown settings.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @return fixture connection
   * @throws Exception
   *           if connection creation fails
   */
  private Connection connect(boolean predicate, boolean projection) throws Exception {
    return DriverManager
        .getConnection("jdbc:jparq:" + base + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection);
  }

  /**
   * Reads identifiers or grouping keys from the first result column.
   *
   * @param connection
   *          fixture connection
   * @param sql
   *          query to execute
   * @return distinct first-column values
   * @throws Exception
   *           if execution fails
   */
  private static Set<Integer> ids(Connection connection, String sql) throws Exception {
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      Set<Integer> ids = new HashSet<>();
      while (result.next()) {
        ids.add(result.getInt(1));
      }
      return ids;
    }
  }
}
