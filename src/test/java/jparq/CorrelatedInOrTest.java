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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Verifies correlated IN retains its Boolean position within a disjunction. */
class CorrelatedInOrTest {
  @TempDir
  Path base;

  /** Creates disjoint five-row and 89-row predicate results. */
  @BeforeEach
  void fixture() throws Exception {
    write("big", 100, 0);
    write("small", 5, 0);
    write("shifted", 5, 1);
  }

  /**
   * Writes identifiers and deterministic correlation groups.
   *
   * @param name
   *          table name
   * @param count
   *          number of rows
   * @param shift
   *          correlation group offset
   * @throws Exception
   *           if writing fails
   */
  private void write(String name, int count, int shift) throws Exception {
    Schema schema = new Schema.Parser().parse("""
        {"type":"record","name":"Row","fields":[{"name":"id","type":"int"},{"name":"g","type":"int"}]}
        """);
    var conf = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(base.resolve(name + ".parquet").toUri()),
        conf);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(conf).build()) {
      for (int id = 0; id < count; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("g", (id + shift) % 5);
        writer.write(row);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void correlatedInAndOrdinaryPredicateComposeWithOr(boolean predicate, boolean projection) throws Exception {
    try (var conn = DriverManager.getConnection(
        "jdbc:jparq:" + base + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection)) {
      String correlated = "b.id IN (SELECT s2.id FROM small s2 WHERE s2.g = b.g)";
      Set<Integer> matchingIn = ids(conn, correlated);
      Set<Integer> matchingRange = ids(conn, "b.id > 10");
      assertEquals(Set.of(0, 1, 2, 3, 4), matchingIn);
      assertEquals(89, matchingRange.size());
      Set<Integer> expected = new HashSet<>(matchingIn);
      expected.addAll(matchingRange);
      assertEquals(94, expected.size());
      String combined = correlated + " OR b.id > 10";
      var parsed = se.alipsa.jparq.engine.SqlParser.parseSelect("SELECT b.id FROM big b WHERE " + combined);
      assertInstanceOf(net.sf.jsqlparser.expression.operators.conditional.OrExpression.class, parsed.where());
      assertEquals(expected, ids(conn, combined), parsed.where().toString());
      assertEquals(expected, ids(conn, "b.id > 10 OR " + correlated));
      assertEquals(expected, ids(conn, "(" + correlated + ") OR b.id > 10"));
      assertTrue(ids(conn, correlated + " AND b.id > 10").isEmpty());
      assertEquals(matchingIn, ids(conn, correlated + " AND b.id < 10"));
      assertEquals(14, ids(conn, correlated + " AND b.id < 10 OR b.id > 90").size());
      assertEquals(6, ids(conn, "NOT (" + combined + ")").size());
      assertEquals(14, ids(conn, correlated + " OR b.id > 10 AND b.id < 20").size());
      assertEquals(14, ids(conn, "(" + combined + ") AND b.id < 20").size());
      assertEquals(95, ids(conn, correlated.replace(" IN ", " NOT IN ") + " OR b.id > 10").size());
      String shifted = "b.id IN (SELECT s2.id FROM shifted s2 WHERE s2.g = b.g)";
      assertTrue(ids(conn, shifted).isEmpty(), "Correlations must use each outer row rather than the first result");
      assertEquals(matchingRange, ids(conn, shifted + " OR b.id > 10"));
      assertEquals(matchingIn, ids(conn, "b.id IN (SELECT s2.id FROM shifted s2)"));
    }
  }

  /**
   * Reads a predicate's matching outer identifiers.
   *
   * @param conn
   *          query connection
   * @param predicate
   *          outer WHERE predicate
   * @return matching identifiers
   * @throws Exception
   *           if execution fails
   */
  private static Set<Integer> ids(Connection conn, String predicate) throws Exception {
    try (var stmt = conn.createStatement();
        var result = stmt.executeQuery("SELECT b.id FROM big b WHERE " + predicate)) {
      Set<Integer> ids = new HashSet<>();
      while (result.next()) {
        ids.add(result.getInt(1));
      }
      return ids;
    }
  }
}
