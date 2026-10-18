package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void inPrecedenceIsPreservedAcrossEveryQueryScope(boolean predicate, boolean projection) throws Exception {
    try (var conn = DriverManager.getConnection(
        "jdbc:jparq:" + base + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection)) {
      String condition = "b.id IN (SELECT id FROM small) OR b.id > 10";
      Set<Integer> expected = ids(conn, condition);
      assertEquals(94, expected.size());
      for (String query : List.of("SELECT id FROM (SELECT b.id FROM big b WHERE " + condition + ") t",
          "WITH t AS (SELECT b.id FROM big b WHERE " + condition + ") SELECT id FROM t",
          "SELECT b.id FROM big b JOIN small s ON b.g = s.g WHERE " + condition,
          "SELECT b.id FROM big b JOIN small s ON b.g = s.g AND (" + condition + ")",
          "SELECT b.id FROM big b GROUP BY b.id HAVING " + condition)) {
        assertEquals(expected, new HashSet<>(queryIds(conn, query)), query);
      }
      for (String nestedCondition : List.of("b.id IN (SELECT s2.id FROM small s2 WHERE s2.g = b.g) OR b.id > 10",
          "b.id IN (0,1,2,3,4) OR b.id > 10")) {
        for (String query : List.of("SELECT id FROM (SELECT b.id FROM big b WHERE " + nestedCondition + ") t",
            "WITH t AS (SELECT b.id FROM big b WHERE " + nestedCondition + ") SELECT id FROM t",
            "SELECT b.id FROM big b JOIN small s ON b.g = s.g WHERE " + nestedCondition,
            "SELECT b.id FROM big b JOIN small s ON b.g = s.g AND (" + nestedCondition + ")")) {
          assertEquals(expected, new HashSet<>(queryIds(conn, query)), query);
        }
      }
      List<Integer> union = queryIds(conn, "SELECT b.id FROM big b WHERE b.id IN (SELECT id FROM small) "
          + "OR b.id > 98 UNION ALL SELECT id FROM small");
      assertEquals(11, union.size());
      assertEquals(1, java.util.Collections.frequency(union, 99));
      for (int id = 0; id < 5; id++) {
        assertEquals(2, java.util.Collections.frequency(union, id));
      }
      try (var stmt = conn.createStatement();
          var result = stmt.executeQuery("SELECT SUM(CASE WHEN " + condition + " THEN 1 ELSE 0 END) AS n FROM big b")) {
        assertTrue(result.next());
        assertEquals(94, result.getInt(1));
      }
      assertEquals(expected, ids(conn, "b.id IN (0,1,2,3,4) OR b.id > 10"));
      assertEquals(expected, ids(conn, "b.id > 10 OR b.id IN (0,1,2,3,4)"));
      assertEquals(90, ids(conn, "b.id IN (0) OR b.id > 10").size());
      assertEquals(95, ids(conn, "b.id NOT IN (0,1,2,3,4) OR b.id > 10").size());
      assertEquals(89, ids(conn, "(b.id > 10) IN (TRUE OR FALSE)").size());
    }
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void subqueryCteShadowsThePhysicalTable(boolean predicate, boolean projection) throws Exception {
    try (var conn = DriverManager.getConnection(
        "jdbc:jparq:" + base + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection)) {
      Set<Integer> expected = new HashSet<>(queryIds(conn, "SELECT id FROM big"));
      for (String reference : List.of("b.id", "id")) {
        assertEquals(expected, ids(conn,
            "EXISTS (WITH small AS (SELECT id AS q FROM big) " + "SELECT 1 FROM small WHERE q = " + reference + ")"));
        assertEquals(expected, ids(conn,
            "b.id IN (WITH small(q) AS (SELECT id FROM big) " + "SELECT q FROM small WHERE q = " + reference + ")"));
      }
      assertEquals(expected,
          ids(conn, "EXISTS (WITH small AS (SELECT id AS q FROM big) SELECT 1 FROM small WHERE q = 99)"));
      assertEquals(expected,
          new HashSet<>(
              queryIds(conn, "WITH small AS (SELECT id FROM big WHERE id < 2) SELECT b.id FROM big b WHERE EXISTS "
                  + "(WITH small AS (SELECT id AS q FROM big) SELECT 1 FROM small WHERE q = b.id)")));
      try (var statement = conn.createStatement()) {
        var failure = assertThrows(java.sql.SQLException.class, () -> statement
            .executeQuery("WITH small AS (SELECT id FROM big), small AS (SELECT id FROM big) SELECT id FROM small"));
        assertTrue(failure.getCause().getMessage().contains("Duplicate CTE"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void derivedCteTakesPrecedenceOverTheInheritedName(boolean predicate, boolean projection) throws Exception {
    try (var conn = DriverManager.getConnection(
        "jdbc:jparq:" + base + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection)) {
      String outer = "WITH a AS (SELECT id FROM small) ";
      String derived = "(WITH a AS (SELECT id FROM big) SELECT id FROM a) t";
      for (String query : List.of(outer + "SELECT COUNT(*) FROM " + derived,
          outer + "SELECT (SELECT COUNT(*) FROM " + derived + ") AS n FROM small LIMIT 1",
          outer + "SELECT COUNT(*) FROM " + derived + " JOIN big b ON b.id = t.id",
          outer + "SELECT COUNT(*) FROM big b JOIN " + derived + " ON b.id = t.id",
          outer + "SELECT COUNT(*) FROM (WITH a(q) AS (SELECT id FROM big) SELECT q FROM a) t WHERE t.q >= 0")) {
        try (var statement = conn.createStatement(); var result = statement.executeQuery(query)) {
          assertTrue(result.next(), query);
          assertEquals(100, result.getInt(1), query);
          assertFalse(result.next(), query);
        }
      }
      assertEquals(new HashSet<>(queryIds(conn, "SELECT id FROM big")),
          new HashSet<>(queryIds(conn, outer + "SELECT t.id FROM " + derived)));
      assertEquals(5, queryIds(conn, outer + "SELECT id FROM a").size());
    }
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void derivedQueriesOverInheritedCtesKeepFiltersAndProjections(boolean predicate, boolean projection)
      throws Exception {
    try (var conn = DriverManager.getConnection(
        "jdbc:jparq:" + base + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection)) {
      String outer = "WITH a AS (SELECT id FROM small) ";
      String filtered = "(SELECT id FROM a WHERE id > 2) t";
      assertEquals(List.of(2), queryIds(conn, outer + "SELECT COUNT(*) FROM " + filtered));
      assertEquals(List.of(2),
          queryIds(conn, outer + "SELECT COUNT(*) FROM " + filtered + " JOIN small s ON s.id = t.id"));
      assertEquals(Set.of(3, 4), new HashSet<>(queryIds(conn, outer + "SELECT t.id FROM " + filtered)));
      // Assert the numeric value; aggregate type consistency is a separate issue.
      assertEquals(List.of(10), queryIds(conn, outer + "SELECT SUM(x) FROM (SELECT id * 1 AS x FROM a) t"));
      assertEquals(List.of(100), queryIds(conn, outer + "SELECT COUNT(*) FROM (SELECT id FROM big) a"));
    }
  }

  /**
   * Reads SQL output while retaining duplicates from set operations.
   *
   * @param conn
   *          query connection
   * @param sql
   *          complete query
   * @return identifiers in scan order
   * @throws Exception
   *           if execution fails
   */
  private static List<Integer> queryIds(Connection conn, String sql) throws Exception {
    try (var stmt = conn.createStatement(); var result = stmt.executeQuery(sql)) {
      List<Integer> values = new ArrayList<>();
      while (result.next()) {
        values.add(result.getInt(1));
      }
      return values;
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
    return new HashSet<>(queryIds(conn, "SELECT b.id FROM big b WHERE " + predicate));
  }
}
