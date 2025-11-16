package jparq.sampling;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for TABLESAMPLE semantics. */
public class TableSampleTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = TableSampleTest.class.getResource("/mtcars.parquet");
    Assertions.assertNotNull(mtcarsUrl, "mtcars.parquet must be on the test classpath (src/test/resources)");
    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void testSystemPercentSampleWithRepeatableSeed() {
    String sql = """
        SELECT model FROM mtcars
        TABLESAMPLE SYSTEM (50 PERCENT)
        REPEATABLE (4242)
        """;
    List<String> first = fetchModels(sql);
    List<String> second = fetchModels(sql);
    Assertions.assertEquals(16, first.size(), "Expected deterministic 50% sample");
    Assertions.assertEquals(first, second, "REPEATABLE seed should produce identical samples");
  }

  @Test
  void testBernoulliRepeatableIsDeterministic() {
    String sql = """
        SELECT model FROM mtcars
        TABLESAMPLE BERNOULLI (30 PERCENT)
        REPEATABLE (99)
        ORDER BY model
        """;
    List<String> first = fetchModels(sql);
    List<String> second = fetchModels(sql);
    Assertions.assertEquals(first, second, "REPEATABLE seed should produce identical samples");
  }

  @Test
  void testNonRepeatableSamplesDiffer() {
    String sql = """
        SELECT model FROM mtcars
        TABLESAMPLE BERNOULLI (30 PERCENT)
        """;
    Set<List<String>> seen = new HashSet<>();
    int attempts = 0;
    while (attempts < 3) {
      List<String> sample = fetchModels(sql);
      if (!seen.add(sample)) {
        attempts++;
        continue;
      }
      if (seen.size() > 1) {
        return;
      }
      attempts++;
    }
    Assertions.fail("Expected different samples across executions without REPEATABLE");
  }

  @Test
  void testFixedRowSampleUsesReservoirSampling() {
    String sql = """
        SELECT model FROM mtcars
        TABLESAMPLE SYSTEM (5 ROWS)
        REPEATABLE (777)
        ORDER BY model
        """;
    List<String> first = fetchModels(sql);
    List<String> second = fetchModels(sql);
    Assertions.assertEquals(5, first.size(), "ROW sampling should return a fixed number of rows");
    Assertions.assertEquals(first, second, "REPEATABLE seed should stabilize ROW sampling");
  }

  private static List<String> fetchModels(String sql) {
    List<String> models = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          models.add(rs.getString("model"));
        }
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
    return models;
  }
}
