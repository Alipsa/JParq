package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for alias operations. */
public class SubQueryTest {

  static JParqSql jparqSql;
  static Path mtcarsFilePath;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = WhereTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be on the test classpath (src/test/resources)");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();
    mtcarsFilePath = mtcarsPath;

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void inClauseSubqueryFiltersRows() {
    jparqSql.query("SELECT model FROM mtcars where mpg in (select distinct mpg from mtcars)", rs -> {
      List<String> seen = new ArrayList<>();
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(1, md.getColumnCount(), "Expected 1 column");

        int rows = 0;

        while (rs.next()) {
          String model = rs.getString("model");
          seen.add(model);
          rows++;
        }

        assertEquals(32, rows, "Expected 32 rows");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void scalarSubqueryInSelectClause() {
    jparqSql.query("SELECT model, (SELECT COUNT(*) FROM mtcars) AS total_cars FROM mtcars", rs -> {
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Expected 2 columns");

        int rows = 0;
        Long observedTotal = null;
        while (rs.next()) {
          long total = rs.getLong("total_cars");
          if (observedTotal == null) {
            observedTotal = total;
          } else {
            assertEquals(observedTotal, total, "Scalar subquery should yield a constant value");
          }
          rows++;
        }
        assertEquals(32, rows, "Expected 32 rows");
        assertEquals(32L, observedTotal.longValue(), "Expected scalar subquery to return 32");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void subqueryInFromClauseActsAsDerivedTable() {
    jparqSql.query("SELECT * FROM (SELECT model, cyl FROM mtcars WHERE cyl = 4) AS fours", rs -> {
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Derived table should expose projected columns");

        int rows = 0;
        while (rs.next()) {
          assertEquals(4, rs.getInt("cyl"));
          rows++;
        }
        assertEquals(11, rows, "Expected 11 rows for 4-cylinder cars");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void existsSubqueryEvaluatesPresence() {
    jparqSql.query("SELECT model FROM mtcars WHERE NOT EXISTS (SELECT 1 FROM mtcars WHERE cyl = 4)", rs -> {
      try {
        assertTrue(!rs.next(), "NOT EXISTS should eliminate all rows when subquery returns data");
      } catch (SQLException e) {
        fail(e);
      }
    });

    jparqSql.query("SELECT model FROM mtcars WHERE EXISTS (SELECT 1 FROM mtcars WHERE cyl = 4)", rs -> {
      try {
        int rows = 0;
        while (rs.next()) {
          rows++;
        }
        assertEquals(32, rows, "EXISTS should return all rows when subquery has data");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  @Test
  void correlatedExistsFiltersRows() throws IOException {
    List<Car> cars = loadCars();
    List<String> expected = cars.stream()
        .filter(
            car -> cars.stream().anyMatch(other -> other != car && other.cyl() == car.cyl() && other.hp() > car.hp()))
        .map(Car::model).sorted().collect(Collectors.toList());

    jparqSql.query("SELECT model FROM mtcars m WHERE EXISTS (SELECT 1 FROM mtcars sub "
        + "WHERE sub.cyl = m.cyl AND sub.hp > m.hp)", rs -> {
          try {
            List<String> actual = new ArrayList<>();
            while (rs.next()) {
              actual.add(rs.getString("model"));
            }
            actual.sort(String::compareTo);
            assertEquals(expected, actual,
                "Correlated EXISTS should include cars that have a peer with the same cylinder count "
                    + "and higher horsepower");
          } catch (SQLException e) {
            fail(e);
          }
        });
  }

  @Test
  void correlatedScalarSubqueryUsesProjectionAlias() {
    Map<Integer, Long> cylinderCounts = new LinkedHashMap<>();

    jparqSql.query("SELECT cyl, COUNT(*) AS cnt FROM mtcars GROUP BY cyl", rs -> {
      try {
        while (rs.next()) {
          cylinderCounts.put(rs.getInt("cyl"), rs.getLong("cnt"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertFalse(cylinderCounts.isEmpty(), "Baseline cylinder counts should be available for correlation validation");

    Set<Integer> seenGroups = new HashSet<>();

    jparqSql.query("SELECT cyl AS grp, "
        + "(SELECT COUNT(*) FROM mtcars m2 WHERE m2.cyl = grp) AS correlated_cnt "
        + "FROM mtcars mc ORDER BY grp, model", rs -> {
          try {
            int rows = 0;
            while (rs.next()) {
              rows++;
              int group = rs.getInt("grp");
              long correlatedCount = rs.getLong("correlated_cnt");
              Long expected = cylinderCounts.get(group);
              assertNotNull(expected, "Unexpected cylinder value returned: " + group);
              assertEquals(expected.longValue(), correlatedCount,
                  "Correlation context should expose projection aliases to subqueries");
              seenGroups.add(group);
            }
            assertFalse(seenGroups.isEmpty(), "Expected correlated counts for at least one group");
            assertEquals(cylinderCounts.size(), seenGroups.size(),
                "Expected correlated counts for each distinct cylinder");
          } catch (SQLException e) {
            fail(e);
          }
        });
  }

  @Test
  void havingClauseSupportsSubqueries() {
    jparqSql.query("SELECT COUNT(*) AS total FROM mtcars HAVING COUNT(*) >= (SELECT COUNT(*) FROM mtcars)", rs -> {
      try {
        assertTrue(rs.next(), "HAVING condition should pass when comparison evaluates to true");
        assertEquals(32L, rs.getLong("total"));
        assertFalse(rs.next(), "Only a single aggregate row should be returned");
      } catch (SQLException e) {
        fail(e);
      }
    });

    jparqSql.query("SELECT COUNT(*) AS total FROM mtcars HAVING COUNT(*) > (SELECT COUNT(*) FROM mtcars)", rs -> {
      try {
        assertFalse(rs.next(), "HAVING condition should filter out the aggregate row when false");
      } catch (SQLException e) {
        fail(e);
      }
    });
  }

  /**
   * Read all car rows from the Parquet test dataset.
   *
   * @return the list of {@link Car} records loaded from {@code mtcars.parquet}
   * @throws IOException
   *           if the Parquet file cannot be read
   */
  private static List<Car> loadCars() throws IOException {
    List<Car> cars = new ArrayList<>();
    Configuration conf = new Configuration(false);
    org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(mtcarsFilePath.toUri());
    try (ParquetReader<GenericRecord> reader = AvroParquetReader
        .<GenericRecord>builder(HadoopInputFile.fromPath(filePath, conf)).withConf(conf).build()) {
      GenericRecord record = reader.read();
      while (record != null) {
        String model = record.get("model").toString();
        int cyl = ((Number) record.get("cyl")).intValue();
        int hp = ((Number) record.get("hp")).intValue();
        cars.add(new Car(model, cyl, hp));
        record = reader.read();
      }
    }
    return cars;
  }

  @Test
  void loadCarsReadsAllRows() throws IOException {
    List<Car> cars = loadCars();
    assertEquals(32, cars.size(), "Expected all cars to be loaded from the Parquet dataset");
  }

  /**
   * Simple value object representing a car entry from the {@code mtcars} dataset.
   */
  private record Car(String model, int cyl, int hp) {
  }

}
