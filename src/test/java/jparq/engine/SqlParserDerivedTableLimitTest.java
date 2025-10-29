package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Integration tests ensuring derived tables preserve LIMIT semantics. */
class SqlParserDerivedTableLimitTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = SqlParserDerivedTableLimitTest.class.getResource("/mtcars.parquet");
    if (mtcarsUrl == null) {
      throw new IllegalStateException("mtcars.parquet must be available on the classpath");
    }
    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void derivedTableLimitIsAppliedBeforeOuterOrder() {
    AtomicInteger order = new AtomicInteger();
    List<CarRow> topTenByMpg = new ArrayList<>();
    jparqSql.query("SELECT model, cyl, mpg FROM mtcars ORDER BY mpg DESC LIMIT 10", rs -> {
      try {
        while (rs.next()) {
          topTenByMpg.add(new CarRow(rs.getString("model"), rs.getInt("cyl"), order.getAndIncrement()));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    topTenByMpg.sort(Comparator.comparingInt(CarRow::cyl).thenComparingInt(CarRow::orderIndex));

    List<String> expectedModels = topTenByMpg.stream().limit(5).map(CarRow::model).toList();

    List<String> actualModels = new ArrayList<>();
    jparqSql.query("""
        SELECT model
        FROM (
          SELECT model, cyl, mpg FROM mtcars ORDER BY mpg DESC LIMIT 10
        ) t
        ORDER BY cyl
        LIMIT 5
        """, rs -> {
          try {
            while (rs.next()) {
              actualModels.add(rs.getString("model"));
            }
          } catch (SQLException e) {
            fail(e);
          }
        });

    assertEquals(expectedModels, actualModels,
        "Derived table LIMIT should reduce rows before applying outer ORDER BY");
  }

  @Test
  void derivedTableLimitWithoutInnerOrderStillRestrictsOuterOrdering() {
    AtomicInteger order = new AtomicInteger();
    List<CarRow> firstTen = new ArrayList<>();
    jparqSql.query("SELECT model FROM mtcars LIMIT 10", rs -> {
      try {
        while (rs.next()) {
          firstTen.add(new CarRow(rs.getString("model"), 0, order.getAndIncrement()));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    firstTen.sort(Comparator.comparing(CarRow::model).thenComparingInt(CarRow::orderIndex));

    List<String> expectedModels = firstTen.stream().limit(5).map(CarRow::model).toList();

    List<String> actualModels = new ArrayList<>();
    jparqSql.query("""
        SELECT model
        FROM (
          SELECT model FROM mtcars LIMIT 10
        ) limited
        ORDER BY model
        LIMIT 5
        """, rs -> {
          try {
            while (rs.next()) {
              actualModels.add(rs.getString("model"));
            }
          } catch (SQLException e) {
            fail(e);
          }
        });

    assertEquals(expectedModels, actualModels,
        "Outer ORDER BY should operate on the rows restricted by the inner LIMIT");
  }

  private record CarRow(String model, int cyl, int orderIndex) {
  }
}
