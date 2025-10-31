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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Integration tests verifying OFFSET semantics for simple and derived queries.
 */
class SqlParserOffsetTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = SqlParserOffsetTest.class.getResource("/mtcars.parquet");
    if (mtcarsUrl == null) {
      throw new IllegalStateException("mtcars.parquet must be available on the classpath");
    }
    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void offsetAppliesAfterOrdering() {
    List<String> orderedModels = new ArrayList<>();
    jparqSql.query("SELECT model FROM mtcars ORDER BY mpg DESC", rs -> {
      try {
        while (rs.next()) {
          orderedModels.add(rs.getString("model"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    int offset = 5;
    int limit = 3;
    int start = Math.min(offset, orderedModels.size());
    int end = Math.min(start + limit, orderedModels.size());
    List<String> expected = orderedModels.subList(start, end);

    List<String> actual = new ArrayList<>();
    jparqSql.query("""
        SELECT model
        FROM mtcars
        ORDER BY mpg DESC
        OFFSET 5
        LIMIT 3
        """, rs -> {
      try {
        while (rs.next()) {
          actual.add(rs.getString("model"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertEquals(expected, actual, "OFFSET should skip rows after ORDER BY has been applied");
  }

  @Test
  void derivedTableOffsetIsAppliedBeforeOuterOrdering() {
    List<CarRow> orderedRows = new ArrayList<>();
    jparqSql.query("SELECT model, mpg FROM mtcars ORDER BY mpg DESC", rs -> {
      try {
        while (rs.next()) {
          orderedRows.add(new CarRow(rs.getString("model"), rs.getDouble("mpg")));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    int innerOffset = 5;
    int innerLimit = 10;
    int innerStart = Math.min(innerOffset, orderedRows.size());
    int innerEnd = Math.min(innerStart + innerLimit, orderedRows.size());
    List<CarRow> innerSlice = orderedRows.subList(innerStart, innerEnd);

    List<String> expected = innerSlice.stream()
        .sorted(Comparator.comparingDouble(CarRow::mpg).thenComparing(CarRow::model)).limit(3).map(CarRow::model)
        .toList();

    List<String> actual = new ArrayList<>();
    jparqSql.query("""
        SELECT model
        FROM (
          SELECT model, mpg FROM mtcars ORDER BY mpg DESC OFFSET 5 LIMIT 10
        ) ranked
        ORDER BY mpg ASC
        LIMIT 3
        """, rs -> {
      try {
        while (rs.next()) {
          actual.add(rs.getString("model"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertEquals(expected, actual,
        "Outer ORDER BY should operate on the rows remaining after the inner OFFSET and LIMIT");
  }

  private record CarRow(String model, double mpg) {
  }
}
