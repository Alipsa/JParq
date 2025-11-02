package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests covering UNION and UNION ALL query semantics. */
public class UnionTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = UnionTest.class.getResource("/mtcars.parquet");
    if (mtcarsUrl == null) {
      throw new IllegalStateException("mtcars.parquet must be available on the test classpath");
    }
    Path dataPath = Paths.get(mtcarsUrl.toURI());
    Path dir = dataPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void unionAllIncludesDuplicates() {
    String sql = """
        SELECT cyl FROM mtcars WHERE cyl = 4
        UNION ALL
        SELECT cyl FROM mtcars WHERE cyl = 4
        """;
    jparqSql.query(sql, rs -> {
      try {
        int rows = 0;
        while (rs.next()) {
          rows++;
        }
        assertEquals(22, rows, "UNION ALL should preserve duplicates");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  void unionRemovesDuplicates() {
    String sql = """
        SELECT cyl FROM mtcars WHERE cyl IN (4, 6)
        UNION
        SELECT cyl FROM mtcars WHERE cyl IN (6, 8)
        """;
    jparqSql.query(sql, rs -> {
      try {
        Set<Integer> values = new LinkedHashSet<>();
        while (rs.next()) {
          values.add(rs.getInt(1));
        }
        assertEquals(3, values.size(), "UNION should eliminate duplicates");
        assertTrue(values.containsAll(Set.of(4, 6, 8)), "UNION result must include all unique cylinder counts");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  void unionMixedOperationsFollowSqlSemantics() {
    String sql = """
        SELECT cyl FROM mtcars WHERE cyl = 4
        UNION ALL
        SELECT cyl FROM mtcars WHERE cyl = 6
        UNION
        SELECT cyl FROM mtcars WHERE cyl = 4
        """;
    jparqSql.query(sql, rs -> {
      try {
        int rows = 0;
        while (rs.next()) {
          rows++;
        }
        assertEquals(18, rows, "UNION should retain UNION ALL duplicates but suppress later duplicates");
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  void unionRequiresCompatibleColumns() {
    String sql = """
        SELECT model, cyl FROM mtcars
        UNION
        SELECT cyl FROM mtcars
        """;
    RuntimeException ex = assertThrows(RuntimeException.class, () -> jparqSql.query(sql, rs -> {
    }));
    assertTrue(ex.getCause() instanceof SQLException, "Expected SQLException when UNION columns are incompatible");
  }
}
