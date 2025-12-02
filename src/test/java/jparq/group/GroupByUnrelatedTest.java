
package jparq.group;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

public class GroupByUnrelatedTest {

  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = GroupByUnrelatedTest.class.getResource("/mtcars.parquet");
    assertNotNull(mtcarsUrl, "mtcars.parquet must be available on the classpath");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void testGroupByColumnNotInSelectList() {
    List<Long> counts = new ArrayList<>();
    jparqSql.query("SELECT COUNT(*) AS total FROM mtcars GROUP BY cyl", rs -> {
      try {
        while (rs.next()) {
          counts.add(rs.getLong("total"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });
    // There are 3 distinct values for cyl (4, 6, 8)
    assertEquals(3, counts.size(), "Should return one row for each group, even if the group key is not projected");
  }
}
