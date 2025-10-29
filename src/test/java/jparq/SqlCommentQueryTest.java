package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Integration style tests that ensure SQL comments are handled end-to-end. */
class SqlCommentQueryTest {
  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = SqlCommentQueryTest.class.getResource("/mtcars.parquet");
    Assertions.assertNotNull(mtcarsUrl, "mtcars.parquet must be available on the classpath");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void queryWithLineAndBlockCommentsExecutes() {
    String sql = """
        /* ensure comments preceding the statement are ignored */
        SELECT model, cyl -- project specific columns
        FROM mtcars
        WHERE cyl = 6 -- restrict to six cylinders
          /* multiline
             comment */
        ORDER BY model
        """;

    List<String> models = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount(), "Only projected columns should be present");
        while (rs.next()) {
          models.add(rs.getString("model"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertEquals(7, models.size(), "There should be seven models with six cylinders");
  }
}
