package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/** Tests for LIKE and NOT LIKE operations. */
public class LikeTest {

  static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = WhereTest.class.getResource("/mtcars.parquet");
    Assertions.assertNotNull(
        mtcarsUrl, "mtcars.parquet must be on the test classpath (src/test/resources)");

    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();

    jparqSql = new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
  }

  @Test
  void testLike() {
    jparqSql.query(
        "SELECT model, mpg, cyl FROM mtcars where model like 'Toyota%'",
        rs -> {
          try {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(3, md.getColumnCount(), "Expected 3 columns");

            int rows = 0;
            while (rs.next()) {
              String first = rs.getString(1);
              // System.out.println(rows + ". " + first + " " + rs.getDouble(2) + " " +
              // rs.getInt(3));
              rows++;
            }
            assertEquals(2, rows, "Expected 2 rows, got " + rows);
          } catch (SQLException e) {
            // Convert SQLExceptions in the lambda to RuntimeExceptions
            fail(e);
          }
        });
  }

  @Test
  void testNotLike() {
    jparqSql.query(
        "SELECT model, mpg, cyl FROM mtcars where model not like 'Toyota%'",
        rs -> {
          try {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(3, md.getColumnCount(), "Expected 3 columns");

            int rows = 0;
            while (rs.next()) {
              String first = rs.getString(1);
              // System.out.println(rows + ". " + first + " " + rs.getDouble(2) + " " +
              // rs.getInt(3));
              rows++;
            }
            assertEquals(30, rows, "Expected 30 rows, got " + rows);
          } catch (SQLException e) {
            // Convert SQLExceptions in the lambda to RuntimeExceptions
            fail(e);
          }
        });
  }
}
