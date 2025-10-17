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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

public class WhereTest {
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
  void testWhereEq() {

    jparqSql.query(
        "SELECT model, mpg, cyl FROM mtcars where cyl = 6",
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
            assertEquals(7, rows, "Expected 7 rows, got " + rows);
          } catch (SQLException e) {
            // Convert SQLExceptions in the lambda to RuntimeExceptions
            fail(e);
          }
        });
  }

  @Test
  void testWhereGtEq() {

    jparqSql.query(
        "SELECT model, mpg, cyl FROM mtcars where cyl >= 6",
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
            assertEquals(21, rows, "Expected 21 rows, got " + rows);
          } catch (SQLException e) {
            fail(e);
          }
        });
  }

  @Test
  void testWhereLtEq() {

    jparqSql.query(
        "SELECT model, mpg, cyl FROM mtcars where cyl <= 6",
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
            assertEquals(18, rows, "Expected 18 rows, got " + rows);
          } catch (SQLException e) {
            fail(e);
          }
        });
  }

  @Test
  void testWhereLt() {

    jparqSql.query(
        "SELECT * FROM mtcars where cyl < 6",
        rs -> {
          try {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(12, md.getColumnCount(), "Expected 12 columns");

            int rows = 0;
            while (rs.next()) {
              String first = rs.getString(1);
              // System.out.println(rows + ". " + first + " " + rs.getDouble(2) + " " +
              // rs.getInt(3));
              rows++;
            }
            assertEquals(11, rows, "Expected 11 rows, got " + rows);
          } catch (SQLException e) {
            fail(e);
          }
        });
  }

  @Test
  void testWhereGt() {

    jparqSql.query(
        "SELECT * FROM mtcars where cyl > 6",
        rs -> {
          try {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(12, md.getColumnCount(), "Expected 12 columns");

            int rows = 0;
            while (rs.next()) {
              String first = rs.getString(1);
              // System.out.println(rows + ". " + first + " " + rs.getDouble(2) + " " +
              // rs.getInt(3));
              rows++;
            }
            assertEquals(14, rows, "Expected 14 rows, got " + rows);
          } catch (SQLException e) {
            fail(e);
          }
        });
  }

  // Not yet implemented
  @Disabled
  @Test
  void testWhereIn() {
    jparqSql.query(
        "SELECT * FROM mtcars where cyl in (6, 8)",
        rs -> {
          try {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(12, md.getColumnCount(), "Expected 12 columns");

            int rows = 0;
            while (rs.next()) {
              String first = rs.getString(1);
              // System.out.println(rows + ". " + first + " " + rs.getDouble(2) + " " +
              // rs.getInt(3));
              rows++;
            }
            assertEquals(21, rows, "Expected 21 rows, got " + rows);
          } catch (SQLException e) {
            fail(e);
          }
        });
  }
}
