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

/** Tests for WHERE clause operations. */
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

  @Test
  void testWhereIn() {
    List<String> models = new ArrayList<>();
    jparqSql.query(
        "SELECT * FROM mtcars where cyl in (6, 8)",
        rs -> {
          try {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(12, md.getColumnCount(), "Expected 12 columns");

            int rows = 0;
            while (rs.next()) {
              String first = rs.getString(1);
              models.add(first + " " + rs.getInt("cyl"));
              rows++;
            }
            assertEquals(
                21,
                rows,
                "Expected 21 rows, got " + rows + " models: " + String.join(", ", models));
          } catch (SQLException e) {
            System.err.println(String.join("\n", models));
            fail(e);
          }
        });
  }

  @Test
  void testWhereNot() {
    List<String> models = new ArrayList<>();
    jparqSql.query(
        "SELECT * FROM mtcars where not (cyl = 4)",
        rs -> {
          try {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(12, md.getColumnCount(), "Expected 12 columns");

            int rows = 0;
            while (rs.next()) {
              String first = rs.getString(1);
              models.add(first + " " + rs.getInt("cyl"));
              rows++;
            }
            assertEquals(
                21,
                rows,
                "Expected 21 rows, got " + rows + " models: " + String.join(", ", models));
          } catch (SQLException e) {
            System.err.println(String.join("\n", models));
            fail(e);
          }
        });

    jparqSql.query(
        "SELECT * FROM mtcars where not cyl = 4",
        rs -> {
          try {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(12, md.getColumnCount(), "Expected 12 columns");

            int rows = 0;
            while (rs.next()) {
              String first = rs.getString(1);
              models.add(first + " " + rs.getInt("cyl"));
              rows++;
            }
            assertEquals(
                21,
                rows,
                "Expected 21 rows, got " + rows + " models: " + String.join(", ", models));
          } catch (SQLException e) {
            System.err.println(String.join("\n", models));
            fail(e);
          }
        });
  }

  @Test
  void testWhereIsNotNull() {
    List<String> models = new ArrayList<>();
    jparqSql.query(
        "SELECT * FROM mtcars where model is not null",
        rs -> {
          try {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(12, md.getColumnCount(), "Expected 12 columns");

            int rows = 0;
            while (rs.next()) {
              String first = rs.getString(1);
              models.add(first);
              rows++;
            }
            assertEquals(
                32,
                rows,
                "Expected 32 rows, got " + rows + " models: " + String.join(", ", models));
          } catch (SQLException e) {
            System.err.println(String.join("\n", models));
            fail(e);
          }
        });
  }

  @Test
  void testWhereBetween() {
    jparqSql.query(
        "SELECT model, mpg FROM mtcars WHERE mpg BETWEEN 18 AND 21",
        rs -> {
          List<String> models = new ArrayList<>();
          try {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(2, md.getColumnCount(), "Expected 2 columns");

            int rows = 0;

            while (rs.next()) {
              String first = rs.getString(1);
              models.add(first + " " + rs.getDouble(2));
              rows++;
            }
            assertEquals(
                7, rows, "Expected 7 rows, got " + rows + " models: " + String.join(", ", models));
          } catch (SQLException e) {
            System.err.println(String.join("\n", models));
            fail(e);
          }
        });
  }
}
