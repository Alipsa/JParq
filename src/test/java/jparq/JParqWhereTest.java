package jparq;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JParqWhereTest {
  static String jdbcUrl;
  @BeforeAll
  static void setup() throws URISyntaxException {
    URL mtcarsUrl = JParqWhereTest.class.getResource("/mtcars.parquet");
    Assertions.assertNotNull(mtcarsUrl, "mtcars.parquet must be on the test classpath (src/test/resources)");


    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dir = mtcarsPath.getParent();


    jdbcUrl = "jdbc:jparq:" + dir.toAbsolutePath();
  }

  @Test
  void testSelectMtcarsWhereEq() throws SQLException, ClassNotFoundException, URISyntaxException {

    try (Connection conn = DriverManager.getConnection(jdbcUrl);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT model, mpg, cyl FROM mtcars where cyl = 6")) {


      ResultSetMetaData md = rs.getMetaData();
      Assertions.assertEquals(3, md.getColumnCount(), "Expected 3 columns");


      int rows = 0;
      while (rs.next()) {
        String first = rs.getString(1);
        //System.out.println(rows + ". " + first + " " + rs.getDouble(2) + " " + rs.getInt(3));
        rows++;
      }
      Assertions.assertEquals(7, rows, "Expected 7 rows, got " + rows);
    }
  }

  @Test
  void testSelectMtcarsWhereGtEq() throws SQLException, ClassNotFoundException, URISyntaxException {

    try (Connection conn = DriverManager.getConnection(jdbcUrl);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT model, mpg, cyl FROM mtcars where cyl >= 6")) {


      ResultSetMetaData md = rs.getMetaData();
      Assertions.assertEquals(3, md.getColumnCount(), "Expected 3 columns");


      int rows = 0;
      while (rs.next()) {
        String first = rs.getString(1);
        //System.out.println(rows + ". " + first + " " + rs.getDouble(2) + " " + rs.getInt(3));
        rows++;
      }
      Assertions.assertEquals(21, rows, "Expected 21 rows, got " + rows);
    }
  }

  @Test
  void testSelectMtcarsWhereLtEq() throws SQLException, ClassNotFoundException, URISyntaxException {

    try (Connection conn = DriverManager.getConnection(jdbcUrl);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT model, mpg, cyl FROM mtcars where cyl <= 6")) {


      ResultSetMetaData md = rs.getMetaData();
      Assertions.assertEquals(3, md.getColumnCount(), "Expected 3 columns");


      int rows = 0;
      while (rs.next()) {
        String first = rs.getString(1);
        //System.out.println(rows + ". " + first + " " + rs.getDouble(2) + " " + rs.getInt(3));
        rows++;
      }
      Assertions.assertEquals(18, rows, "Expected 18 rows, got " + rows);
    }
  }

  @Test
  void testSelectMtcarsWhereLt() throws SQLException, ClassNotFoundException, URISyntaxException {

    try (Connection conn = DriverManager.getConnection(jdbcUrl);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM mtcars where cyl < 6")) {


      ResultSetMetaData md = rs.getMetaData();
      Assertions.assertEquals(12, md.getColumnCount(), "Expected 12 columns");


      int rows = 0;
      while (rs.next()) {
        String first = rs.getString(1);
        //System.out.println(rows + ". " + first + " " + rs.getDouble(2) + " " + rs.getInt(3));
        rows++;
      }
      Assertions.assertEquals(11, rows, "Expected 11 rows, got " + rows);
    }
  }

  @Test
  void testSelectMtcarsWhereGt() throws SQLException, ClassNotFoundException, URISyntaxException {

    try (Connection conn = DriverManager.getConnection(jdbcUrl);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM mtcars where cyl > 6")) {


      ResultSetMetaData md = rs.getMetaData();
      Assertions.assertEquals(12, md.getColumnCount(), "Expected 12 columns");


      int rows = 0;
      while (rs.next()) {
        String first = rs.getString(1);
        //System.out.println(rows + ". " + first + " " + rs.getDouble(2) + " " + rs.getInt(3));
        rows++;
      }
      Assertions.assertEquals(14, rows, "Expected 14 rows, got " + rows);
    }
  }
}
