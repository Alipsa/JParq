package jparq.derived;

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

/**
 * Integration tests verifying SQL value table constructors.
 */
class ValueTablesTest {

  private static JParqSql jparqSql;
  private static String jdbcUrl;

  @BeforeAll
  static void setUp() throws URISyntaxException {
    URL mtcarsUrl = ValueTablesTest.class.getResource("/mtcars.parquet");
    Assertions.assertNotNull(mtcarsUrl, "mtcars.parquet must be available on the classpath");
    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dataDirectory = mtcarsPath.getParent();
    jdbcUrl = "jdbc:jparq:" + dataDirectory.toAbsolutePath();
    jparqSql = new JParqSql(jdbcUrl);
  }

  @Test
  void valuesStatementProducesRows() {
    String sql = "VALUES (1, 'A'), (2, 'B'), (3, 'C')";
    jparqSql.query(sql, rs -> {
      try {
        ResultSetMetaData meta = rs.getMetaData();
        Assertions.assertEquals(2, meta.getColumnCount(), "VALUES statement should expose two columns");
        Assertions.assertEquals("column1", meta.getColumnLabel(1));
        Assertions.assertEquals("column2", meta.getColumnLabel(2));
        List<String> values = new ArrayList<>();
        while (rs.next()) {
          values.add(rs.getInt(1) + ":" + rs.getString(2));
        }
        Assertions.assertEquals(List.of("1:A", "2:B", "3:C"), values);
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
  }

  @Test
  void valuesDerivedTableSupportsAliases() {
    String sql = """
        SELECT t.id, t.name
        FROM (VALUES (1, 'Alpha'), (2, 'Beta')) AS t(id, name)
        ORDER BY t.id
        """;
    jparqSql.query(sql, rs -> {
      try {
        ResultSetMetaData meta = rs.getMetaData();
        Assertions.assertEquals("id", meta.getColumnLabel(1));
        Assertions.assertEquals("name", meta.getColumnLabel(2));
        List<String> rows = new ArrayList<>();
        while (rs.next()) {
          rows.add(rs.getInt("id") + ":" + rs.getString("name"));
        }
        Assertions.assertEquals(List.of("1:Alpha", "2:Beta"), rows);
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
  }

  @Test
  void valuesJoinWithExistingTable() {
    String sql = """
        SELECT m.model, lookup.category
        FROM mtcars m
        JOIN (VALUES (4, 'Economy'), (6, 'Sport'), (8, 'Performance')) AS lookup(cyl, category)
          ON m.cyl = lookup.cyl
        WHERE m.model IN ('Mazda RX4', 'Datsun 710', 'Camaro Z28')
        ORDER BY m.model
        """;
    jparqSql.query(sql, rs -> {
      try {
        List<String> results = new ArrayList<>();
        while (rs.next()) {
          results.add(rs.getString("model") + ":" + rs.getString("category"));
        }
        Assertions.assertEquals(List.of("Camaro Z28:Performance", "Datsun 710:Economy", "Mazda RX4:Sport"), results);
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
  }

  @Test
  void valuesRealColumnReportsFloat() {
    String sql = "VALUES (CAST(1.0 AS REAL)), (CAST(2.5 AS REAL))";
    jparqSql.query(sql, rs -> {
      try {
        ResultSetMetaData meta = rs.getMetaData();
        Assertions.assertEquals(Float.class.getName(), meta.getColumnClassName(1),
            "REAL literals should report Float column class");
        Assertions.assertTrue(rs.next(), "First VALUES row should be available");
        Assertions.assertEquals(1.0f, rs.getFloat(1));
        Assertions.assertTrue(rs.next(), "Second VALUES row should be available");
        Assertions.assertEquals(2.5f, rs.getFloat(1));
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
  }

  @Test
  void valuesIntAndRealColumnReportsDouble() {
    String sql = "VALUES (1), (CAST(2.5 AS REAL))";
    jparqSql.query(sql, rs -> {
      try {
        ResultSetMetaData meta = rs.getMetaData();
        Assertions.assertEquals(Double.class.getName(), meta.getColumnClassName(1),
            "INT and REAL values should widen to Double column class");
        Assertions.assertTrue(rs.next(), "First VALUES row should be available");
        Assertions.assertEquals(1.0d, rs.getDouble(1));
        Assertions.assertTrue(rs.next(), "Second VALUES row should be available");
        Assertions.assertEquals(2.5d, rs.getDouble(1));
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
  }

  @Test
  void valuesDateAndTimeColumnFallbacksToString() {
    String sql = "VALUES (DATE '2023-01-01'), (TIME '12:34:56')";
    jparqSql.query(sql, rs -> {
      try {
        ResultSetMetaData meta = rs.getMetaData();
        Assertions.assertEquals(String.class.getName(), meta.getColumnClassName(1),
            "Mismatched DATE and TIME values should be exposed as Strings");
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals("2023-01-01", rs.getString(1));
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals("12:34:56", rs.getString(1));
      } catch (SQLException e) {
        Assertions.fail(e);
      }
    });
  }
}
