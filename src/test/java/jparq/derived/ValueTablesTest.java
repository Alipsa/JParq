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

  @BeforeAll
  static void setUp() throws URISyntaxException {
    URL mtcarsUrl = ValueTablesTest.class.getResource("/mtcars.parquet");
    Assertions.assertNotNull(mtcarsUrl, "mtcars.parquet must be available on the classpath");
    Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
    Path dataDirectory = mtcarsPath.getParent();
    jparqSql = new JParqSql("jdbc:jparq:" + dataDirectory.toAbsolutePath());
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
}
