package jparq.usage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

public class AcmeTest {
  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = AcmeTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  /**
   * Expected result: id first_name last_name salary 2 Karin Pettersson 180000 4
   * Arne Larsson 195000 5 Sixten Svensson 230000 3 Tage Lundström 140000 1 Per
   * Andersson 165000
   *
   * <p>
   * Historically some environments surfaced Parquet string columns as binary
   * data, producing metadata such as {@code e__id} and {@code [B@...}
   * representations instead of human readable values.
   */
  @Test
  void testClassicSubQuery() {
    String sql = """
        select e.*, s.salary from employees e join salary s on e.id = s.employee
        WHERE
            s.change_date = (
                SELECT
                    MAX(s2.change_date)
                FROM
                    salary s2
                WHERE
                    s2.employee = s.employee
            );
        """;
    List<Integer> ids = new ArrayList<>();
    Map<String, Double> salaries = new LinkedHashMap<>();
    jparqSql.query(sql, rs -> {
      try {
        ResultSetMetaData metaData = rs.getMetaData();
        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        List<String> columnClassNames = new ArrayList<>();
        Map<String, String> headers = new LinkedHashMap<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          String name = metaData.getColumnName(i);
          String type = metaData.getColumnTypeName(i);
          columnNames.add(name);
          columnTypes.add(type);
          headers.put(name, type);
          columnClassNames.add(metaData.getColumnClassName(i));
        }
        assertEquals(List.of("id", "first_name", "last_name", "salary"), columnNames,
            "Column names should match the underlying table schema");
        assertEquals(List.of("INTEGER", "VARCHAR", "VARCHAR", "DOUBLE"), columnTypes,
            "Column JDBC types should match the expected Avro mappings");
        assertEquals(List.of("java.lang.Integer", "java.lang.String", "java.lang.String", "java.lang.Double"),
            columnClassNames, "Column class names should match the expected Java types for the JDBC types");
        while (rs.next()) {
          Integer id = rs.getInt("id");
          ids.add(id);
          Object firstName = rs.getObject("first_name");
          assertEquals(String.class, firstName.getClass(),
              "If the underlying class is String, getObject should return String");
          String lastName = rs.getString("last_name");
          Double salary = rs.getDouble("salary");
          salaries.put(firstName + " " + lastName, salary);
        }
        assertEquals(5, ids.size(), "Expected 5 distinct employee ids");
        assertEquals(180000, salaries.get("Karin Pettersson"));
        assertEquals(195000, salaries.get("Arne Larsson"));
        assertEquals(230000, salaries.get("Sixten Svensson"));
        assertEquals(140000, salaries.get("Tage Lundström"));
        assertEquals(165000, salaries.get("Per Andersson"));
      } catch (Exception e) {
        Assertions.fail(e);
      }
    });
  }
}
