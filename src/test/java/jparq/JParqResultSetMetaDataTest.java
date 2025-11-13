package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqResultSetMetaData;

/**
 * Tests for {@link JParqResultSetMetaData}.
 */
class JParqResultSetMetaDataTest {

  @Test
  void rowNumberFunctionReportsBigIntType() {
    JParqResultSetMetaData metaData = createMetadataWithExpression(schemaWithField(), List.of("row_number"),
        Collections.singletonList(null), List.of(analyticExpression("ROW_NUMBER", null)));

    assertEquals(Types.BIGINT, metaData.getColumnType(1), "ROW_NUMBER should be reported as BIGINT");
  }

  @Test
  void columnClassNameMatchesSchemaType() throws SQLException {
    Schema schema = schemaWithField();
    JParqResultSetMetaData metaData = createMetadataWithExpression(schema, List.of("value"),
        Collections.singletonList("value"), Collections.emptyList());

    assertEquals(Double.class.getName(), metaData.getColumnClassName(1),
        "Double schema fields should map to java.lang.Double");
  }

  @Test
  void rowNumberFunctionClassNameMatchesBigIntType() throws SQLException {
    JParqResultSetMetaData metaData = createMetadataWithExpression(schemaWithField(), List.of("row_number"),
        Collections.singletonList(null), List.of(analyticExpression("ROW_NUMBER", null)));

    assertEquals(Long.class.getName(), metaData.getColumnClassName(1),
        "ROW_NUMBER should be reported as java.lang.Long");
  }

  @Test
  void lagFunctionUsesReferencedColumnType() {
    Schema schema = schemaWithField();
    Column column = new Column();
    column.setColumnName("value");
    AnalyticExpression lagExpression = analyticExpression("LAG", column);
    JParqResultSetMetaData metaData = createMetadataWithExpression(schema, List.of("lag_value"),
        Collections.singletonList(null), List.of(lagExpression));

    assertEquals(Types.DOUBLE, metaData.getColumnType(1), "LAG should report the referenced column type");
  }

  @Test
  void lagFunctionUsesReferencedColumnClassName() throws SQLException {
    Schema schema = schemaWithField();
    Column column = new Column();
    column.setColumnName("value");
    AnalyticExpression lagExpression = analyticExpression("LAG", column);
    JParqResultSetMetaData metaData = createMetadataWithExpression(schema, List.of("lag_value"),
        Collections.singletonList(null), List.of(lagExpression));

    assertEquals(Double.class.getName(), metaData.getColumnClassName(1),
        "LAG should report the referenced column class name");
  }

  @Test
  void leadFunctionUsesReferencedColumnType() {
    Schema schema = schemaWithField();
    Column column = new Column();
    column.setColumnName("value");
    AnalyticExpression leadExpression = analyticExpression("LEAD", column);
    JParqResultSetMetaData metaData = createMetadataWithExpression(schema, List.of("lead_value"),
        Collections.singletonList(null), List.of(leadExpression));

    assertEquals(Types.DOUBLE, metaData.getColumnType(1), "LEAD should report the referenced column type");
  }

  @Test
  void listMetaData() throws URISyntaxException, SQLException {
    URL acmesUrl = JParqResultSetMetaDataTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    String jdbcUrl = "jdbc:jparq:" + acmePath.toAbsolutePath();
    try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
      DatabaseMetaData metaData = conn.getMetaData();
      ResultSet tables = metaData.getTables(null, null, null, null);
      List<String> tableNames = new ArrayList<>();
      while (tables.next()) {
        String table = tables.getString("TABLE_NAME");
        tableNames.add(table);
      }
      tables.close();
      assertTrue(tableNames.contains("employees"));
      assertTrue(tableNames.contains("departments"));
      assertTrue(tableNames.contains("employee_department"));
      assertTrue(tableNames.contains("salary"));

      ResultSet columns = metaData.getColumns(null, null, "employees", null);
      List<String> columnNames = new ArrayList<>();
      while (columns.next()) {
        String column = columns.getString("COLUMN_NAME");
        columnNames.add(column);
      }
      columns.close();
      assertTrue(columnNames.contains("id"));
      assertTrue(columnNames.contains("first_name"));
      assertTrue(columnNames.contains("last_name"));
    }

  }

  /**
   * Create an Avro schema containing a single double field named {@code value}.
   *
   * @return the parsed schema used by the tests
   */
  private Schema schemaWithField() {
    String schemaJson = "{\"type\":\"record\",\"name\":\"Test\",\"fields\":["
        + "{\"name\":\"value\",\"type\":\"double\"}]}";
    return new Schema.Parser().parse(schemaJson);
  }

  /**
   * Construct an analytic expression with the supplied name and optional argument
   * expression.
   *
   * @param name
   *          the function name
   * @param expression
   *          the argument supplied to the analytic function (may be {@code null})
   * @return the configured analytic expression
   */
  private AnalyticExpression analyticExpression(String name, Expression expression) {
    AnalyticExpression analyticExpression = new AnalyticExpression();
    analyticExpression.setName(name);
    analyticExpression.setExpression(expression);
    return analyticExpression;
  }

  /**
   * Create a metadata instance with the supplied schema and expression list.
   *
   * @param schema
   *          the schema backing the result set
   * @param labels
   *          the result labels returned to the caller
   * @param physicalNames
   *          the underlying physical column names (may contain {@code null})
   * @param expressions
   *          the parsed expressions representing the projected columns
   * @return the configured metadata instance
   */
  private JParqResultSetMetaData createMetadataWithExpression(Schema schema, List<String> labels,
      List<String> physicalNames, List<Expression> expressions) {
    List<String> canonical = physicalNames;
    return new JParqResultSetMetaData(schema, labels, physicalNames, canonical, "test", expressions);
  }
}
