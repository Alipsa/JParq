package jparq.meta;

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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import jparq.usage.AcmeTest;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqResultSetMetaData;
import se.alipsa.jparq.JParqSql;

/**
 * Tests for {@link JParqResultSetMetaData}.
 */
class JParqResultSetMetaDataTest {

  static String jdbcUrl;
  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = AcmeTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jdbcUrl = "jdbc:jparq:" + acmePath.toAbsolutePath();
  }

  @Test
  void rowNumberFunctionReportsBigIntType() {
    JParqResultSetMetaData metaData = createMetadataWithExpression(schemaWithField(), List.of("row_number"),
        Collections.singletonList(null), List.of(analyticExpression("ROW_NUMBER", null)));

    assertEquals(Types.BIGINT, metaData.getColumnType(1), "ROW_NUMBER should be reported as BIGINT");
  }

  @Test
  void columnClassNameMatchesSchemaType() {
    Schema schema = schemaWithField();
    JParqResultSetMetaData metaData = createMetadataWithExpression(schema, List.of("value"),
        Collections.singletonList("value"), Collections.emptyList());

    assertEquals(Double.class.getName(), metaData.getColumnClassName(1),
        "Double schema fields should map to java.lang.Double");
  }

  @Test
  void rowNumberFunctionClassNameMatchesBigIntType() {
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
  void lagFunctionUsesReferencedColumnClassName() {
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
  void nullableSchemaFieldsUnwrapUnionForClassName() {
    Schema schema = schemaWithNullableField();
    JParqResultSetMetaData metaData = createMetadataWithExpression(schema, List.of("optional"),
        Collections.singletonList("optional"), Collections.emptyList());

    assertEquals(Integer.class.getName(), metaData.getColumnClassName(1),
        "Nullable union should map to the non-null field type");
  }

  @Test
  void arraySchemaFieldsReportListClass() {
    Schema schema = schemaWithArrayField();
    JParqResultSetMetaData metaData = createMetadataWithExpression(schema, List.of("entries"),
        Collections.singletonList("entries"), Collections.emptyList());

    assertEquals(List.class.getName(), metaData.getColumnClassName(1),
        "Array schema fields should report java.util.List");
  }

  @Test
  void recordSchemaFieldsReportMapClass() {
    Schema schema = schemaWithRecordField();
    JParqResultSetMetaData metaData = createMetadataWithExpression(schema, List.of("child"),
        Collections.singletonList("child"), Collections.emptyList());

    assertEquals(Map.class.getName(), metaData.getColumnClassName(1),
        "Nested record schema fields should report java.util.Map");
  }

  @Test
  void computedArrayExpressionsReportListClass() {
    Schema schema = schemaWithField();
    ArrayConstructor arrayConstructor = new ArrayConstructor();
    JParqResultSetMetaData metaData = createMetadataWithExpression(schema, List.of("computed_array"),
        Collections.singletonList(null), List.of(arrayConstructor));

    assertEquals(List.class.getName(), metaData.getColumnClassName(1),
        "Computed array expressions should report java.util.List");
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
   * Create a schema containing a nullable integer field.
   *
   * @return the schema used for nullable field tests
   */
  private Schema schemaWithNullableField() {
    String schemaJson = "{\"type\":\"record\",\"name\":\"NullableTest\",\"fields\":["
        + "{\"name\":\"optional\",\"type\":[\"null\",\"int\"],\"default\":null}]}";
    return new Schema.Parser().parse(schemaJson);
  }

  /**
   * Create a schema containing an array of strings field.
   *
   * @return the schema used for array field tests
   */
  private Schema schemaWithArrayField() {
    String schemaJson = "{\"type\":\"record\",\"name\":\"ArrayTest\",\"fields\":["
        + "{\"name\":\"entries\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}";
    return new Schema.Parser().parse(schemaJson);
  }

  /**
   * Create a schema containing a nested record field.
   *
   * @return the schema used for nested record tests
   */
  private Schema schemaWithRecordField() {
    String schemaJson = "{\"type\":\"record\",\"name\":\"RecordTest\",\"fields\":["
        + "{\"name\":\"child\",\"type\":{\"type\":\"record\",\"name\":\"Child\","
        + "\"fields\":[{\"name\":\"identifier\",\"type\":\"long\"}]}}]}";
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

  @Test
  void resultSetMetaDataTest() throws SQLException {
    JParqSql jparqSql = new JParqSql(jdbcUrl);
    jparqSql.query("select id, first_name, last_name from employees", rs -> {
      try {
        ResultSetMetaData rsm = rs.getMetaData();
        assertEquals("INTEGER", rsm.getColumnTypeName(1));
        assertEquals("VARCHAR", rsm.getColumnTypeName(2));
        assertEquals("VARCHAR", rsm.getColumnTypeName(3));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
