package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Types;
import java.util.Collections;
import java.util.List;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqResultSetMetaData;

/** Tests for {@link JParqResultSetMetaData}. */
class JParqResultSetMetaDataTest {

  @Test
  void rowNumberFunctionReportsBigIntType() {
    JParqResultSetMetaData metaData = createMetadataWithExpression(schemaWithField(), List.of("row_number"),
        Collections.singletonList(null), List.of(analyticExpression("ROW_NUMBER", null)));

    assertEquals(Types.BIGINT, metaData.getColumnType(1), "ROW_NUMBER should be reported as BIGINT");
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
  void leadFunctionUsesReferencedColumnType() {
    Schema schema = schemaWithField();
    Column column = new Column();
    column.setColumnName("value");
    AnalyticExpression leadExpression = analyticExpression("LEAD", column);
    JParqResultSetMetaData metaData = createMetadataWithExpression(schema, List.of("lead_value"),
        Collections.singletonList(null), List.of(leadExpression));

    assertEquals(Types.DOUBLE, metaData.getColumnType(1), "LEAD should report the referenced column type");
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
    return new JParqResultSetMetaData(schema, labels, physicalNames, "test", expressions);
  }
}
