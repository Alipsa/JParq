package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqDriver;
import se.alipsa.jparq.engine.CorrelationContextBuilder;
import se.alipsa.jparq.engine.ExpressionEvaluator;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;
import se.alipsa.jparq.engine.window.WindowState;

/**
 * Tests that correlated subquery rewriting resolves qualified outer references
 * using qualifier-aware mappings.
 */
class CorrelatedQualifierResolutionTest {

  @Test
  void correlatedExistsResolvesQualifiedColumn() throws Exception {
    Schema schema = SchemaBuilder.record("Outer").fields().name("outer_id").type().intType().noDefault().endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("outer_id", 7);

    Map<String, Map<String, String>> qualifierMapping = Map.of("outer_alias", Map.of("id", "outer_id"));

    SubqueryExecutor.SubqueryResult subqueryResult = new SubqueryExecutor.SubqueryResult(List.of("c"),
        List.of(List.of(1)));
    StubbedExecutor executor = new StubbedExecutor(subqueryResult);

    ExpressionEvaluator evaluator = new ExpressionEvaluator(schema, executor.executor(), List.of("outer_alias"),
        qualifierMapping, Map.of());

    Expression existsExpression = CCJSqlParserUtil
        .parseCondExpression("EXISTS (SELECT 1 FROM dummy d WHERE d.owner = outer_alias.id)");

    assertTrue(evaluator.eval(existsExpression, record));
    assertNotNull(executor.lastSql());
    assertTrue(executor.lastSql().contains("= 7"), "Correlated rewrite should embed the outer alias value");
  }

  @Test
  void correlatedArraySubqueryResolvesQualifiedColumn() throws Exception {
    Schema schema = SchemaBuilder.record("Outer").fields().name("outer_id").type().intType().noDefault().endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("outer_id", 3);

    Map<String, Map<String, String>> qualifierMapping = Map.of("outer_alias", Map.of("id", "outer_id"));

    SubqueryExecutor.SubqueryResult subqueryResult = new SubqueryExecutor.SubqueryResult(List.of("value"),
        List.of(List.of(10), List.of(11)));
    StubbedExecutor executor = new StubbedExecutor(subqueryResult);

    ValueExpressionEvaluator evaluator = new ValueExpressionEvaluator(schema, executor.executor(),
        List.of("outer_alias"), qualifierMapping, Map.of());

    Expression arrayExpression = CCJSqlParserUtil
        .parseExpression("ARRAY(SELECT value FROM dummy d WHERE d.owner = outer_alias.id)");

    Object value = evaluator.eval(arrayExpression, record);
    assertEquals(List.of(10, 11), value);
    assertNotNull(executor.lastSql());
    assertTrue(executor.lastSql().contains("= 3"), "Correlated rewrite should embed the outer alias value");
  }

  @Test
  void correlatedScalarSubqueryUsesCorrelationContextForAliases() throws Exception {
    Schema schema = SchemaBuilder.record("Derived").fields().name("base_id").type().intType().noDefault().endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("base_id", 9);

    Map<String, Map<String, String>> correlationContext = CorrelationContextBuilder.build(List.of("d"),
        List.of("alias_id"), List.of("base_id"), Map.of());

    SubqueryExecutor.SubqueryResult subqueryResult = new SubqueryExecutor.SubqueryResult(List.of("v"),
        List.of(List.of("ok")));
    StubbedExecutor executor = new StubbedExecutor(subqueryResult);

    ValueExpressionEvaluator evaluator = new ValueExpressionEvaluator(schema, executor.executor(), List.of("d"),
        Map.of(), Map.of(), correlationContext, WindowState.empty());

    Expression expression = CCJSqlParserUtil.parseExpression("(SELECT v FROM dummy dd WHERE dd.parent = d.alias_id)");

    Object value = evaluator.eval(expression, record);
    assertEquals("ok", value);
    assertNotNull(executor.lastSql());
    assertTrue(executor.lastSql().contains("= 9"), "Correlation context should supply aliased columns to subqueries");
  }

  private static final class StubbedExecutor {

    private final SubqueryExecutor executor;
    private final AtomicReference<String> lastSql = new AtomicReference<>();

    StubbedExecutor(SubqueryExecutor.SubqueryResult result) throws Exception {
      SubqueryExecutor.StatementFactory factory = sql -> {
        lastSql.set(sql);
        return createPreparedStatement(() -> result);
      };
      var tempDir = Files.createTempDirectory("jparq-test");
      String url = JParqDriver.URL_PREFIX + tempDir;
      executor = new SubqueryExecutor(new JParqConnection(url, new Properties()), factory);
    }

    SubqueryExecutor executor() {
      return executor;
    }

    String lastSql() {
      return lastSql.get();
    }
  }

  private static java.sql.PreparedStatement createPreparedStatement(
      Supplier<SubqueryExecutor.SubqueryResult> resultSupplier) {
    return (java.sql.PreparedStatement) Proxy.newProxyInstance(CorrelatedQualifierResolutionTest.class.getClassLoader(),
        new Class<?>[]{
            java.sql.PreparedStatement.class
        }, (proxy, method, args) -> switch (method.getName()) {
          case "executeQuery" -> createResultSet(resultSupplier.get());
          case "close" -> null;
          case "unwrap" -> proxy;
          case "isWrapperFor" -> Boolean.FALSE;
          default -> throw new UnsupportedOperationException("Unsupported method: " + method.getName());
        });
  }

  private static java.sql.ResultSet createResultSet(SubqueryExecutor.SubqueryResult result) {
    return (java.sql.ResultSet) Proxy.newProxyInstance(CorrelatedQualifierResolutionTest.class.getClassLoader(),
        new Class<?>[]{
            java.sql.ResultSet.class
        }, new ResultSetInvocationHandler(result));
  }

  private static final class ResultSetInvocationHandler implements java.lang.reflect.InvocationHandler {

    private final List<List<Object>> rows;
    private final List<String> labels;
    private int index = -1;

    ResultSetInvocationHandler(SubqueryExecutor.SubqueryResult result) {
      this.rows = result.rows();
      this.labels = result.columnLabels();
    }

    @Override
    public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) {
      return switch (method.getName()) {
        case "next" -> next();
        case "getObject" -> getObject(((Number) args[0]).intValue());
        case "getMetaData" -> createMetadata();
        case "close" -> null;
        case "unwrap" -> proxy;
        case "isWrapperFor" -> Boolean.FALSE;
        default -> throw new UnsupportedOperationException("Unsupported method: " + method.getName());
      };
    }

    private Boolean next() {
      index++;
      return index < rows.size();
    }

    private Object getObject(int columnIndex) {
      return rows.get(index).get(columnIndex - 1);
    }

    private java.sql.ResultSetMetaData createMetadata() {
      return (java.sql.ResultSetMetaData) Proxy
          .newProxyInstance(CorrelatedQualifierResolutionTest.class.getClassLoader(), new Class<?>[]{
              java.sql.ResultSetMetaData.class
          }, (metaProxy, method, metaArgs) -> switch (method.getName()) {
            case "getColumnCount" -> labels.size();
            case "getColumnLabel", "getColumnName" -> labels.get(((Number) metaArgs[0]).intValue() - 1);
            default -> throw new UnsupportedOperationException("Unsupported method: " + method.getName());
          });
    }
  }
}
