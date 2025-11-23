package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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

/**
 * Verifies that correlated subqueries retain derived qualifier mappings
 * provided through the correlation context when delegating to
 * {@link SubqueryExecutor}.
 */
class SubqueryCorrelatedFiltersIsolatorUnitTest {

  @Test
  void derivedQualifiersFromCorrelationContextDriveRewrite() throws Exception {
    Schema schema = SchemaBuilder.record("Derived").fields().name("derived_id").type().intType().noDefault()
        .endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("derived_id", 5);

    Map<String, Map<String, String>> correlationContext = CorrelationContextBuilder.build(List.of("derived"),
        List.of("alias_id"), List.of("derived_id"), Map.of());

    SubqueryExecutor.SubqueryResult subqueryResult = new SubqueryExecutor.SubqueryResult(List.of("c"),
        List.of(List.of(1)));
    AtomicReference<String> lastSql = new AtomicReference<>();
    SubqueryExecutor executor = new SubqueryExecutor(new JParqConnection(jdbcUrl(), new Properties()), sql -> {
      lastSql.set(sql);
      return createPreparedStatement(() -> subqueryResult);
    });

    ExpressionEvaluator evaluator = new ExpressionEvaluator(schema, executor, List.of(), correlationContext, Map.of());
    Expression exists = CCJSqlParserUtil
        .parseCondExpression("EXISTS (SELECT 1 FROM dummy d WHERE d.owner = derived.alias_id)");

    assertTrue(evaluator.eval(exists, record));
    String sql = lastSql.get();
    assertNotNull(sql, "Subquery SQL should be captured");
    assertTrue(sql.contains("= 5"), "Correlated rewrite should embed derived alias value but was " + sql);
    assertFalse(sql.contains("derived.alias_id"), "Correlation context should prevent unresolved qualifier in " + sql);
  }

  private static String jdbcUrl() throws Exception {
    var tempDir = Files.createTempDirectory("jparq-isolator-test");
    return JParqDriver.URL_PREFIX + tempDir;
  }

  private static PreparedStatement createPreparedStatement(Supplier<SubqueryExecutor.SubqueryResult> resultSupplier) {
    return (PreparedStatement) Proxy.newProxyInstance(SubqueryCorrelatedFiltersIsolatorUnitTest.class.getClassLoader(),
        new Class<?>[]{
            PreparedStatement.class
        }, (proxy, method, args) -> switch (method.getName()) {
          case "executeQuery" -> createResultSet(resultSupplier.get());
          case "close" -> null;
          case "unwrap" -> proxy;
          case "isWrapperFor" -> Boolean.FALSE;
          default -> throw new UnsupportedOperationException("Unsupported method: " + method.getName());
        });
  }

  private static ResultSet createResultSet(SubqueryExecutor.SubqueryResult result) {
    return (ResultSet) Proxy.newProxyInstance(SubqueryCorrelatedFiltersIsolatorUnitTest.class.getClassLoader(),
        new Class<?>[]{
            ResultSet.class
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

    private ResultSetMetaData createMetadata() {
      return (ResultSetMetaData) Proxy
          .newProxyInstance(SubqueryCorrelatedFiltersIsolatorUnitTest.class.getClassLoader(), new Class<?>[]{
              ResultSetMetaData.class
          }, (metaProxy, method, metaArgs) -> switch (method.getName()) {
            case "getColumnCount" -> labels.size();
            case "getColumnLabel", "getColumnName" -> labels.get(((Number) metaArgs[0]).intValue() - 1);
            default -> throw new UnsupportedOperationException("Unsupported method: " + method.getName());
          });
    }
  }
}
