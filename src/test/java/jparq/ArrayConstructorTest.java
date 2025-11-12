package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.math.BigDecimal;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;

/** Tests for SQL ARRAY constructor evaluation. */
class ArrayConstructorTest {

  private static Schema schema;
  private static GenericRecord record;

  @BeforeAll
  static void initRecord() {
    schema = SchemaBuilder.record("dummy_record").fields().name("id").type().intType().noDefault().endRecord();
    record = new GenericData.Record(schema);
    record.put("id", 0);
  }

  @Test
  void constructsArrayFromLiterals() throws Exception {
    ValueExpressionEvaluator evaluator = new ValueExpressionEvaluator(schema);
    Expression expression = CCJSqlParserUtil.parseExpression("ARRAY[1, 2, 3]");
    Object result = evaluator.eval(expression, record);
    assertInstanceOf(List.class, result);
    assertEquals(List.of(1L, 2L, 3L), result);
  }

  @Test
  void promotesNumericTypesWithinArray() throws Exception {
    ValueExpressionEvaluator evaluator = new ValueExpressionEvaluator(schema);
    Expression expression = CCJSqlParserUtil.parseExpression("ARRAY[1, 2.5, 3]");
    Object result = evaluator.eval(expression, record);
    assertInstanceOf(List.class, result);
    assertEquals(List.of(BigDecimal.valueOf(1L), new BigDecimal("2.5"), BigDecimal.valueOf(3L)), result);
  }

  @Test
  void constructsArrayFromSubquery(@TempDir Path tempDir) throws Exception {
    Class.forName("se.alipsa.jparq.JParqDriver");
    List<List<Object>> rows = List.of(List.of(10), List.of(20), List.of(30));
    try (Connection connection = DriverManager.getConnection("jdbc:jparq:" + tempDir.toAbsolutePath())) {
      SubqueryExecutor executor = new SubqueryExecutor((JParqConnection) connection,
          sql -> stubPreparedStatement(List.of("val"), rows));
      ValueExpressionEvaluator evaluator = new ValueExpressionEvaluator(schema, executor);
      Expression expression = CCJSqlParserUtil.parseExpression("ARRAY(SELECT val FROM numbers)");
      Object result = evaluator.eval(expression, record);
      assertInstanceOf(List.class, result);
      assertEquals(List.of(10, 20, 30), result);
    }
  }

  private static PreparedStatement stubPreparedStatement(List<String> labels, List<List<Object>> rows) {
    InvocationHandler handler = (proxy, method, args) -> {
      String name = method.getName();
      switch (name) {
        case "executeQuery":
          return createResultSet(labels, rows);
        case "close":
          return null;
        case "getParameterMetaData":
          return null;
        case "getMetaData":
          return createMetaData(labels);
        case "isWrapperFor":
          return args != null && args.length == 1 && ((Class<?>) args[0]).isInstance(proxy);
        case "unwrap":
          if (args != null && args.length == 1 && ((Class<?>) args[0]).isInstance(proxy)) {
            return proxy;
          }
          throw new SQLException("Not a wrapper for "
              + (args == null ? "" : args[0]));
        default:
          return defaultValue(method.getReturnType());
      }
    };
    return (PreparedStatement) Proxy.newProxyInstance(
        PreparedStatement.class.getClassLoader(),
        new Class<?>[]{ PreparedStatement.class },
        handler);
  }

  private static ResultSet createResultSet(List<String> labels, List<List<Object>> rows) {
    ResultSetMetaData metaData = createMetaData(labels);
    int[] index = new int[]{ -1 };
    InvocationHandler handler = (proxy, method, args) -> {
      String name = method.getName();
      switch (name) {
        case "next":
          index[0]++;
          return index[0] < rows.size();
        case "getObject":
          int column = ((Integer) args[0]) - 1;
          return rows.get(index[0]).get(column);
        case "getMetaData":
          return metaData;
        case "close":
          return null;
        case "wasNull":
          return false;
        case "isClosed":
          return false;
        case "isWrapperFor":
          return args != null && args.length == 1 && ((Class<?>) args[0]).isInstance(proxy);
        case "unwrap":
          if (args != null && args.length == 1 && ((Class<?>) args[0]).isInstance(proxy)) {
            return proxy;
          }
          throw new SQLException("Not a wrapper for "
              + (args == null ? "" : args[0]));
        default:
          return defaultValue(method.getReturnType());
      }
    };
    return (ResultSet) Proxy.newProxyInstance(
        ResultSet.class.getClassLoader(),
        new Class<?>[]{ ResultSet.class },
        handler);
  }

  private static ResultSetMetaData createMetaData(List<String> labels) {
    InvocationHandler handler = (proxy, method, args) -> {
      String name = method.getName();
      switch (name) {
        case "getColumnCount":
          return labels.size();
        case "getColumnLabel":
        case "getColumnName":
          if (args == null || args.length == 0) {
            throw new SQLException(name + " called with insufficient arguments");
          }
          int column = ((Integer) args[0]) - 1;
          return labels.get(column);
        case "getColumnType":
          return Types.OTHER;
        case "isWrapperFor":
          return args != null && args.length == 1 && ((Class<?>) args[0]).isInstance(proxy);
        case "unwrap":
          if (args != null && args.length == 1 && ((Class<?>) args[0]).isInstance(proxy)) {
            return proxy;
          }
          throw new SQLException("Not a wrapper for "
              + (args == null ? "" : args[0]));
        default:
          return defaultValue(method.getReturnType());
      }
    };
    return (ResultSetMetaData) Proxy.newProxyInstance(
        ResultSetMetaData.class.getClassLoader(),
        new Class<?>[]{ ResultSetMetaData.class },
        handler);
  }

  private static Object defaultValue(Class<?> returnType) {
    if (returnType == Void.TYPE) {
      return null;
    }
    if (!returnType.isPrimitive()) {
      return null;
    }
    if (returnType == boolean.class) {
      return false;
    }
    if (returnType == byte.class) {
      return (byte) 0;
    }
    if (returnType == short.class) {
      return (short) 0;
    }
    if (returnType == int.class) {
      return 0;
    }
    if (returnType == long.class) {
      return 0L;
    }
    if (returnType == float.class) {
      return 0.0f;
    }
    if (returnType == double.class) {
      return 0.0d;
    }
    if (returnType == char.class) {
      return '\0';
    }
    return null;
  }
}
