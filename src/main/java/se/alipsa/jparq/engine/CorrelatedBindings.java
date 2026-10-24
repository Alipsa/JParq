package se.alipsa.jparq.engine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import org.apache.avro.Schema;

/** Supplies typed outer values while an eagerly consumed nested query runs. */
public final class CorrelatedBindings implements AutoCloseable {
  private static final String FUNCTION_NAME = "__JPARQ_CORRELATED_VALUE";
  private static final ThreadLocal<Map<String, Frame>> ACTIVE = new ThreadLocal<>();
  private final String key;
  private final Frame previous;

  /** Values and optional source schemas belonging to one active query. */
  private record Frame(List<Object> values, List<Schema> schemas) {
  }

  /**
   * Installs one binding frame, preserving ancestor frames for deeper queries.
   *
   * @param key
   *          query-shape identity
   * @param values
   *          typed values, including nulls
   */
  public CorrelatedBindings(String key, List<Object> values) {
    this(key, values, List.of());
  }

  /**
   * Installs typed values with source schemas for schema-dependent table
   * functions.
   *
   * @param key
   *          query-shape identity
   * @param values
   *          typed values, including nulls
   * @param schemas
   *          source schemas in slot order, or an empty list when unknown
   */
  public CorrelatedBindings(String key, List<Object> values, List<Schema> schemas) {
    this.key = key;
    Map<String, Frame> active = ACTIVE.get();
    if (active == null) {
      active = new HashMap<>();
      ACTIVE.set(active);
    }
    previous = active.put(key, new Frame(values, schemas));
  }

  /**
   * Recognizes the private placeholder used in parsed correlated query shapes.
   *
   * @param function
   *          function expression
   * @return whether this expression reads a correlated binding
   */
  public static boolean matches(Function function) {
    var arguments = function.getParameters();
    return isReserved(function) && arguments != null && arguments.size() == 2 && arguments.get(0) instanceof StringValue
        && arguments.get(1) instanceof LongValue;
  }

  /**
   * Identifies the reserved internal function, including malformed calls.
   *
   * @param function
   *          function expression
   * @return whether the function uses the reserved binding name
   */
  public static boolean isReserved(Function function) {
    return FUNCTION_NAME.equalsIgnoreCase(function.getName());
  }

  /**
   * Reads a typed value without rendering or reparsing a SQL literal.
   *
   * @param function
   *          private placeholder expression
   * @return current outer value, possibly null
   * @throws IllegalArgumentException
   *           if the call is malformed, inactive, or has an invalid slot
   */
  public static Object value(Function function) {
    String message = "Invalid correlated binding: reserved for internal query execution";
    if (!matches(function)) {
      throw new IllegalArgumentException(message);
    }
    var arguments = function.getParameters();
    String key = ((StringValue) arguments.get(0)).getValue();
    var index = ((LongValue) arguments.get(1)).getBigIntegerValue();
    var active = ACTIVE.get();
    Frame frame = active == null ? null : active.get(key);
    List<Object> values = frame == null ? null : frame.values();
    if (values == null || index.signum() < 0 || index.compareTo(java.math.BigInteger.valueOf(values.size())) >= 0) {
      throw new IllegalArgumentException(message);
    }
    return values.get(index.intValue());
  }

  /**
   * Reads a binding's source schema, including when its current value is null.
   *
   * @param function
   *          private placeholder expression
   * @return source schema, or null when the binding has no schema information
   * @throws IllegalArgumentException
   *           if the binding is invalid or inactive
   */
  public static Schema schema(Function function) {
    value(function);
    var arguments = function.getParameters();
    Frame frame = ACTIVE.get().get(((StringValue) arguments.get(0)).getValue());
    int index = ((LongValue) arguments.get(1)).getBigIntegerValue().intValue();
    return frame.schemas().isEmpty() ? null : frame.schemas().get(index);
  }

  /** Restores ancestor bindings and removes thread state after the last frame. */
  @Override
  public void close() {
    var active = ACTIVE.get();
    if (previous == null) {
      active.remove(key);
    } else {
      active.put(key, previous);
    }
    if (active.isEmpty()) {
      ACTIVE.remove();
    }
  }

  /**
   * Placeholder emitted by the schema-aware correlation rewriter.
   *
   * @param key
   *          query-shape identity
   * @param index
   *          zero-based binding index
   */
  public record Reference(String key, int index) {
    /**
     * Builds a parseable placeholder while preserving the original Java value.
     *
     * @return placeholder function
     */
    public Function expression() {
      var function = new Function();
      function.setName(FUNCTION_NAME);
      function.setParameters(new ExpressionList<>(new StringValue(key), new LongValue(index)));
      return function;
    }
  }
}
