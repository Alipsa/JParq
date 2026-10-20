package se.alipsa.jparq.engine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;

/** Supplies typed outer values while an eagerly consumed nested query runs. */
public final class CorrelatedBindings implements AutoCloseable {
  private static final String FUNCTION_NAME = "__JPARQ_CORRELATED_VALUE";
  private static final ThreadLocal<Map<String, List<Object>>> ACTIVE = new ThreadLocal<>();
  private final String key;
  private final List<Object> previous;

  /**
   * Installs one binding frame, preserving ancestor frames for deeper queries.
   *
   * @param key
   *          query-shape identity
   * @param values
   *          typed values, including nulls
   */
  public CorrelatedBindings(String key, List<Object> values) {
    this.key = key;
    Map<String, List<Object>> active = ACTIVE.get();
    if (active == null) {
      active = new HashMap<>();
      ACTIVE.set(active);
    }
    previous = active.put(key, values);
  }

  /**
   * Recognizes the private placeholder used in parsed correlated query shapes.
   *
   * @param function
   *          function expression
   * @return whether this expression reads a correlated binding
   */
  public static boolean matches(Function function) {
    return FUNCTION_NAME.equalsIgnoreCase(function.getName());
  }

  /**
   * Reads a typed value without rendering or reparsing a SQL literal.
   *
   * @param function
   *          private placeholder expression
   * @return current outer value, possibly null
   * @throws IllegalStateException
   *           if the placeholder is evaluated outside its binding frame
   */
  public static Object value(Function function) {
    var arguments = function.getParameters();
    String key = ((StringValue) arguments.get(0)).getValue();
    int index = Math.toIntExact(((LongValue) arguments.get(1)).getValue());
    var active = ACTIVE.get();
    if (active == null || !active.containsKey(key)) {
      throw new IllegalStateException("Correlated binding is not active");
    }
    return active.get(key).get(index);
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
