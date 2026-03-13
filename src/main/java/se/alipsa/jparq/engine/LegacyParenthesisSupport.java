package se.alipsa.jparq.engine;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import net.sf.jsqlparser.expression.Expression;

/**
 * Compatibility helpers for JSqlParser's legacy {@code Parenthesis} expression
 * wrapper.
 */
public final class LegacyParenthesisSupport {

  private static final String LEGACY_PARENTHESIS_CLASS = "net.sf.jsqlparser.expression.Parenthesis";
  private static volatile ParenthesisAccessor accessor;

  private LegacyParenthesisSupport() {
  }

  /**
   * Determine whether the supplied expression is JSqlParser's legacy
   * {@code Parenthesis} wrapper.
   *
   * @param expression
   *          expression to inspect
   * @return {@code true} if the expression is the legacy parenthesis wrapper
   */
  public static boolean isLegacyParenthesis(Expression expression) {
    return expression != null && LEGACY_PARENTHESIS_CLASS.equals(expression.getClass().getName());
  }

  /**
   * Extract the wrapped expression from a legacy {@code Parenthesis} node.
   *
   * @param expression
   *          parenthesized expression wrapper
   * @return the wrapped expression
   * @throws IllegalStateException
   *           if the legacy wrapper cannot be accessed reflectively
   */
  public static Expression extract(Expression expression) {
    try {
      return (Expression) accessorFor(expression.getClass()).getter().invoke(expression);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Unable to access legacy Parenthesis expression", e);
    }
  }

  /**
   * Replace the wrapped expression inside a legacy {@code Parenthesis} node.
   *
   * @param expression
   *          parenthesized expression wrapper
   * @param nestedExpression
   *          expression to store
   * @throws IllegalStateException
   *           if the legacy wrapper cannot be updated reflectively
   */
  public static void replace(Expression expression, Expression nestedExpression) {
    try {
      accessorFor(expression.getClass()).setter().invoke(expression, nestedExpression);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Unable to update legacy Parenthesis expression", e);
    }
  }

  /**
   * Resolve and cache reflective accessors for the legacy parenthesis type.
   *
   * @param expressionClass
   *          runtime class of the legacy parenthesis wrapper
   * @return cached reflective accessor
   * @throws IllegalStateException
   *           if the required methods cannot be resolved
   */
  private static ParenthesisAccessor accessorFor(Class<?> expressionClass) {
    ParenthesisAccessor current = accessor;
    if (current != null && current.expressionClass() == expressionClass) {
      return current;
    }
    synchronized (LegacyParenthesisSupport.class) {
      current = accessor;
      if (current != null && current.expressionClass() == expressionClass) {
        return current;
      }
      try {
        Method getter = expressionClass.getMethod("getExpression");
        Method setter = expressionClass.getMethod("setExpression", Expression.class);
        accessor = new ParenthesisAccessor(expressionClass, getter, setter);
        return accessor;
      } catch (NoSuchMethodException e) {
        throw new IllegalStateException("Unable to resolve legacy Parenthesis accessors", e);
      }
    }
  }

  /**
   * Cached reflective accessors for the legacy parenthesis wrapper.
   *
   * @param expressionClass
   *          runtime class of the wrapper
   * @param getter
   *          reflective getter for the wrapped expression
   * @param setter
   *          reflective setter for the wrapped expression
   */
  private record ParenthesisAccessor(Class<?> expressionClass, Method getter, Method setter) {
  }
}
