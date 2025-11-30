package se.alipsa.jparq.engine.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import org.apache.avro.generic.GenericRecord;

/**
 * Resolves the arguments of a SQL {@link Function} expression using the
 * provided evaluator.
 */
public final class FunctionArguments {

  private final Function function;
  private final GenericRecord record;
  private final BiFunction<Expression, GenericRecord, Object> evaluator;
  private List<Object> positionalValues;
  private NamedArgs namedValues;
  private List<Expression> positionalExpressions;

  /**
   * Create a resolver for the supplied {@link Function}.
   *
   * @param function
   *          the function to resolve
   * @param record
   *          the current record used when evaluating child expressions
   * @param evaluator
   *          evaluation callback for child expressions
   */
  public FunctionArguments(Function function, GenericRecord record,
      BiFunction<Expression, GenericRecord, Object> evaluator) {
    this.function = function;
    this.record = record;
    this.evaluator = evaluator;
  }

  /**
   * Return the positional expressions exactly as they appear in the function
   * call.
   *
   * @return immutable list of positional expressions (may be empty)
   */
  public List<Expression> positionalExpressions() {
    if (positionalExpressions != null) {
      return positionalExpressions;
    }
    if (!(function.getParameters() instanceof ExpressionList<?> params) || params.isEmpty()) {
      positionalExpressions = List.of();
      return positionalExpressions;
    }
    List<Expression> expressions = new ArrayList<>(params.size());
    for (Expression expr : params) {
      expressions.add(expr);
    }
    positionalExpressions = Collections.unmodifiableList(expressions);
    return positionalExpressions;
  }

  /**
   * Evaluate and return the positional argument values.
   *
   * @return immutable list of evaluated positional values (may be empty)
   */
  public List<Object> positionalValues() {
    if (positionalValues != null) {
      return positionalValues;
    }
    List<Expression> expressions = positionalExpressions();
    if (expressions.isEmpty()) {
      positionalValues = List.of();
      return positionalValues;
    }
    List<Object> values = new ArrayList<>(expressions.size());
    for (Expression expr : expressions) {
      values.add(evaluator.apply(expr, record));
    }
    positionalValues = Collections.unmodifiableList(values);
    return positionalValues;
  }

  /**
   * Evaluate and return the named arguments, if present.
   *
   * @return named arguments or {@code null} when the function has no named
   *         parameters
   */
  public NamedArgs named() {
    if (namedValues != null) {
      return namedValues;
    }
    if (!(function.getNamedParameters() instanceof NamedExpressionList<?> namedParams)) {
      return null;
    }
    List<String> names = new ArrayList<>(namedParams.getNames());
    List<Object> values = new ArrayList<>(names.size());
    for (Expression expr : namedParams) {
      values.add(evaluator.apply(expr, record));
    }
    namedValues = new NamedArgs(Collections.unmodifiableList(names), Collections.unmodifiableList(values));
    return namedValues;
  }

  /**
   * Return the first positional value or {@code null} when no positional
   * arguments are present.
   *
   * @return the first positional value or {@code null}
   */
  public Object firstValue() {
    List<Object> values = positionalValues();
    return values.isEmpty() ? null : values.getFirst();
  }

  /**
   * Evaluate a single expression using the configured evaluator.
   *
   * @param expression
   *          the expression to evaluate
   * @return the evaluated value
   */
  public Object evaluate(Expression expression) {
    return evaluator.apply(expression, record);
  }

  /**
   * Named argument values for a function call.
   *
   * @param names
   *          argument labels in the order provided by the parser
   * @param values
   *          evaluated values corresponding to each name
   */
  public record NamedArgs(List<String> names, List<Object> values) {
  }
}
