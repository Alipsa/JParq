package se.alipsa.jparq.engine.function;

import static se.alipsa.jparq.engine.SqlExceptions.syntaxFailure;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.IsUnknownExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.NumericType;
import se.alipsa.jparq.engine.ColumnMappingUtil;
import se.alipsa.jparq.engine.CorrelatedBindings;
import se.alipsa.jparq.engine.CorrelationMappings;
import se.alipsa.jparq.engine.ExpressionEvaluator;
import se.alipsa.jparq.engine.Identifier;
import se.alipsa.jparq.engine.JSqlParserSupport;
import se.alipsa.jparq.engine.LegacyParenthesisSupport;
import se.alipsa.jparq.engine.Operation;
import se.alipsa.jparq.engine.OrderingUtil;
import se.alipsa.jparq.engine.QuantifiedComparisons;
import se.alipsa.jparq.engine.RecordReader;
import se.alipsa.jparq.engine.RelationalKeys;
import se.alipsa.jparq.engine.SqlComparisons;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.engine.SqlParser.OrderKey;
import se.alipsa.jparq.engine.SqlTruth;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.engine.ValueExpressionEvaluator;
import se.alipsa.jparq.engine.window.WindowState;
import se.alipsa.jparq.helper.JParqUtil;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.LiteralConverter;

/**
 * Utilities for detecting and evaluating aggregate functions in SELECT lists.
 */
public final class AggregateFunctions {

  private AggregateFunctions() {
  }

  /** Supported aggregate function names. */
  private enum AggregateType {
    COUNT, SUM, AVG, MAX, MIN, STRING_AGG;

    static AggregateType from(String name) {
      if (name == null) {
        return null;
      }
      return switch (name.toUpperCase(Locale.ROOT)) {
        case "COUNT" -> COUNT;
        case "SUM" -> SUM;
        case "AVG", "AVERAGE" -> AVG;
        case "MAX" -> MAX;
        case "MIN" -> MIN;
        case "STRING_AGG" -> STRING_AGG;
        default -> null;
      };
    }
  }

  /**
   * Plan describing aggregate and grouping behaviour for a SELECT statement.
   *
   * @param specs
   *          aggregate specifications in the order aggregates appear in the
   *          SELECT list
   * @param resultColumns
   *          result column definitions in SELECT order describing whether each
   *          column is a grouping expression or aggregate function
   * @param groupExpressions
   *          expressions that participate in the GROUP BY clause
   * @param groupingSets
   *          grouping sets derived from the GROUP BY clause
   */
  public record AggregatePlan(List<AggregateSpec> specs, List<ResultColumn> resultColumns,
      List<GroupExpression> groupExpressions, List<GroupingSet> groupingSets) {
    /**
     * Canonical constructor validating the provided collections.
     *
     * @param specs
     *          aggregate specifications in the order aggregates appear in the
     *          SELECT list
     * @param resultColumns
     *          result column definitions in SELECT order describing whether each
     *          column is a grouping expression or aggregate function
     * @param groupExpressions
     *          expressions that participate in the GROUP BY clause
     * @param groupingSets
     *          grouping sets derived from the GROUP BY clause
     */
    public AggregatePlan {
      Objects.requireNonNull(specs, "specs");
      Objects.requireNonNull(resultColumns, "resultColumns");
      Objects.requireNonNull(groupExpressions, "groupExpressions");
      Objects.requireNonNull(groupingSets, "groupingSets");
      specs = List.copyOf(specs);
      resultColumns = List.copyOf(resultColumns);
      groupExpressions = List.copyOf(groupExpressions);
      groupingSets = List.copyOf(groupingSets);
    }

    /**
     * Projection labels in the same order as the SELECT list.
     *
     * @return immutable list of labels
     */
    public List<String> labels() {
      List<String> labels = new ArrayList<>(resultColumns.size());
      for (ResultColumn column : resultColumns) {
        labels.add(column.label());
      }
      return List.copyOf(labels);
    }
  }

  /**
   * Metadata describing a column emitted by an aggregate query.
   *
   * @param label
   *          column label exposed to JDBC clients
   * @param aggregateIndex
   *          index of the aggregate specification backing the column, or
   *          {@code -1} for other column kinds
   * @param groupIndex
   *          index into the {@link GroupExpression} list describing the grouping
   *          value used for this column, or {@code -1} when the column is not a
   *          grouping expression
   * @param kind
   *          origin of the column within the aggregate plan
   * @param groupingIndexes
   *          grouping expression indexes referenced by a GROUPING() invocation
   * @param expressionText
   *          textual representation of a GROUPING() or scalar expression
   * @param subquery
   *          scalar subquery evaluated after grouping, or null
   */
  public record ResultColumn(String label, ColumnKind kind, int aggregateIndex, int groupIndex,
      List<Integer> groupingIndexes, String expressionText, Select subquery) {
    /**
     * Canonical constructor normalising the provided column metadata.
     *
     * @param label
     *          column label exposed to JDBC clients
     * @param aggregateIndex
     *          index of the aggregate specification backing the column, or
     *          {@code -1} for other column kinds
     * @param groupIndex
     *          index into the {@link GroupExpression} list describing the grouping
     *          value used for this column, or {@code -1} when the column is not a
     *          grouping expression
     * @param kind
     *          origin of the column within the aggregate plan
     * @param groupingIndexes
     *          grouping expression indexes referenced by a GROUPING() invocation
     * @param expressionText
     *          textual representation of a GROUPING() or scalar expression
     * @param subquery
     *          scalar subquery evaluated after grouping, or null
     */
    public ResultColumn {
      Objects.requireNonNull(label, "label");
      Objects.requireNonNull(kind, "kind");
      groupingIndexes = groupingIndexes == null ? List.of() : List.copyOf(groupingIndexes);
      expressionText = expressionText == null ? "" : expressionText;
      switch (kind) {
        case AGGREGATE -> {
          if (aggregateIndex < 0) {
            throw new IllegalArgumentException("Aggregate result column requires a valid aggregate index");
          }
        }
        case GROUP -> {
          if (groupIndex < 0) {
            throw new IllegalArgumentException("Group expression column requires a valid group index");
          }
        }
        case GROUPING -> {
          if (groupingIndexes.isEmpty()) {
            throw new IllegalArgumentException("GROUPING function requires at least one argument");
          }
        }
        case SCALAR_SUBQUERY -> Objects.requireNonNull(subquery, "subquery");
        case EXPRESSION -> Objects.requireNonNull(expressionText, "expressionText");
        default -> throw new IllegalArgumentException("Unsupported result column kind: " + kind);
      }
    }

    /**
     * Creates a result column without a scalar subquery.
     *
     * @param label
     *          JDBC column label
     * @param kind
     *          origin of the column
     * @param aggregateIndex
     *          aggregate specification index, or -1
     * @param groupIndex
     *          grouping expression index, or -1
     * @param groupingIndexes
     *          indexes used by GROUPING()
     * @param expressionText
     *          GROUPING() or scalar expression text
     */
    public ResultColumn(String label, ColumnKind kind, int aggregateIndex, int groupIndex,
        List<Integer> groupingIndexes, String expressionText) {
      this(label, kind, aggregateIndex, groupIndex, groupingIndexes, expressionText, null);
    }

    /**
     * Determine if the column represents an aggregate function.
     *
     * @return {@code true} when the column is sourced from an aggregate
     */
    public boolean isAggregate() {
      return kind == ColumnKind.AGGREGATE;
    }

    /**
     * Determine if the column represents a grouping function.
     *
     * @return {@code true} when the column originates from GROUPING()
     */
    public boolean isGrouping() {
      return kind == ColumnKind.GROUPING;
    }

    /**
     * Determine if the column represents a grouping expression value.
     *
     * @return {@code true} when the column provides a grouping expression value
     */
    public boolean isGroup() {
      return kind == ColumnKind.GROUP;
    }
  }

  /**
   * Enumeration describing the origin of a result column in an aggregate query.
   */
  public enum ColumnKind {
    /** Column originates from an aggregate function. */
    AGGREGATE,
    /** Column represents a grouping expression value. */
    GROUP,
    /** Column returns the output of a GROUPING() function. */
    GROUPING,
    /** Column returns a scalar subquery evaluated against the current group. */
    SCALAR_SUBQUERY,
    /** Column evaluates a scalar expression over completed group values. */
    EXPRESSION
  }

  /**
   * Definition of a grouping set.
   */
  public static final class GroupingSet {

    private final List<Integer> indexes;
    private final Set<Integer> indexSet;

    /**
     * Create a grouping set based on the supplied grouping expression indexes.
     *
     * @param indexes
     *          grouping expression indexes that participate in the set
     */
    public GroupingSet(List<Integer> indexes) {
      Objects.requireNonNull(indexes, "indexes");
      this.indexes = List.copyOf(indexes);
      indexSet = Set.copyOf(this.indexes);
    }

    /**
     * Retrieve the grouping expression indexes included in this grouping set.
     *
     * @return immutable list of grouping expression indexes
     */
    public List<Integer> indexes() {
      return indexes;
    }

    /**
     * Determine whether the grouping set includes the specified grouping
     * expression.
     *
     * @param index
     *          grouping expression index to check
     * @return {@code true} if the expression participates in the grouping set
     */
    public boolean contains(int index) {
      return indexSet.contains(index);
    }
  }

  /**
   * Representation of a grouping expression in the GROUP BY clause.
   *
   * @param expression
   *          expression used to compute the grouping key
   * @param label
   *          human readable label for the expression (column name or expression
   *          text)
   */
  public record GroupExpression(Expression expression, String label) {
    /**
     * Canonical constructor storing the grouping expression metadata.
     *
     * @param expression
     *          expression used to compute the grouping key
     * @param label
     *          human readable label for the expression (column name or expression
     *          text)
     */
    public GroupExpression {
      Objects.requireNonNull(expression, "expression");
      label = (label == null || label.isBlank()) ? expression.toString() : label;
    }
  }

  /**
   * Description of a single aggregate invocation.
   *
   * @param type
   *          aggregate function type
   * @param arguments
   *          function argument expressions (empty for COUNT(*))
   * @param label
   *          projection label exposed to JDBC
   * @param countStar
   *          true when representing COUNT(*)
   */
  public record AggregateSpec(AggregateType type, List<Expression> arguments, String label, boolean countStar) {
    /**
     * Canonical constructor enforcing invariant constraints for an aggregate
     * specification.
     *
     * @param type
     *          aggregate function type
     * @param arguments
     *          function argument expressions (empty for COUNT(*))
     * @param label
     *          projection label exposed to JDBC
     * @param countStar
     *          true when representing COUNT(*)
     */
    public AggregateSpec {
      Objects.requireNonNull(type, "type");
      Objects.requireNonNull(label, "label");
      arguments = arguments == null ? List.of() : List.copyOf(arguments);
      if (type == AggregateType.COUNT && countStar) {
        if (!arguments.isEmpty()) {
          throw new IllegalArgumentException("COUNT(*) cannot have arguments");
        }
      } else {
        if (arguments.isEmpty()) {
          throw new IllegalArgumentException("Aggregate function " + type + " requires arguments");
        }
      }
    }

    /**
     * Convenience accessor for the first argument.
     *
     * @return first argument expression or {@code null}
     */
    public Expression argument() {
      return arguments.isEmpty() ? null : arguments.getFirst();
    }
  }

  /**
   * Result of evaluating aggregates and grouping.
   *
   * @param rows
   *          computed rows in SELECT order
   * @param sqlTypes
   *          SQL types associated with each result column
   */
  public record AggregateResult(List<List<Object>> rows, List<Integer> sqlTypes) {
    /**
     * Canonical constructor ensuring result collections are present.
     *
     * @param rows
     *          computed rows in SELECT order
     * @param sqlTypes
     *          SQL types associated with each result column
     */
    public AggregateResult {
      Objects.requireNonNull(rows, "rows");
      Objects.requireNonNull(sqlTypes, "sqlTypes");
    }
  }

  /**
   * Identifies supported aggregate names, including aliases, without planning an
   * aggregate expression.
   *
   * @param name
   *          function name, possibly null
   * @return whether the name identifies a supported aggregate
   */
  public static boolean isAggregateName(String name) {
    return AggregateType.from(name) != null;
  }

  /**
   * Attempt to build an {@link AggregatePlan} for the provided SELECT. Returns
   * {@code null} if the statement contains no aggregates and no GROUP BY clause.
   * Aggregate discovery includes scalar operands in SELECT, HAVING, and ORDER BY.
   *
   * @param select
   *          parsed SELECT statement
   * @return aggregate plan for the SELECT list, or {@code null} when the query
   *         should be handled by the non-aggregate execution path. The planner
   *         validates SELECT expressions against the GROUP BY clause using
   *         structural expression comparison so that functionally equivalent
   *         constructs (for example {@code COALESCE} invocations) are accepted
   *         even when they are formatted differently. Scalar subqueries are
   *         evaluated after grouping; their outer references are validated
   *         against grouping columns before reading the aggregate input.
   */
  public static AggregatePlan plan(SqlParser.Select select) {
    List<Expression> expressions = select.expressions();
    if (expressions.isEmpty()) {
      return null;
    }

    boolean containsAggregate = expressions.stream().anyMatch(expr -> !aggregateFunctions(expr).isEmpty())
        || !aggregateFunctions(select.having()).isEmpty()
        || select.orderBy().stream().anyMatch(key -> !aggregateFunctions(key.expression()).isEmpty());
    List<GroupExpression> groupExpressions = buildGroupExpressions(select.groupByExpressions());
    final Map<String, Integer> groupIndexByText = groupIndexLookup(groupExpressions);
    List<GroupingSet> groupingSets = buildGroupingSets(select.groupingSets(), groupExpressions.size(),
        containsAggregate);

    boolean hasGroupBy = !groupExpressions.isEmpty() || !groupingSets.isEmpty();
    if (!hasGroupBy && !containsAggregate) {
      return null;
    }

    List<Expression> aggregateSources = new ArrayList<>(expressions);
    aggregateSources.add(select.having());
    select.orderBy().forEach(key -> aggregateSources.add(key.expression()));
    aggregateSources.forEach(AggregateFunctions::rejectGroupedWindows);

    List<String> labels = select.labels();
    List<AggregateSpec> specs = new ArrayList<>();
    List<ResultColumn> resultColumns = new ArrayList<>(expressions.size());

    for (int i = 0; i < expressions.size(); i++) {
      Expression expr = expressions.get(i);
      String label = labelFor(labels, i, expr);

      if (expr instanceof Function func) {
        AggregateSpec spec = aggregateSpec(func, label);
        if (spec != null) {
          int aggIndex = specs.size();
          specs.add(spec);
          resultColumns.add(new ResultColumn(label, ColumnKind.AGGREGATE, aggIndex, -1, List.of(), null, null));
          continue;
        }
        if (isGroupingFunction(func)) {
          List<Integer> groupingIndexes = groupingArgumentIndexes(func, groupIndexByText, groupExpressions);
          resultColumns
              .add(new ResultColumn(label, ColumnKind.GROUPING, -1, -1, groupingIndexes, func.toString(), null));
          continue;
        }
      }

      if (unwrap(expr) instanceof Select subquery) {
        resultColumns.add(new ResultColumn(label, ColumnKind.SCALAR_SUBQUERY, -1, -1, List.of(), null, subquery));
        continue;
      }

      Integer groupIndex = findGroupIndex(expr, groupIndexByText, groupExpressions);
      if (groupIndex == null) {
        GroupedExpression.validate(expr, operand -> {
          if (operand instanceof Column && findGroupIndex(operand, groupIndexByText, groupExpressions) == null) {
            throw syntaxFailure(
                "SELECT expression '" + expr + "' must appear in the GROUP BY clause when aggregates are present");
          }
          return isGroupOperand(operand, groupIndexByText, groupExpressions);
        });
        resultColumns.add(new ResultColumn(label, ColumnKind.EXPRESSION, -1, -1, List.of(), expr.toString()));
        continue;
      }
      resultColumns.add(new ResultColumn(label, ColumnKind.GROUP, -1, groupIndex, List.of(), null, null));
    }

    Set<String> outputNames = resultColumns.stream().map(column -> Identifier.lookupKey(column.label()))
        .collect(Collectors.toSet());
    validateGroupReferences(select.having(), "HAVING", groupIndexByText, groupExpressions, outputNames);
    for (OrderKey key : select.orderBy()) {
      validateGroupReferences(key.expression(), "ORDER BY", groupIndexByText, groupExpressions, outputNames);
    }

    for (Expression source : aggregateSources) {
      for (Function func : aggregateFunctions(source)) {
        AggregateSpec spec = aggregateSpec(func, func.toString());
        if (spec != null && !containsEquivalentAggregate(specs, spec)) {
          specs.add(spec);
        }
      }
    }

    return new AggregatePlan(specs, resultColumns, groupExpressions, groupingSets);
  }

  /**
   * Validates grouped scalar references without entering aggregate or subquery
   * scopes.
   *
   * @param expression
   *          scalar expression, or null
   * @param clause
   *          clause name for diagnostics
   * @param groupIndexByText
   *          grouping expression lookup
   * @param groups
   *          grouping expressions
   * @param aliases
   *          visible SELECT aliases
   * @throws IllegalArgumentException
   *           if an ungrouped column is referenced
   */
  private static void validateGroupReferences(Expression expression, String clause,
      Map<String, Integer> groupIndexByText, List<GroupExpression> groups, Set<String> aliases) {
    if (expression == null) {
      return;
    }
    GroupedExpression.validate(expression, operand -> {
      if (operand instanceof Column column && findGroupIndex(column, groupIndexByText, groups) == null
          && (column.getTable() != null || !aliases.contains(Identifier.lookupKey(column.getColumnName())))) {
        throw syntaxFailure("Missing columns in " + clause + " grouped scope: " + column
            + "; columns must appear in the GROUP BY clause or SELECT list");
      }
      return isGroupOperand(operand, groupIndexByText, groups);
    });
  }

  /**
   * Rejects windows in the current aggregate query while preserving nested query
   * scopes.
   *
   * @param expression
   *          expression to inspect, or null
   * @throws IllegalArgumentException
   *           if a window expression is present
   */
  private static void rejectGroupedWindows(Expression expression) {
    if (expression == null) {
      return;
    }
    expression.accept(new JSqlParserSupport.NamedArgumentVisitor() {
      @Override
      public <S> Void visit(AnalyticExpression analytic, S context) {
        throw syntaxFailure("Window functions are not supported in grouped queries: "
            + se.alipsa.jparq.engine.SqlExceptions.describe(analytic));
      }

      @Override
      public <S> Void visit(ParenthesedSelect select, S context) {
        return null;
      }

      @Override
      public <S> Void visit(Select select, S context) {
        return null;
      }
    }, null);
  }

  /**
   * Identifies operands supplied by the group rather than the scalar evaluator.
   *
   * @param operand
   *          candidate operand
   * @param groupIndexByText
   *          grouping expression lookup
   * @param groups
   *          grouping expressions
   * @return whether the operand needs a typed group binding
   */
  private static boolean isGroupOperand(Expression operand, Map<String, Integer> groupIndexByText,
      List<GroupExpression> groups) {
    return operand instanceof Column
        || operand instanceof Select || operand instanceof Function function && (isAggregateName(function.getName())
            || isGroupingFunction(function) || CorrelatedBindings.isReserved(function))
        || findGroupIndex(operand, groupIndexByText, groups) != null;
  }

  /**
   * Identifies predicates whose subquery or quantified operands require grouped
   * evaluation.
   *
   * @param expression
   *          candidate predicate
   * @return whether the whole predicate must use the grouped evaluator
   */
  private static boolean isGroupedPredicate(Expression expression) {
    return expression instanceof InExpression in && in.getRightExpression() instanceof Select
        || expression instanceof ExistsExpression
        || expression instanceof BinaryExpression binary
            && (JSqlParserSupport.isQuantifiedOperand(binary.getLeftExpression())
                || JSqlParserSupport.isQuantifiedOperand(binary.getRightExpression()));
  }

  /**
   * Removes surrounding single-expression parentheses without crossing query
   * scopes.
   *
   * @param expression
   *          expression to inspect
   * @return expression inside the parentheses
   */
  static Expression unwrap(Expression expression) {
    Expression result = ExpressionEvaluator.unwrapParenthesis(expression);
    while (result instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      result = ExpressionEvaluator.unwrapParenthesis(list.getFirst());
    }
    return result;
  }

  /**
   * Produce normalized lookup keys for the provided expression.
   *
   * <p>
   * The returned list contains the rendered expression text and, when the
   * expression represents a column reference, additional entries for any table
   * aliases or names that qualify the column. These keys allow quick matching
   * against GROUP BY expressions regardless of how they were written in the
   * query.
   * </p>
   *
   * @param expression
   *          the expression to analyze, may be {@code null}
   * @return immutable list of normalized expression keys
   */
  private static List<String> expressionKeys(Expression expression) {
    if (expression == null) {
      return List.of();
    }
    Set<String> keys = new LinkedHashSet<>();
    String rendered = expression.toString();
    addIfNotBlank(keys, rendered);
    if (expression instanceof Column column) {
      Identifier identifier = Identifier.of(column.getColumnName());
      if (identifier != null) {
        addIfNotBlank(keys, identifier.text());
      }
      String columnName = JParqUtil.normalizeQualifier(column.getColumnName());
      if (columnName != null) {
        keys.add(columnName);
        addTableQualifiers(keys, columnName, column.getTable());
      }
    }
    return List.copyOf(keys);
  }

  /**
   * Recognizes both parser representations of COUNT(*).
   *
   * @param function
   *          aggregate function to inspect
   * @return whether the call uses a star argument
   */
  private static boolean isCountStar(Function function) {
    List<Expression> args = parameters(function);
    return function.isAllColumns() || args.size() == 1 && args.getFirst() instanceof AllColumns;
  }

  /**
   * Build an aggregate specification from a function expression.
   *
   * @param func
   *          function expression to analyze
   * @param label
   *          projection label for the aggregate
   * @return aggregate specification or {@code null} when the function is not a
   *         supported aggregate
   */
  public static AggregateSpec aggregateSpec(Function func, String label) {
    AggregateType type = AggregateType.from(func.getName());
    if (type == null) {
      return null;
    }
    if (func.isDistinct()) {
      throw new IllegalArgumentException("DISTINCT aggregates are not supported yet");
    }

    boolean countStar = false;
    List<Expression> args = List.of();

    if (type == AggregateType.COUNT && isCountStar(func)) {
      countStar = true;
    } else {
      args = parameters(func);
      if (args.isEmpty()) {
        throw new IllegalArgumentException("Aggregate function " + func + " requires an argument");
      }
      if (type == AggregateType.STRING_AGG) {
        if (args.size() != 2) {
          throw new IllegalArgumentException("STRING_AGG requires two arguments (value, separator)");
        }
      } else if (args.size() != 1) {
        throw new IllegalArgumentException("Only single-argument aggregate functions are supported");
      }
    }

    if (args.stream().anyMatch(argument -> !aggregateFunctions(argument).isEmpty())) {
      throw syntaxFailure("Nested aggregate functions are not supported: " + func);
    }
    return new AggregateSpec(type, args, label, countStar);
  }

  /**
   * Build a lookup map associating normalized group expression text with the
   * index of the expression in the GROUP BY clause.
   *
   * @param groupExpressions
   *          grouping expressions defined for the SELECT
   * @return immutable lookup map from expression text to index
   */
  private static Map<String, Integer> groupIndexLookup(List<GroupExpression> groupExpressions) {
    Map<String, Integer> lookup = new HashMap<>();
    for (int i = 0; i < groupExpressions.size(); i++) {
      Expression expression = groupExpressions.get(i).expression();
      for (String key : expressionKeys(expression)) {
        if (key != null) {
          lookup.putIfAbsent(key, i);
        }
      }
    }
    return lookup;
  }

  /**
   * Create {@link GroupingSet} instances from the parsed grouping set definition.
   *
   * @param rawGroupingSets
   *          grouping set definitions expressed as expression indexes
   * @param expressionCount
   *          number of grouping expressions available in the SELECT
   * @param containsAggregate
   *          {@code true} when the SELECT list contains aggregate expressions
   * @return immutable list of grouping sets
   */
  private static List<GroupingSet> buildGroupingSets(List<List<Integer>> rawGroupingSets, int expressionCount,
      boolean containsAggregate) {
    List<List<Integer>> groupingSets = rawGroupingSets == null ? List.of() : rawGroupingSets;
    if (groupingSets.isEmpty()) {
      if (expressionCount == 0) {
        return containsAggregate ? List.of(new GroupingSet(List.of())) : List.of();
      }
      List<Integer> indexes = new ArrayList<>(expressionCount);
      for (int i = 0; i < expressionCount; i++) {
        indexes.add(i);
      }
      return List.of(new GroupingSet(indexes));
    }
    List<GroupingSet> result = new ArrayList<>(groupingSets.size());
    for (List<Integer> groupingSet : groupingSets) {
      List<Integer> indexes = new ArrayList<>();
      if (groupingSet != null) {
        for (Integer idx : groupingSet) {
          if (idx == null) {
            throw new IllegalArgumentException("Grouping set index cannot be null");
          }
          if (idx < 0 || idx >= expressionCount) {
            throw new IllegalArgumentException("Grouping set index " + idx + " is out of bounds");
          }
          indexes.add(idx);
        }
      }
      result.add(new GroupingSet(indexes));
    }
    return List.copyOf(result);
  }

  /**
   * Determine whether a function represents the SQL GROUPING function.
   *
   * @param func
   *          function expression to test
   * @return {@code true} if the function is GROUPING, otherwise {@code false}
   */
  private static boolean isGroupingFunction(Function func) {
    return func != null && "GROUPING".equalsIgnoreCase(func.getName());
  }

  /**
   * Resolve the grouping expression indexes referenced by a GROUPING function
   * invocation.
   *
   * @param func
   *          GROUPING function expression
   * @param groupIndexByText
   *          lookup of grouping expression indexes by normalized text
   * @param groupExpressions
   *          expressions present in the GROUP BY clause used for structural
   *          matching
   * @return immutable list of grouping expression indexes
   */
  private static List<Integer> groupingArgumentIndexes(Function func, Map<String, Integer> groupIndexByText,
      List<GroupExpression> groupExpressions) {
    ExpressionList<?> parameters = func.getParameters();
    if (parameters == null || parameters.isEmpty()) {
      throw new IllegalArgumentException("GROUPING function requires at least one argument");
    }
    List<Integer> indexes = new ArrayList<>(parameters.size());
    for (Expression argument : parameters) {
      Integer match = findGroupIndex(argument, groupIndexByText, groupExpressions);
      if (match == null) {
        throw new IllegalArgumentException("GROUPING argument '" + argument + "' must appear in the GROUP BY clause");
      }
      indexes.add(match);
    }
    return List.copyOf(indexes);
  }

  /**
   * Locate the index of an expression within the GROUP BY clause.
   *
   * @param expression
   *          expression to match
   * @param groupIndexByText
   *          lookup keyed by normalized expression text
   * @param groupExpressions
   *          expressions defined in the GROUP BY clause
   * @return matching index or {@code null} when no match exists
   */
  private static Integer findGroupIndex(Expression expression, Map<String, Integer> groupIndexByText,
      List<GroupExpression> groupExpressions) {
    if (expression == null) {
      return null;
    }
    for (String key : expressionKeys(expression)) {
      if (key == null) {
        continue;
      }
      Integer index = groupIndexByText.get(key);
      if (index != null) {
        return index;
      }
    }
    for (int i = 0; i < groupExpressions.size(); i++) {
      if (expressionsEquivalent(expression, groupExpressions.get(i).expression())) {
        return i;
      }
    }
    return null;
  }

  private static String labelFor(List<String> labels, int index, Expression expr) {
    if (labels != null && index < labels.size()) {
      String label = labels.get(index);
      if (label != null && !label.isBlank()) {
        return label;
      }
    }
    return expr == null ? "" : expr.toString();
  }

  private static List<Function> aggregateFunctions(Expression expression) {
    if (expression == null) {
      return List.of();
    }
    List<Function> functions = new ArrayList<>();
    expression.accept(new JSqlParserSupport.NamedArgumentVisitor() {
      @Override
      public <S> Void visit(Function function, S context) {
        if (AggregateType.from(function.getName()) != null) {
          functions.add(function);
        }
        return super.visit(function, context);
      }

      @Override
      public <S> Void visit(ParenthesedSelect select, S context) {
        // Aggregates inside subqueries are evaluated independently and should
        // not be registered in the outer aggregate plan.
        return null;
      }

      @Override
      public <S> Void visit(Select select, S context) {
        // Aggregates inside subqueries are evaluated independently and should
        // not be registered in the outer aggregate plan.
        return null;
      }
    });
    return functions;
  }

  private static boolean containsEquivalentAggregate(List<AggregateSpec> specs, AggregateSpec candidate) {
    for (AggregateSpec spec : specs) {
      if (spec.type() != candidate.type()) {
        continue;
      }
      if (spec.countStar() != candidate.countStar()) {
        continue;
      }
      List<Expression> args = spec.arguments();
      List<Expression> otherArgs = candidate.arguments();
      if (args.size() != otherArgs.size()) {
        continue;
      }
      boolean matches = true;
      for (int i = 0; i < args.size(); i++) {
        if (!Objects.equals(args.get(i).toString(), otherArgs.get(i).toString())) {
          matches = false;
          break;
        }
      }
      if (matches) {
        return true;
      }
    }
    return false;
  }

  /**
   * Compare two Expression to see if they are equivalent or not.
   *
   * @param first
   *          the left Expression to compare
   * @param second
   *          the right Expression to compare
   * @return true if they are equivalent, otherwise false
   */
  public static boolean expressionsEquivalent(Expression first, Expression second) {
    if (first == second) {
      return true;
    }
    if (first == null || second == null) {
      return false;
    }
    if (!first.getClass().equals(second.getClass())) {
      return false;
    }
    if (first instanceof Column leftColumn) {
      return columnsEquivalent(leftColumn, (Column) second);
    }
    if (first instanceof Function leftFunction) {
      return functionsEquivalent(leftFunction, (Function) second);
    }
    if (first instanceof BinaryExpression leftBinary) {
      return binaryExpressionsEquivalent(leftBinary, (BinaryExpression) second);
    }
    if (first instanceof Between leftBetween) {
      Between rightBetween = (Between) second;
      return leftBetween.isNot() == rightBetween.isNot()
          && expressionsEquivalent(leftBetween.getLeftExpression(), rightBetween.getLeftExpression())
          && expressionsEquivalent(leftBetween.getBetweenExpressionStart(), rightBetween.getBetweenExpressionStart())
          && expressionsEquivalent(leftBetween.getBetweenExpressionEnd(), rightBetween.getBetweenExpressionEnd());
    }
    if (first instanceof InExpression leftIn) {
      InExpression rightIn = (InExpression) second;
      if (leftIn.isNot() != rightIn.isNot()) {
        return false;
      }
      if (!expressionsEquivalent(leftIn.getLeftExpression(), rightIn.getLeftExpression())) {
        return false;
      }
      return expressionsEquivalent(leftIn.getRightExpression(), rightIn.getRightExpression());
    }
    if (first instanceof IsNullExpression leftIsNull) {
      IsNullExpression rightIsNull = (IsNullExpression) second;
      return leftIsNull.isNot() == rightIsNull.isNot()
          && expressionsEquivalent(leftIsNull.getLeftExpression(), rightIsNull.getLeftExpression());
    }
    if (first instanceof CaseExpression leftCase) {
      CaseExpression rightCase = (CaseExpression) second;
      if (!expressionsEquivalent(leftCase.getSwitchExpression(), rightCase.getSwitchExpression())) {
        return false;
      }
      Expression leftElse = leftCase.getElseExpression();
      Expression rightElse = rightCase.getElseExpression();
      if (!expressionsEquivalent(leftElse, rightElse)) {
        return false;
      }
      List<WhenClause> leftClauses = leftCase.getWhenClauses();
      List<WhenClause> rightClauses = rightCase.getWhenClauses();
      if (leftClauses == null || rightClauses == null) {
        return leftClauses == rightClauses;
      }
      if (leftClauses.size() != rightClauses.size()) {
        return false;
      }
      for (int i = 0; i < leftClauses.size(); i++) {
        if (!expressionsEquivalent(leftClauses.get(i), rightClauses.get(i))) {
          return false;
        }
      }
      return true;
    }
    if (first instanceof WhenClause leftWhen) {
      WhenClause rightWhen = (WhenClause) second;
      return expressionsEquivalent(leftWhen.getWhenExpression(), rightWhen.getWhenExpression())
          && expressionsEquivalent(leftWhen.getThenExpression(), rightWhen.getThenExpression());
    }
    if (first instanceof SignedExpression leftSigned) {
      SignedExpression rightSigned = (SignedExpression) second;
      return leftSigned.getSign() == rightSigned.getSign()
          && expressionsEquivalent(leftSigned.getExpression(), rightSigned.getExpression());
    }
    if (first instanceof NotExpression leftNot) {
      return expressionsEquivalent(leftNot.getExpression(), ((NotExpression) second).getExpression());
    }
    if (first instanceof ParenthesedExpressionList<?> leftParenthesizedExpressionList) {
      ParenthesedExpressionList<?> rightParenthesizedExpressionList = (ParenthesedExpressionList<?>) second;
      if (leftParenthesizedExpressionList.size() != rightParenthesizedExpressionList.size()) {
        return false;
      }
      for (int i = 0; i < leftParenthesizedExpressionList.size(); i++) {
        if (!expressionsEquivalent((Expression) leftParenthesizedExpressionList.get(i),
            (Expression) rightParenthesizedExpressionList.get(i))) {
          return false;
        }
      }
      return true;
    }
    if (LegacyParenthesisSupport.isLegacyParenthesis(first)) {
      return expressionsEquivalent(LegacyParenthesisSupport.extract(first), LegacyParenthesisSupport.extract(second));
    }
    if (first instanceof CastExpression leftCast) {
      CastExpression rightCast = (CastExpression) second;
      if (!Objects.equals(String.valueOf(leftCast.getColDataType()), String.valueOf(rightCast.getColDataType()))) {
        return false;
      }
      return expressionsEquivalent(leftCast.getLeftExpression(), rightCast.getLeftExpression());
    }
    if (first instanceof NullValue) {
      return true;
    }
    if (first instanceof StringValue leftString) {
      return Objects.equals(LiteralConverter.decodeStringLiteral(leftString),
          LiteralConverter.decodeStringLiteral((StringValue) second));
    }
    if (first instanceof DateValue leftDate) {
      return Objects.equals(leftDate.getValue(), ((DateValue) second).getValue());
    }
    if (first instanceof TimeValue leftTime) {
      return Objects.equals(leftTime.getValue(), ((TimeValue) second).getValue());
    }
    if (first instanceof TimestampValue leftTimestamp) {
      return Objects.equals(leftTimestamp.getValue(), ((TimestampValue) second).getValue());
    }
    if (first instanceof JdbcNamedParameter leftNamed) {
      return Objects.equals(leftNamed.getName(), ((JdbcNamedParameter) second).getName());
    }
    if (first instanceof JdbcParameter leftParam) {
      return Objects.equals(leftParam.getIndex(), ((JdbcParameter) second).getIndex());
    }
    return Objects.equals(first.toString(), second.toString());
  }

  private static boolean binaryExpressionsEquivalent(BinaryExpression left, BinaryExpression right) {
    if (left instanceof LikeExpression leftLike) {
      LikeExpression rightLike = (LikeExpression) right;
      if (leftLike.isNot() != rightLike.isNot()) {
        return false;
      }
      // Compare ESCAPE clauses
      if (!expressionsEquivalent(leftLike.getEscape(), rightLike.getEscape())) {
        return false;
      }
    }
    if (left instanceof SimilarToExpression leftSimilarTo) {
      SimilarToExpression rightSimilarTo = (SimilarToExpression) right;
      if (leftSimilarTo.isNot() != rightSimilarTo.isNot()) {
        return false;
      }
      // Compare ESCAPE strings
      if (!Objects.equals(leftSimilarTo.getEscape(), rightSimilarTo.getEscape())) {
        return false;
      }
    }
    return expressionsEquivalent(left.getLeftExpression(), right.getLeftExpression())
        && expressionsEquivalent(left.getRightExpression(), right.getRightExpression());
  }

  /**
   * Compare two columns to see if they are equivalent or not.
   *
   * @param left
   *          the left column to compare
   * @param right
   *          the right column to compare
   * @return true if they are equivalent, otherwise false
   */
  public static boolean columnsEquivalent(Column left, Column right) {
    if (left == null || right == null) {
      return left == right;
    }
    String leftName = JParqUtil.normalizeQualifier(left.getColumnName());
    String rightName = JParqUtil.normalizeQualifier(right.getColumnName());
    if (!Objects.equals(leftName, rightName)) {
      return false;
    }
    return tablesEquivalent(left.getTable(), right.getTable());
  }

  /**
   * Compare two Tables to see if they are equivalent or not.
   *
   * @param left
   *          the left Table to compare
   * @param right
   *          the right Table to compare
   * @return true if they are equivalent, otherwise false
   */
  public static boolean tablesEquivalent(Table left, Table right) {
    if (left == right) {
      return true;
    }
    if (left == null || right == null) {
      return false;
    }
    if (!Objects.equals(tableAlias(left), tableAlias(right))) {
      return false;
    }
    return Objects.equals(tableName(left), tableName(right));
  }

  /**
   * Compare two Function to see if they are equivalent or not.
   *
   * @param left
   *          the left Function to compare
   * @param right
   *          the right Function to compare
   * @return true if they are equivalent, otherwise false
   */
  public static boolean functionsEquivalent(Function left, Function right) {
    if (!Objects.equals(normalizeFunctionName(left.getName()), normalizeFunctionName(right.getName()))) {
      return false;
    }
    if (left.isAllColumns() != right.isAllColumns()) {
      return false;
    }
    if (left.isDistinct() != right.isDistinct()) {
      return false;
    }
    List<Expression> leftArgs = parameters(left);
    List<Expression> rightArgs = parameters(right);
    if (leftArgs.size() != rightArgs.size()) {
      return false;
    }
    for (int i = 0; i < leftArgs.size(); i++) {
      if (!expressionsEquivalent(leftArgs.get(i), rightArgs.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static String normalizeFunctionName(String name) {
    return name == null ? null : name.toUpperCase(Locale.ROOT);
  }

  private static List<GroupExpression> buildGroupExpressions(List<Expression> groupByExpressions) {
    if (groupByExpressions == null || groupByExpressions.isEmpty()) {
      return List.of();
    }
    List<GroupExpression> groups = new ArrayList<>(groupByExpressions.size());
    for (Expression expr : groupByExpressions) {
      String label;
      if (expr instanceof Column column) {
        label = column.getColumnName();
      } else {
        label = expr.toString();
      }
      groups.add(new GroupExpression(expr, label));
    }
    return groups;
  }

  private static List<Expression> parameters(Function func) {
    ExpressionList<?> list = func.getParameters();
    if (list == null || list.isEmpty()) {
      return List.of();
    }
    List<Expression> params = new ArrayList<>(list.size());
    params.addAll(list);
    return params;
  }

  /**
   * Adds a rendered expression to the key set if it is not blank.
   *
   * @param keys
   *          the accumulator of keys, never {@code null}
   * @param rendered
   *          the expression rendered as text, may be {@code null}
   */
  private static void addIfNotBlank(Set<String> keys, String rendered) {
    if (rendered != null && !rendered.isBlank()) {
      keys.add(rendered);
    }
  }

  /**
   * Adds table-qualified column names to the key set when a table reference
   * exists.
   *
   * @param keys
   *          the accumulator of keys, never {@code null}
   * @param columnName
   *          the normalized column name, never {@code null}
   * @param table
   *          the table reference that may contribute qualifiers
   */
  private static void addTableQualifiers(Set<String> keys, String columnName, Table table) {
    if (table == null) {
      return;
    }
    String alias = tableAlias(table);
    addQualifiedIfPresent(keys, alias, columnName);
    String tableName = tableName(table);
    addQualifiedIfPresent(keys, tableName, columnName);
  }

  /**
   * Adds a qualified key to the set when the qualifier is present.
   *
   * @param keys
   *          the accumulator of keys, never {@code null}
   * @param qualifier
   *          the qualifier to prefix, may be {@code null}
   * @param columnName
   *          the normalized column name, never {@code null}
   */
  private static void addQualifiedIfPresent(Set<String> keys, String qualifier, String columnName) {
    if (qualifier != null) {
      keys.add(qualifier + "." + columnName);
    }
  }

  /**
   * Normalizes the alias of a table when available.
   *
   * @param table
   *          the table that may contain an alias
   * @return the normalized alias or {@code null}
   */
  private static String tableAlias(Table table) {
    if (table.getAlias() == null || table.getAlias().getName() == null) {
      return null;
    }
    return JParqUtil.normalizeQualifier(table.getAlias().getName());
  }

  /**
   * Normalizes the table name when available.
   *
   * @param table
   *          the table containing the name
   * @return the normalized table name or {@code null}
   */
  private static String tableName(Table table) {
    if (table.getName() == null) {
      return null;
    }
    return JParqUtil.normalizeQualifier(table.getName());
  }

  /**
   * Evaluate aggregates and grouping by streaming the Parquet reader.
   *
   * @param reader
   *          parquet reader
   * @param plan
   *          aggregate plan
   * @param residual
   *          residual WHERE expression (may be null)
   * @param having
   *          HAVING expression to evaluate after aggregation (may be null)
   * @param orderBy
   *          ORDER BY clauses to apply after aggregation (may be empty)
   * @param subqueryExecutor
   *          executor used to evaluate subqueries referenced by the aggregate
   *          expressions
   * @param outerQualifiers
   *          table names or aliases from the outer query scope used when
   *          resolving correlated subquery references
   * @param qualifierColumnMapping
   *          mapping from qualifier to canonical column names (may be empty)
   * @param unqualifiedColumnMapping
   *          mapping from unqualified column names to canonical field names
   * @return aggregate rows and associated column metadata
   * @throws IOException
   *           if reading the records fails
   */
  public static AggregateResult evaluate(RecordReader reader, AggregatePlan plan, Expression residual,
      Expression having, List<OrderKey> orderBy, SubqueryExecutor subqueryExecutor, List<String> outerQualifiers,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping)
      throws IOException {
    return evaluate(reader, plan, residual, having, orderBy, subqueryExecutor, outerQualifiers, qualifierColumnMapping,
        unqualifiedColumnMapping, qualifierColumnMapping);
  }

  /**
   * Evaluate the supplied aggregation plan using the provided records.
   *
   * @param reader
   *          source of {@link GenericRecord} rows
   * @param plan
   *          aggregate plan describing grouping, aggregates and projection labels
   * @param residual
   *          residual WHERE clause to apply before aggregation
   * @param having
   *          HAVING clause to apply after aggregation
   * @param orderBy
   *          ORDER BY keys used after aggregation
   * @param subqueryExecutor
   *          executor used for correlated subqueries
   * @param outerQualifiers
   *          qualifiers visible to correlated subqueries
   * @param qualifierColumnMapping
   *          mapping from qualifier to canonical column names used when resolving
   *          correlated subquery references
   * @param unqualifiedColumnMapping
   *          mapping from unqualified column names to canonical field names
   * @param correlationContext
   *          correlation context describing qualifier aware column aliases
   * @return aggregate rows and associated column metadata
   * @throws IOException
   *           if reading the records fails
   */
  public static AggregateResult evaluate(RecordReader reader, AggregatePlan plan, Expression residual,
      Expression having, List<OrderKey> orderBy, SubqueryExecutor subqueryExecutor, List<String> outerQualifiers,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping,
      Map<String, Map<String, String>> correlationContext) throws IOException {
    List<GroupExpression> groupExpressions = plan.groupExpressions();
    final List<GroupingSet> groupingSets = plan.groupingSets();
    List<GroupTypeTracker> groupTrackers = new ArrayList<>(groupExpressions.size());
    for (int i = 0; i < groupExpressions.size(); i++) {
      groupTrackers.add(new GroupTypeTracker());
    }

    final Map<String, Map<String, String>> normalizedCorrelationContext = ColumnMappingUtil
        .normaliseQualifierMapping(correlationContext);

    Map<Integer, GroupedScalarSubquery> scalarSubqueries = new HashMap<>();
    Map<Integer, GroupTypeTracker> scalarTypes = new HashMap<>();
    for (int index = 0; index < plan.resultColumns().size(); index++) {
      ResultColumn resultColumn = plan.resultColumns().get(index);
      if (resultColumn.kind() == ColumnKind.EXPRESSION) {
        scalarTypes.put(index, new GroupTypeTracker());
      }
      Select subquery = resultColumn.subquery();
      if (subquery != null) {
        Objects.requireNonNull(subqueryExecutor, "Scalar subqueries require a subquery executor");
        scalarSubqueries.put(index, new GroupedScalarSubquery(subquery, groupExpressions, subqueryExecutor,
            CorrelationMappings.of(outerQualifiers, qualifierColumnMapping, unqualifiedColumnMapping)));
        scalarTypes.put(index, new GroupTypeTracker());
      }
    }

    Map<String, GroupedScalarSubquery> groupedSubqueries = new HashMap<>();
    Map<Integer, Expression> projectionExpressions = new HashMap<>();
    for (int index = 0; index < plan.resultColumns().size(); index++) {
      ResultColumn column = plan.resultColumns().get(index);
      if (column.kind() == ColumnKind.EXPRESSION) {
        projectionExpressions.put(index, parseGroupedExpression(column.expressionText()));
      }
    }
    List<Expression> groupedScalars = new ArrayList<>(projectionExpressions.values());
    groupedScalars.add(having);
    if (orderBy != null) {
      orderBy.forEach(key -> groupedScalars.add(key.expression()));
    }
    for (Expression expression : groupedScalars) {
      if (expression != null) {
        expression.accept(new JSqlParserSupport.NamedArgumentVisitor() {
          @Override
          public <S> Void visit(Function function, S context) {
            return isAggregateName(function.getName()) ? null : super.visit(function, context);
          }

          @Override
          public <S> Void visit(ParenthesedSelect select, S context) {
            return visit((Select) select, context);
          }

          @Override
          public <S> Void visit(AnyComparisonExpression comparison, S context) {
            return visit(comparison.getSelect(), context);
          }

          @Override
          public <S> Void visit(Select select, S context) {
            Objects.requireNonNull(subqueryExecutor, "Scalar subqueries require a subquery executor");
            if (expression == having) {
              Set<String> aliases = plan.resultColumns().stream().map(column -> Identifier.lookupKey(column.label()))
                  .collect(Collectors.toSet());
              GroupedScalarSubquery.validateCorrelations(select, groupExpressions, subqueryExecutor,
                  CorrelationMappings.of(outerQualifiers, qualifierColumnMapping, unqualifiedColumnMapping), aliases);
              return null;
            }
            groupedSubqueries.computeIfAbsent(select.toString(),
                text -> new GroupedScalarSubquery(select, groupExpressions, subqueryExecutor,
                    CorrelationMappings.of(outerQualifiers, qualifierColumnMapping, unqualifiedColumnMapping)));
            return null;
          }
        }, null);
      }
    }
    int aggregateCount = plan.specs().size();
    int[] aggregateSqlTypes = new int[aggregateCount];
    for (int i = 0; i < aggregateCount; i++) {
      aggregateSqlTypes[i] = AggregateAccumulator.create(plan.specs().get(i)).sqlType();
    }

    Map<GroupKey, GroupState> states = new LinkedHashMap<>();
    Predicate<Column> booleanColumn = column -> false;

    try (RecordReader autoClose = reader) {
      GenericRecord rec = autoClose.read();
      Schema schema = null;
      ExpressionEvaluator whereEval = null;
      ValueExpressionEvaluator valueEval = null;

      while (rec != null) {
        if (schema == null) {
          schema = rec.getSchema();
          if (residual != null) {
            whereEval = new ExpressionEvaluator(schema, subqueryExecutor, outerQualifiers, qualifierColumnMapping,
                unqualifiedColumnMapping);
          }
          if (having != null) {
            booleanColumn = booleanColumnPredicate(schema, qualifierColumnMapping, unqualifiedColumnMapping);
          }
          CorrelationMappings mappings = new CorrelationMappings(outerQualifiers, qualifierColumnMapping,
              unqualifiedColumnMapping, normalizedCorrelationContext);
          valueEval = new ValueExpressionEvaluator(schema, subqueryExecutor, mappings, WindowState.empty());
        }

        boolean matches = residual == null || whereEval.eval(residual, rec);
        if (matches) {
          List<Object> groupValues = evaluateGroupValues(groupExpressions, valueEval, rec, groupTrackers);
          for (int setIndex = 0; setIndex < groupingSets.size(); setIndex++) {
            GroupingSet groupingSet = groupingSets.get(setIndex);
            List<Object> keyValues = keyValuesForGroupingSet(groupValues, groupingSet);
            GroupKey key = new GroupKey(setIndex, keyValues);
            GroupState state = states.get(key);
            if (state == null) {
              List<Object> projected = projectGroupValuesForSet(groupValues, groupingSet, groupExpressions.size());
              state = new GroupState(projected, plan.specs(), setIndex);
              states.put(key, state);
            }
            state.add(valueEval, rec);
          }
        }

        rec = autoClose.read();
      }
    }

    if (states.isEmpty() && groupExpressions.isEmpty()) {
      for (int i = 0; i < groupingSets.size(); i++) {
        GroupState state = new GroupState(List.of(), plan.specs(), i);
        states.put(new GroupKey(i, List.of()), state);
      }
    }

    boolean[] aggregateTypeObserved = new boolean[aggregateCount];
    for (GroupState state : states.values()) {
      for (int i = 0; i < aggregateCount; i++) {
        if (!aggregateTypeObserved[i]) {
          aggregateSqlTypes[i] = state.accumulator(i).sqlType();
          aggregateTypeObserved[i] = true;
        }
      }
    }

    List<Integer> groupSqlTypes = new ArrayList<>(groupTrackers.size());
    for (GroupTypeTracker tracker : groupTrackers) {
      groupSqlTypes.add(tracker.sqlType());
    }

    Map<String, GroupedExpression> scalarExpressions = new HashMap<>();
    List<AggregatedRow> rows = new ArrayList<>();
    Expression normalizedHaving = ExpressionEvaluator.unwrapParenthesis(having);

    for (GroupState state : states.values()) {
      List<Object> aggregateValues = state.results();
      List<Object> row = buildRow(plan, state, aggregateValues, scalarSubqueries, projectionExpressions,
          scalarExpressions, subqueryExecutor, outerQualifiers, normalizedCorrelationContext, groupedSubqueries);
      boolean include = true;
      if (normalizedHaving != null) {
        HavingEvaluator evaluator = new HavingEvaluator(plan, aggregateValues,
            buildLabelLookup(plan, row, state.groupValues()), subqueryExecutor, outerQualifiers,
            normalizedCorrelationContext, booleanColumn,
            new GroupEvaluation(state.groupValues(), state.groupingSetIndex(), scalarExpressions, Map.of()));
        include = evaluator.eval(normalizedHaving);
      }
      if (include) {
        for (var entry : scalarTypes.entrySet()) {
          int index = entry.getKey();
          Object value = resolveScalarValue(row.get(index));
          row.set(index, value);
          entry.getValue().track(value);
        }
        rows.add(new AggregatedRow(row, state.groupValues(), aggregateValues, state.groupingSetIndex()));
      }
    }

    if (orderBy != null && !orderBy.isEmpty()) {
      sortAggregatedRows(rows, plan, orderBy, new ScalarExecution(subqueryExecutor, outerQualifiers,
          normalizedCorrelationContext, scalarExpressions, groupedSubqueries));
    }

    List<Integer> columnSqlTypes = new ArrayList<>(plan.resultColumns().size());
    for (int index = 0; index < plan.resultColumns().size(); index++) {
      ResultColumn column = plan.resultColumns().get(index);
      if (column.isAggregate()) {
        columnSqlTypes.add(aggregateSqlTypes[column.aggregateIndex()]);
      } else if (column.isGrouping()) {
        columnSqlTypes.add(Types.INTEGER);
      } else if (column.subquery() != null) {
        int observed = scalarTypes.get(index).sqlType();
        columnSqlTypes.add(observed == Types.OTHER ? scalarSubqueries.get(index).declaredSqlType() : observed);
      } else if (column.kind() == ColumnKind.EXPRESSION) {
        columnSqlTypes.add(scalarTypes.get(index).sqlType());
      } else {
        columnSqlTypes.add(groupSqlTypes.isEmpty() ? Types.OTHER : groupSqlTypes.get(column.groupIndex()));
      }
    }

    List<List<Object>> projectedRows = rows.stream().map(AggregatedRow::rowValues).toList();

    return new AggregateResult(List.copyOf(projectedRows), List.copyOf(columnSqlTypes));
  }

  /**
   * Evaluates sort keys once per group, then sorts using the cached values.
   *
   * @param rows
   *          completed groups to sort
   * @param plan
   *          aggregate plan
   * @param orderBy
   *          ordering expressions
   * @param execution
   *          scalar execution context
   */
  private static void sortAggregatedRows(List<AggregatedRow> rows, AggregatePlan plan, List<OrderKey> orderBy,
      ScalarExecution execution) {
    Map<String, OrderValueAccessor> accessorByColumn = buildOrderAccessors(plan);
    List<OrderValueAccessor> accessors = new ArrayList<>(orderBy.size());
    for (OrderKey key : orderBy) {
      OrderValueAccessor accessor = accessorByColumn.get(Identifier.lookupKey(key.column()));
      if (accessor == null && key.expression() == null) {
        throw syntaxFailure(
            "ORDER BY column '" + key.column() + "' is not present in the SELECT list or grouping expressions");
      }
      accessors.add(accessor);
    }
    boolean needsEvaluator = accessors.contains(null);
    Map<AggregatedRow, List<Object>> sortKeys = new HashMap<>();
    for (AggregatedRow row : rows) {
      HavingEvaluator evaluator = needsEvaluator
          ? new HavingEvaluator(plan, row.aggregateValues(), buildLabelLookup(plan, row.rowValues(), row.groupValues()),
              execution.subqueryExecutor(), execution.outerQualifiers(), execution.correlationContext(),
              column -> false,
              new GroupEvaluation(row.groupValues(), row.groupingSetIndex(), execution.scalarExpressions(),
                  execution.subqueries()))
          : null;
      List<Object> values = new ArrayList<>(orderBy.size());
      for (int index = 0; index < orderBy.size(); index++) {
        OrderValueAccessor accessor = accessors.get(index);
        values.add(accessor == null ? evaluator.value(orderBy.get(index).expression()) : accessor.value(row));
      }
      sortKeys.put(row, values);
    }
    Comparator<AggregatedRow> comparator = (left, right) -> {
      for (int index = 0; index < orderBy.size(); index++) {
        OrderKey key = orderBy.get(index);
        Object lv = sortKeys.get(left).get(index);
        Object rv = sortKeys.get(right).get(index);
        boolean hasNull = lv == null || rv == null;
        int nullCmp = OrderingUtil.compareNulls(lv, rv, key.asc(), key.nullOrdering());
        if (nullCmp != 0) {
          return nullCmp;
        }
        if (hasNull) {
          continue;
        }
        int cmp = SqlComparisons.typedCompare(lv, rv);
        if (cmp != 0) {
          return key.asc() ? cmp : -cmp;
        }
      }
      return 0;
    };
    rows.sort(comparator);
  }

  /**
   * Parses an expression stored in aggregate projection metadata.
   *
   * @param text
   *          SQL expression text
   * @return parsed expression
   * @throws IllegalArgumentException
   *           if parsing fails
   */
  private static Expression parseGroupedExpression(String text) {
    try {
      return net.sf.jsqlparser.parser.CCJSqlParserUtil.parseExpression(text);
    } catch (net.sf.jsqlparser.JSQLParserException e) {
      throw new IllegalArgumentException("Invalid grouped expression: " + text, e);
    }
  }

  private static Map<String, OrderValueAccessor> buildOrderAccessors(AggregatePlan plan) {
    Map<String, OrderValueAccessor> mapping = new HashMap<>();
    List<ResultColumn> columns = plan.resultColumns();
    for (int i = 0; i < columns.size(); i++) {
      ResultColumn column = columns.get(i);
      registerOrderAccessor(mapping, column.label(), OrderValueAccessor.projection(i));
      if (column.isAggregate()) {
        AggregateSpec spec = plan.specs().get(column.aggregateIndex());
        OrderValueAccessor aggregateAccessor = OrderValueAccessor.aggregate(column.aggregateIndex());
        registerOrderAccessor(mapping, spec.label(), aggregateAccessor);
        registerOrderAccessor(mapping, aggregateExpressionText(spec), aggregateAccessor);
      } else if (column.isGrouping() || column.kind() == ColumnKind.EXPRESSION) {
        registerOrderAccessor(mapping, column.expressionText(), OrderValueAccessor.projection(i));
      } else if (column.subquery() != null) {
        registerOrderAccessor(mapping, column.subquery().toString(), OrderValueAccessor.projection(i));
        registerOrderAccessor(mapping, "(" + column.subquery() + ")", OrderValueAccessor.projection(i));
      } else if (column.isGroup()) {
        GroupExpression group = plan.groupExpressions().get(column.groupIndex());
        OrderValueAccessor groupAccessor = OrderValueAccessor.groupValue(column.groupIndex());
        registerOrderAccessor(mapping, group.label(), groupAccessor);
        registerOrderAccessor(mapping, group.expression().toString(), groupAccessor);
      }
    }
    List<GroupExpression> groups = plan.groupExpressions();
    for (int i = 0; i < groups.size(); i++) {
      GroupExpression group = groups.get(i);
      registerOrderAccessor(mapping, group.label(), OrderValueAccessor.groupValue(i));
      registerOrderAccessor(mapping, group.expression().toString(), OrderValueAccessor.groupValue(i));
    }
    registerGroupingFunctionAccessors(mapping, plan);
    return mapping;
  }

  private static void registerGroupingFunctionAccessors(Map<String, OrderValueAccessor> mapping, AggregatePlan plan) {
    List<GroupExpression> groups = plan.groupExpressions();
    for (int i = 0; i < groups.size(); i++) {
      GroupExpression group = groups.get(i);
      OrderValueAccessor accessor = OrderValueAccessor.groupingFunction(List.of(i), plan);
      registerOrderAccessor(mapping, "GROUPING(" + group.label() + ")", accessor);
      registerOrderAccessor(mapping, "GROUPING(" + group.expression() + ")", accessor);
    }
  }

  private static void registerOrderAccessor(Map<String, OrderValueAccessor> mapping, String key,
      OrderValueAccessor accessor) {
    if (key == null || key.isBlank() || accessor == null) {
      return;
    }
    String normalized = Identifier.lookupKey(key);
    if (normalized != null) {
      mapping.putIfAbsent(normalized, accessor);
    }
  }

  private enum OrderValueKind {
    PROJECTION, GROUP, AGGREGATE, GROUPING_FUNCTION
  }

  private static final class OrderValueAccessor {
    private final OrderValueKind kind;
    private final int index;
    private final List<Integer> groupingIndexes;
    private final AggregatePlan plan;

    private OrderValueAccessor(OrderValueKind kind, int index, List<Integer> groupingIndexes, AggregatePlan plan) {
      this.kind = kind;
      this.index = index;
      this.groupingIndexes = groupingIndexes == null ? List.of() : groupingIndexes;
      this.plan = plan;
    }

    static OrderValueAccessor projection(int index) {
      return new OrderValueAccessor(OrderValueKind.PROJECTION, index, List.of(), null);
    }

    static OrderValueAccessor groupValue(int index) {
      return new OrderValueAccessor(OrderValueKind.GROUP, index, List.of(), null);
    }

    static OrderValueAccessor aggregate(int index) {
      return new OrderValueAccessor(OrderValueKind.AGGREGATE, index, List.of(), null);
    }

    static OrderValueAccessor groupingFunction(List<Integer> indexes, AggregatePlan plan) {
      return new OrderValueAccessor(OrderValueKind.GROUPING_FUNCTION, -1, indexes, plan);
    }

    Object value(AggregatedRow row) {
      return switch (kind) {
        case PROJECTION -> row.rowValues().get(index);
        case GROUP -> row.groupValues().get(index);
        case AGGREGATE -> row.aggregateValues().get(index);
        case GROUPING_FUNCTION -> groupingValue(plan, row.groupingSetIndex(), groupingIndexes);
      };
    }
  }

  private static final class AggregatedRow {
    private final List<Object> rowValues;
    private final List<Object> groupValues;
    private final List<Object> aggregateValues;
    private final int groupingSetIndex;

    AggregatedRow(List<Object> rowValues, List<Object> groupValues, List<Object> aggregateValues,
        int groupingSetIndex) {
      this.rowValues = immutableListAllowingNulls(rowValues);
      this.groupValues = immutableListAllowingNulls(groupValues);
      this.aggregateValues = immutableListAllowingNulls(aggregateValues);
      this.groupingSetIndex = groupingSetIndex;
    }

    List<Object> rowValues() {
      return rowValues;
    }

    List<Object> groupValues() {
      return groupValues;
    }

    List<Object> aggregateValues() {
      return aggregateValues;
    }

    int groupingSetIndex() {
      return groupingSetIndex;
    }
  }

  private static String aggregateExpressionText(AggregateSpec spec) {
    if (spec.countStar()) {
      return "COUNT(*)";
    }
    if (spec.arguments().isEmpty()) {
      return spec.type().name();
    }
    return spec.type().name() + "(" + spec.arguments().stream().map(Object::toString).collect(Collectors.joining(", "))
        + ")";
  }

  private static List<Object> evaluateGroupValues(List<GroupExpression> groupExpressions, ValueExpressionEvaluator eval,
      GenericRecord record, List<GroupTypeTracker> trackers) {
    if (groupExpressions.isEmpty()) {
      return List.of();
    }
    List<Object> values = new ArrayList<>(groupExpressions.size());
    for (int i = 0; i < groupExpressions.size(); i++) {
      Object value = eval.eval(groupExpressions.get(i).expression(), record);
      values.add(value);
      trackers.get(i).track(value);
    }
    return values;
  }

  /**
   * Extract the subset of grouping values that participate in a grouping set to
   * form a key.
   *
   * @param groupValues
   *          evaluated grouping expression values for a record
   * @param groupingSet
   *          grouping set definition
   * @return values in key order for the grouping set
   */
  private static List<Object> keyValuesForGroupingSet(List<Object> groupValues, GroupingSet groupingSet) {
    if (groupingSet.indexes().isEmpty()) {
      return List.of();
    }
    List<Object> values = new ArrayList<>(groupingSet.indexes().size());
    for (Integer index : groupingSet.indexes()) {
      values.add(groupValues.get(index));
    }
    return values;
  }

  /**
   * Produce the grouping values that should appear in the result row for a
   * specific grouping set, inserting {@code null} for suppressed expressions.
   *
   * @param groupValues
   *          evaluated grouping expression values for the record
   * @param groupingSet
   *          grouping set definition being processed
   * @param expressionCount
   *          total number of grouping expressions in the SELECT
   * @return list of values aligned with the grouping expressions
   */
  private static List<Object> projectGroupValuesForSet(List<Object> groupValues, GroupingSet groupingSet,
      int expressionCount) {
    if (expressionCount == 0) {
      return List.of();
    }
    List<Object> projected = new ArrayList<>(expressionCount);
    for (int i = 0; i < expressionCount; i++) {
      projected.add(groupingSet.contains(i) ? groupValues.get(i) : null);
    }
    return projected;
  }

  /**
   * Compute the integer value produced by a GROUPING function for the specified
   * grouping set.
   *
   * @param plan
   *          aggregate plan describing grouping sets
   * @param groupingSetIndex
   *          index of the grouping set currently being processed
   * @param groupingIndexes
   *          grouping expression indexes referenced by the GROUPING function
   * @return integer bitmask describing which expressions are aggregated
   */
  private static int groupingValue(AggregatePlan plan, int groupingSetIndex, List<Integer> groupingIndexes) {
    if (groupingIndexes.isEmpty()) {
      return 0;
    }
    if (groupingSetIndex < 0 || groupingSetIndex >= plan.groupingSets().size()) {
      throw new IllegalArgumentException("Grouping set index out of bounds: " + groupingSetIndex);
    }
    GroupingSet groupingSet = plan.groupingSets().get(groupingSetIndex);
    int value = 0;
    for (Integer index : groupingIndexes) {
      value <<= 1;
      if (!groupingSet.contains(index)) {
        value |= 1;
      }
    }
    return value;
  }

  /**
   * Builds a row with scalar projections deferred until HAVING or output needs
   * them.
   *
   * @param plan
   *          aggregate projection plan
   * @param state
   *          current group's values and grouping set
   * @param aggregateValues
   *          computed aggregate values
   * @param scalarSubqueries
   *          scalar evaluators indexed by projection position
   * @param projectionExpressions
   *          parsed scalar projections
   * @param scalarExpressions
   *          execution-local scalar compilation cache
   * @param subqueryExecutor
   *          nested query executor
   * @param outerQualifiers
   *          visible outer qualifiers
   * @param correlationContext
   *          outer correlation mappings
   * @param groupedSubqueries
   *          validated scalar subqueries within projections or ordering
   *          expressions
   * @return mutable row whose scalar placeholders share one cached value per
   *         group
   */
  private static List<Object> buildRow(AggregatePlan plan, GroupState state, List<Object> aggregateValues,
      Map<Integer, GroupedScalarSubquery> scalarSubqueries, Map<Integer, Expression> projectionExpressions,
      Map<String, GroupedExpression> scalarExpressions, SubqueryExecutor subqueryExecutor, List<String> outerQualifiers,
      Map<String, Map<String, String>> correlationContext, Map<String, GroupedScalarSubquery> groupedSubqueries) {
    List<Object> row = new ArrayList<>(plan.resultColumns().size());
    for (int index = 0; index < plan.resultColumns().size(); index++) {
      ResultColumn column = plan.resultColumns().get(index);
      if (column.isAggregate()) {
        row.add(aggregateValues.get(column.aggregateIndex()));
      } else if (column.isGrouping()) {
        row.add(groupingValue(plan, state.groupingSetIndex(), column.groupingIndexes()));
      } else if (column.subquery() != null) {
        GroupedScalarSubquery subquery = scalarSubqueries.get(index);
        row.add(new LazyScalarValue(() -> subquery.evaluate(state.groupValues())));
      } else if (column.kind() == ColumnKind.EXPRESSION) {
        row.add(null);
      } else {
        row.add(state.groupValues().get(column.groupIndex()));
      }
    }
    if (!projectionExpressions.isEmpty()) {
      Map<String, Object> labels = buildLabelLookup(plan, row, state.groupValues());
      HavingEvaluator evaluator = new HavingEvaluator(plan, aggregateValues, labels, subqueryExecutor, outerQualifiers,
          correlationContext, column -> false,
          new GroupEvaluation(state.groupValues(), state.groupingSetIndex(), scalarExpressions, groupedSubqueries));
      for (int index = 0; index < plan.resultColumns().size(); index++) {
        ResultColumn column = plan.resultColumns().get(index);
        if (column.kind() == ColumnKind.EXPRESSION) {
          Expression expression = projectionExpressions.get(index);
          row.set(index, new LazyScalarValue(() -> evaluator.value(expression)));
        }
      }
    }
    return row;
  }

  /**
   * Resolves a deferred scalar projection when its value is needed.
   *
   * @param value
   *          ordinary value or deferred scalar
   * @return resolved value, including null
   */
  private static Object resolveScalarValue(Object value) {
    return value instanceof LazyScalarValue scalar ? scalar.get() : value;
  }

  /** Caches a scalar projection once per group, including a NULL result. */
  private static final class LazyScalarValue {
    private final Supplier<Object> evaluation;
    private boolean resolved;
    private Object value;

    /**
     * Creates a deferred scalar for one group's current bindings.
     *
     * @param evaluation
     *          scalar computation
     */
    private LazyScalarValue(Supplier<Object> evaluation) {
      this.evaluation = evaluation;
    }

    /**
     * Returns the scalar, evaluating it only on the first access.
     *
     * @return scalar value, including null
     */
    private Object get() {
      if (!resolved) {
        value = evaluation.get();
        resolved = true;
      }
      return value;
    }
  }

  private static Map<String, Object> buildLabelLookup(AggregatePlan plan, List<Object> rowValues,
      List<Object> groupValues) {
    Map<String, Object> map = new HashMap<>();
    List<ResultColumn> columns = plan.resultColumns();
    for (int i = 0; i < columns.size(); i++) {
      String label = columns.get(i).label();
      if (label != null && !label.isBlank()) {
        String normalized = Identifier.lookupKey(label);
        if (normalized != null) {
          map.put(normalized, rowValues.get(i));
        }
      }
    }
    List<GroupExpression> groups = plan.groupExpressions();
    for (int i = 0; i < groups.size(); i++) {
      String label = groups.get(i).label();
      if (label != null && !label.isBlank()) {
        String normalized = Identifier.lookupKey(label);
        if (normalized != null) {
          map.putIfAbsent(normalized, groupValues.get(i));
        }
      }
    }
    return map;
  }

  /**
   * Create an immutable list from the supplied values while preserving any null
   * elements.
   *
   * <p>
   * {@link List#copyOf(Collection)} rejects null entries, but GROUPING SETS
   * require nulls to represent suppressed grouping expressions. This method
   * ensures the returned list is immutable yet retains those null placeholders.
   *
   * @param values
   *          source list, which must be non-null but may contain null elements
   * @return an immutable list containing the same elements, including nulls, as
   *         {@code values}
   */
  private static <T> List<T> immutableListAllowingNulls(List<T> values) {
    Objects.requireNonNull(values, "values");
    return Collections.unmodifiableList(new ArrayList<>(values));
  }

  private static final class GroupState {
    private final List<Object> groupValues;
    private final AggregateAccumulator[] accumulators;
    private final int groupingSetIndex;

    GroupState(List<Object> groupValues, List<AggregateSpec> specs, int groupingSetIndex) {
      this.groupValues = immutableListAllowingNulls(groupValues);
      this.groupingSetIndex = groupingSetIndex;
      this.accumulators = new AggregateAccumulator[specs.size()];
      for (int i = 0; i < specs.size(); i++) {
        this.accumulators[i] = AggregateAccumulator.create(specs.get(i));
      }
    }

    void add(ValueExpressionEvaluator eval, GenericRecord record) {
      for (AggregateAccumulator accumulator : accumulators) {
        accumulator.add(eval, record);
      }
    }

    List<Object> groupValues() {
      return groupValues;
    }

    int groupingSetIndex() {
      return groupingSetIndex;
    }

    AggregateAccumulator accumulator(int index) {
      if (index < 0 || index >= accumulators.length) {
        throw new IllegalArgumentException("Accumulator index out of bounds: " + index);
      }
      return accumulators[index];
    }

    List<Object> results() {
      List<Object> values = new ArrayList<>(accumulators.length);
      for (AggregateAccumulator accumulator : accumulators) {
        values.add(accumulator.result());
      }
      return immutableListAllowingNulls(values);
    }
  }

  /**
   * Identifies a group within a grouping set using a cached content hash.
   *
   * <p>
   * The caller must not modify the supplied values list after construction:
   * scalar-only lists are retained directly, and mutation would invalidate the
   * cached hash and map membership.
   */
  private static final class GroupKey {
    private final int groupingSetIndex;
    private final List<Object> values;
    private final int hash;

    /**
     * Creates a key whose values remain stable for its lifetime.
     *
     * @param groupingSetIndex
     *          index of the grouping set
     * @param values
     *          grouping values; the caller must not modify this list after
     *          construction
     */
    GroupKey(int groupingSetIndex, List<Object> values) {
      this.groupingSetIndex = groupingSetIndex;
      this.values = RelationalKeys.row(values);
      this.hash = 31 * groupingSetIndex + this.values.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof GroupKey other)) {
        return false;
      }
      return groupingSetIndex == other.groupingSetIndex && values.equals(other.values);
    }

    @Override
    public int hashCode() {
      return hash;
    }
  }

  private static final class GroupTypeTracker {
    private Class<?> observedType;

    void track(Object value) {
      if (value != null && observedType == null) {
        observedType = value.getClass();
      }
    }

    int sqlType() {
      return sqlTypeForClass(observedType);
    }
  }

  private abstract static class AggregateAccumulator {
    final AggregateSpec spec;
    Class<?> observedType;

    AggregateAccumulator(AggregateSpec spec) {
      this.spec = spec;
    }

    static AggregateAccumulator create(AggregateSpec spec) {
      return switch (spec.type()) {
        case COUNT -> new CountAccumulator(spec);
        case SUM -> new SumAccumulator(spec);
        case AVG -> new AvgAccumulator(spec);
        case MAX -> new ExtremumAccumulator(spec, true);
        case MIN -> new ExtremumAccumulator(spec, false);
        case STRING_AGG -> new StringAggAccumulator(spec);
      };
    }

    abstract void add(ValueExpressionEvaluator eval, GenericRecord record);

    abstract Object result();

    int sqlType() {
      return sqlTypeForClass(observedType);
    }

    Object evaluate(ValueExpressionEvaluator eval, GenericRecord record) {
      if (spec.countStar() || spec.arguments().isEmpty()) {
        return null;
      }
      return evaluate(eval, record, spec.arguments().getFirst());
    }

    Object evaluate(ValueExpressionEvaluator eval, GenericRecord record, Expression argument) {
      return argument == null ? null : eval.eval(argument, record);
    }

    void trackType(Object value) {
      if (value != null && observedType == null) {
        observedType = value.getClass();
      }
    }
  }

  private static final class CountAccumulator extends AggregateAccumulator {
    private long count = 0L;

    CountAccumulator(AggregateSpec spec) {
      super(spec);
    }

    @Override
    void add(ValueExpressionEvaluator eval, GenericRecord record) {
      if (spec.countStar()) {
        count++;
        return;
      }
      Object value = evaluate(eval, record);
      if (value != null) {
        trackType(value);
        count++;
      }
    }

    @Override
    Object result() {
      return count;
    }

    @Override
    int sqlType() {
      return Types.BIGINT;
    }
  }

  private static final class SumAccumulator extends AggregateAccumulator {
    private BigDecimal sum = BigDecimal.ZERO;
    private boolean seenValue = false;

    SumAccumulator(AggregateSpec spec) {
      super(spec);
    }

    @Override
    void add(ValueExpressionEvaluator eval, GenericRecord record) {
      Object value = evaluate(eval, record);
      if (value == null) {
        return;
      }
      trackType(value);
      sum = sum.add(toBigDecimal(value));
      seenValue = true;
    }

    @Override
    Object result() {
      if (!seenValue) {
        return null;
      }
      Class<?> type = observedType;
      if (type == null) {
        return sum.doubleValue();
      }
      if (type == Byte.class || type == Short.class || type == Integer.class || type == Long.class) {
        return sum.longValue();
      }
      if (type == Float.class || type == Double.class) {
        return sum.doubleValue();
      }
      if (type == BigDecimal.class) {
        return sum;
      }
      return sum.doubleValue();
    }

    @Override
    int sqlType() {
      Class<?> type = observedType;
      if (type == null) {
        return Types.DOUBLE;
      }
      if (type == Byte.class || type == Short.class || type == Integer.class || type == Long.class) {
        return Types.BIGINT;
      }
      if (type == Float.class) {
        return Types.REAL;
      }
      if (type == Double.class) {
        return Types.DOUBLE;
      }
      if (type == BigDecimal.class) {
        return Types.DECIMAL;
      }
      return Types.DOUBLE;
    }
  }

  private static final class AvgAccumulator extends AggregateAccumulator {
    private BigDecimal sum = BigDecimal.ZERO;
    private long count = 0L;

    AvgAccumulator(AggregateSpec spec) {
      super(spec);
    }

    @Override
    void add(ValueExpressionEvaluator eval, GenericRecord record) {
      Object value = evaluate(eval, record);
      if (value == null) {
        return;
      }
      trackType(value);
      sum = sum.add(toBigDecimal(value));
      count++;
    }

    @Override
    Object result() {
      if (count == 0L) {
        return null;
      }
      return sum.divide(BigDecimal.valueOf(count), MathContext.DECIMAL64).doubleValue();
    }

    @Override
    int sqlType() {
      return Types.DOUBLE;
    }
  }

  private static final class ExtremumAccumulator extends AggregateAccumulator {
    private Object extremum;
    private final boolean isMax;

    ExtremumAccumulator(AggregateSpec spec, boolean isMax) {
      super(spec);
      this.isMax = isMax;
    }

    @Override
    void add(ValueExpressionEvaluator eval, GenericRecord record) {
      Object value = evaluate(eval, record);
      if (value == null) {
        return;
      }
      trackType(value);
      if (extremum == null) {
        extremum = value;
        return;
      }
      int cmp = SqlComparisons.typedCompare(value, extremum);
      if ((isMax && cmp > 0) || (!isMax && cmp < 0)) {
        extremum = value;
      }
    }

    @Override
    Object result() {
      return extremum;
    }
  }

  private static final class StringAggAccumulator extends AggregateAccumulator {
    private final List<String> parts = new ArrayList<>();
    private String separator;

    StringAggAccumulator(AggregateSpec spec) {
      super(spec);
      observedType = String.class;
    }

    @Override
    void add(ValueExpressionEvaluator eval, GenericRecord record) {
      List<Expression> arguments = spec.arguments();
      Object value = evaluate(eval, record, arguments.getFirst());
      if (value == null) {
        return;
      }
      if (arguments.size() > 1) {
        Object sep = evaluate(eval, record, arguments.get(1));
        if (sep != null) {
          separator = sep.toString();
        }
      }
      parts.add(value.toString());
    }

    @Override
    Object result() {
      if (parts.isEmpty()) {
        return null;
      }
      String sep = separator == null ? "" : separator;
      return String.join(sep, parts);
    }

    @Override
    int sqlType() {
      return Types.VARCHAR;
    }
  }

  private static BigDecimal toBigDecimal(Object value) {
    if (value instanceof BigDecimal bd) {
      return bd;
    }
    if (value instanceof Number num) {
      if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
        return BigDecimal.valueOf(num.longValue());
      }
      return BigDecimal.valueOf(num.doubleValue());
    }
    return new BigDecimal(value.toString());
  }

  private static int sqlTypeForClass(Class<?> cls) {
    if (cls == null) {
      return Types.OTHER;
    }
    if (cls == Byte.class || cls == Short.class || cls == Integer.class) {
      return Types.INTEGER;
    }
    if (cls == Long.class) {
      return Types.BIGINT;
    }
    if (cls == Float.class) {
      return Types.REAL;
    }
    if (cls == Double.class) {
      return Types.DOUBLE;
    }
    if (cls == BigDecimal.class) {
      return Types.DECIMAL;
    }
    if (cls == Boolean.class) {
      return Types.BOOLEAN;
    }
    if (GenericRecord.class.isAssignableFrom(cls)) {
      return Types.STRUCT;
    }
    if (List.class.isAssignableFrom(cls) || Object[].class.isAssignableFrom(cls)) {
      return Types.ARRAY;
    }
    if (cls == String.class || CharSequence.class.isAssignableFrom(cls)) {
      return Types.VARCHAR;
    }
    if (Date.class.isAssignableFrom(cls) || LocalDate.class.isAssignableFrom(cls)) {
      return Types.DATE;
    }
    if (Time.class.isAssignableFrom(cls) || LocalTime.class.isAssignableFrom(cls)) {
      return Types.TIME;
    }
    if (Timestamp.class.isAssignableFrom(cls) || LocalDateTime.class.isAssignableFrom(cls)) {
      return Types.TIMESTAMP;
    }
    return Types.OTHER;
  }

  /**
   * Creates a Boolean type check using source metadata and column mappings.
   *
   * @param schema
   *          the source record schema
   * @param qualifierColumnMapping
   *          qualifier-to-column mappings
   * @param unqualifiedColumnMapping
   *          mappings for unique unqualified columns
   * @return a schema-backed Boolean column predicate
   */
  private static Predicate<Column> booleanColumnPredicate(Schema schema,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping) {
    Map<String, String> columnIndex = new HashMap<>();
    for (Schema.Field field : schema.getFields()) {
      columnIndex.put(Identifier.lookupKey(field.name()), field.name());
    }
    Map<String, Map<String, String>> qualified = ColumnMappingUtil.normaliseQualifierMapping(qualifierColumnMapping);
    Map<String, String> unqualified = ColumnMappingUtil.normaliseUnqualifiedMapping(unqualifiedColumnMapping);
    return column -> {
      String qualifier = column.getTable() == null ? null : column.getTable().getName();
      String canonical = ColumnMappingUtil.canonicalFieldName(qualifier, column.getColumnName(), qualified, unqualified,
          columnIndex);
      Schema.Field field = schema.getField(canonical);
      return field != null && JdbcTypeMapper.nonNullSchema(field.schema()).getType() == Schema.Type.BOOLEAN;
    };
  }

  /**
   * Dependencies shared by scalar ORDER BY evaluations.
   *
   * @param subqueryExecutor
   *          nested query executor
   * @param outerQualifiers
   *          visible source qualifiers
   * @param correlationContext
   *          correlation mappings
   * @param scalarExpressions
   *          execution-local compiled expressions
   * @param subqueries
   *          validated grouped subqueries
   */
  private record ScalarExecution(SubqueryExecutor subqueryExecutor, List<String> outerQualifiers,
      Map<String, Map<String, String>> correlationContext, Map<String, GroupedExpression> scalarExpressions,
      Map<String, GroupedScalarSubquery> subqueries) {
  }

  /**
   * Current group values and the caches shared by its scalar evaluators.
   *
   * @param groupValues
   *          grouping values, including suppressed NULL keys
   * @param groupingSetIndex
   *          current grouping set
   * @param scalarExpressions
   *          execution-local compiled scalar expressions
   * @param groupedSubqueries
   *          validated scalar subqueries inside grouped expressions
   */
  private record GroupEvaluation(List<Object> groupValues, int groupingSetIndex,
      Map<String, GroupedExpression> scalarExpressions, Map<String, GroupedScalarSubquery> groupedSubqueries) {
  }

  private static final class HavingEvaluator {
    private final AggregatePlan plan;
    private final List<Object> aggregateValues;
    private final SubqueryExecutor subqueryExecutor;
    // Values can be LazyScalarValue placeholders; every value read must use
    // resolveScalarValue.
    private final Map<String, Object> labelLookup;
    private final List<String> correlatedQualifiers;
    private final Map<String, Map<String, String>> correlationContext;
    private final int groupingSetIndex;
    private final Map<String, Integer> groupIndexLookup;
    private final Predicate<Column> booleanColumn;
    private final List<Object> groupValues;
    private final Map<String, GroupedExpression> scalarExpressions;
    private final Map<String, GroupedScalarSubquery> groupedSubqueries;

    HavingEvaluator(AggregatePlan plan, List<Object> aggregateValues, Map<String, Object> labelLookup,
        SubqueryExecutor subqueryExecutor, List<String> correlatedQualifiers,
        Map<String, Map<String, String>> correlationContext, Predicate<Column> booleanColumn, GroupEvaluation group) {
      this.plan = plan;
      this.groupValues = group.groupValues();
      this.scalarExpressions = group.scalarExpressions();
      this.groupedSubqueries = group.groupedSubqueries();
      this.aggregateValues = aggregateValues;
      this.labelLookup = Collections.unmodifiableMap(new HashMap<>(labelLookup));
      this.subqueryExecutor = subqueryExecutor;
      this.correlatedQualifiers = correlatedQualifiers == null ? List.of() : List.copyOf(correlatedQualifiers);
      this.correlationContext = ColumnMappingUtil
          .normaliseQualifierMapping(correlationContext == null ? Map.of() : correlationContext);
      this.groupingSetIndex = group.groupingSetIndex();
      this.groupIndexLookup = groupIndexLookup(plan.groupExpressions());
      this.booleanColumn = Objects.requireNonNull(booleanColumn, "booleanColumn");
    }

    boolean eval(Expression expression) {
      return Boolean.TRUE.equals(evalTruth(expression));
    }

    private Boolean evalTruth(Expression expression) {
      Expression expr = ExpressionEvaluator.unwrapParenthesis(expression);
      if (expr == null) {
        return true;
      }
      if (expr instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
        return evalTruth(list.getFirst());
      }
      if (expr instanceof NullValue) {
        return null;
      }
      if (expr instanceof net.sf.jsqlparser.expression.BooleanValue bool) {
        return bool.getValue();
      }
      if (expr instanceof AndExpression and) {
        Boolean left = evalTruth(and.getLeftExpression());
        return Boolean.FALSE.equals(left) ? Boolean.FALSE : SqlTruth.and(left, evalTruth(and.getRightExpression()));
      }
      if (expr instanceof OrExpression or) {
        Boolean left = evalTruth(or.getLeftExpression());
        return Boolean.TRUE.equals(left) ? Boolean.TRUE : SqlTruth.or(left, evalTruth(or.getRightExpression()));
      }
      if (expr instanceof NotExpression not) {
        return SqlTruth.not(evalTruth(not.getExpression()));
      }
      if (expr instanceof IsNullExpression isNull) {
        Object operand = value(isNull.getLeftExpression());
        boolean isNullVal = operand == null;
        return isNull.isNot() != isNullVal;
      }
      if (expr instanceof IsUnknownExpression unknown) {
        return unknown.isNot() != (evalTruth(unknown.getLeftExpression()) == null);
      }
      if (expr instanceof IsBooleanExpression isBoolean) {
        Boolean truth = evalTruth(isBoolean.getLeftExpression());
        return SqlTruth.is(truth, isBoolean.isTrue(), isBoolean.isNot());
      }
      if (expr instanceof Between between) {
        Object val = value(between.getLeftExpression());
        Object low = value(between.getBetweenExpressionStart());
        Object high = value(between.getBetweenExpressionEnd());
        Boolean in = SqlTruth.and(SqlComparisons.compare(">=", SqlComparisons.compareOrUnknown(val, low)),
            SqlComparisons.compare("<=", SqlComparisons.compareOrUnknown(val, high)));
        return between.isNot() ? SqlTruth.not(in) : in;
      }
      if (expr instanceof InExpression in) {
        return evalIn(in);
      }
      if (expr instanceof ExistsExpression exists) {
        return evalExists(exists);
      }
      if (expr instanceof SimilarToExpression) {
        throw new IllegalArgumentException("SIMILAR TO is not supported in HAVING clauses");
      }
      if (expr instanceof LikeExpression like) {
        return evalLike(like);
      }
      if (expr instanceof BinaryExpression be) {
        return evalBinary(be);
      }
      if (expr instanceof Column column && (findGroupIndex(column, groupIndexLookup, plan.groupExpressions()) == null
          || !booleanColumn.test(column))) {
        throw new IllegalArgumentException("Unsupported predicate: " + expr);
      }
      Object result = value(expr);
      if (result == null || result instanceof Boolean) {
        return (Boolean) result;
      }
      throw new IllegalArgumentException("Unsupported predicate: " + expr);
    }

    private Boolean evalIn(InExpression in) {
      Object leftVal = value(in.getLeftExpression());
      Expression right = in.getRightExpression();
      if (right instanceof ExpressionList<?> list) {
        return SqlTruth.in(leftVal, list, this::value, in.isNot());
      }
      if (right instanceof Select subSelect) {
        if (subqueryExecutor == null) {
          throw new IllegalStateException("IN subqueries require a subquery executor");
        }
        SubqueryExecutor.SubqueryResult result = subqueryExecutor.executeCorrelated(subSelect, correlationQualifiers(),
            correlationColumns(), correlationContext, (qualifier, column) -> correlatedValue(qualifier, column));
        return SqlTruth.in(leftVal, result.firstColumnValues(), in.isNot());
      }
      throw new IllegalArgumentException("Unsupported IN expression in HAVING clause: " + in);
    }

    private boolean evalExists(ExistsExpression exists) {
      if (subqueryExecutor == null) {
        throw new IllegalStateException("EXISTS subqueries require a subquery executor");
      }
      if (!(exists.getRightExpression() instanceof Select subSelect)) {
        throw new IllegalArgumentException("EXISTS requires a subquery");
      }
      SubqueryExecutor.SubqueryResult result = subqueryExecutor.executeCorrelated(subSelect, correlationQualifiers(),
          correlationColumns(), correlationContext, (qualifier, column) -> correlatedValue(qualifier, column));
      boolean hasRows = !result.rows().isEmpty();
      return exists.isNot() != hasRows;
    }

    private Boolean evalLike(LikeExpression like) {
      Object left = value(like.getLeftExpression());
      Object right = value(like.getRightExpression());
      if (left == null || right == null) {
        return null;
      }
      String leftText = left.toString();
      String pattern = right.toString();
      LikeExpression.KeyWord keyWord = like.getLikeKeyWord();
      LikeExpression.KeyWord effective = keyWord == null ? LikeExpression.KeyWord.LIKE : keyWord;
      Character escapeChar = null;
      if (like.getEscape() != null) {
        Object escapeVal = value(like.getEscape());
        if (escapeVal != null) {
          String escape = escapeVal.toString();
          if (!escape.isEmpty()) {
            if (escape.length() != 1) {
              throw new IllegalArgumentException("LIKE escape clause must be a single character");
            }
            escapeChar = escape.charAt(0);
          }
        }
      }
      boolean matches;
      if (effective == LikeExpression.KeyWord.SIMILAR_TO) {
        matches = StringFunctions.similarTo(leftText, pattern, escapeChar);
      } else {
        boolean caseInsensitive = effective == LikeExpression.KeyWord.ILIKE;
        matches = StringFunctions.like(leftText, pattern, caseInsensitive, escapeChar);
      }
      return like.isNot() != matches;
    }

    private Boolean evalBinary(BinaryExpression be) {
      Expression leftExpr = be.getLeftExpression();
      Expression rightExpr = be.getRightExpression();
      if (JSqlParserSupport.isQuantifiedOperand(leftExpr) || JSqlParserSupport.isQuantifiedOperand(rightExpr)) {
        return QuantifiedComparisons.evaluate(be,
            expression -> new QuantifiedComparisons.ScalarOperand(value(expression), UnaryOperator.identity()),
            this::value, subSelect -> evaluateSubquery(subSelect).firstColumnValues());
      }
      String op = be.getStringExpression();
      Object left = value(be.getLeftExpression());
      Object right = value(be.getRightExpression());
      if (be instanceof net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression distinct) {
        return distinct.isNot() != SqlTruth.isDistinctFrom(left, right);
      }
      return SqlComparisons.compare(op, SqlComparisons.compareOrUnknown(left, right));
    }

    private Object value(Expression expression) {
      Expression expr = unwrap(expression);
      Integer groupIndex = findGroupIndex(expr, groupIndexLookup, plan.groupExpressions());
      if (groupIndex != null) {
        return groupValues.get(groupIndex);
      }
      if (isGroupedPredicate(expr)) {
        return evalTruth(expr);
      }
      if (expr instanceof Select subSelect) {
        GroupedScalarSubquery grouped = groupedSubqueries.get(subSelect.toString());
        return grouped == null ? evaluateScalarSubquery(subSelect) : grouped.evaluate(groupValues);
      }
      if (expr instanceof ArrayConstructor array) {
        return arrayValues(array.getExpressions());
      }
      if (expr instanceof SignedExpression || expr instanceof Addition || expr instanceof Subtraction
          || expr instanceof Multiplication || expr instanceof Division || expr instanceof Modulo) {
        return scalarValue(expr);
      }
      if (expr instanceof Function func) {
        if ("ARRAY".equalsIgnoreCase(func.getName())) {
          ExpressionList<?> parameters = func.getParameters();
          if (parameters != null && !parameters.isEmpty() && parameters.getFirst() instanceof Select subSelect) {
            return ArrayFunctions.fromSubquery(evaluateSubquery(subSelect), subSelect);
          }
          return arrayValues(parameters);
        }
        if (CorrelatedBindings.isReserved(func)) {
          return CorrelatedBindings.value(func);
        }
        if (isGroupingFunction(func)) {
          return grouping(func);
        }
        if (isAggregateName(func.getName())) {
          return aggregateValue(func);
        }
        return scalarValue(expr);
      }
      if (expr instanceof Column col) {
        return aliasValue(col.getColumnName());
      }
      if (LiteralConverter.isLiteral(expr)) {
        return LiteralConverter.toLiteral(expr);
      }
      return scalarValue(expr);
    }

    /**
     * Evaluates scalar operators through the shared SQL value evaluator. Whole
     * predicate bindings use the grouped evaluator and preserve UNKNOWN as NULL.
     *
     * @param expression
     *          scalar expression containing group-dependent operands
     * @return scalar value, possibly null
     */
    private Object scalarValue(Expression expression) {
      GroupedExpression scalar = scalarExpressions.computeIfAbsent(expression.toString(),
          text -> new GroupedExpression(expression, operand -> isGroupedPredicate(operand)
              || isGroupOperand(operand, groupIndexLookup, plan.groupExpressions())));
      return scalar.evaluate(operand -> {
        Integer groupIndex = findGroupIndex(operand, groupIndexLookup, plan.groupExpressions());
        return groupIndex == null ? value(operand) : groupValues.get(groupIndex);
      });
    }

    /**
     * Evaluates an array constructor using the current group's values.
     *
     * @param expressions
     *          the array elements, or null for an empty constructor
     * @return homogenized array values
     */
    private List<Object> arrayValues(ExpressionList<?> expressions) {
      List<Object> elements = new ArrayList<>();
      if (expressions != null) {
        for (Expression element : expressions) {
          elements.add(value(element));
        }
      }
      return ArrayFunctions.homogenizeArrayValues(elements);
    }

    private Object evaluateScalarSubquery(Select subSelect) {
      return evaluateSubquery(subSelect).scalarValue();
    }

    /**
     * Executes a subquery with the current group's correlation bindings.
     *
     * @param subSelect
     *          the nested query
     * @return the subquery result
     * @throws IllegalStateException
     *           if no subquery executor is available
     */
    private SubqueryExecutor.SubqueryResult evaluateSubquery(Select subSelect) {
      if (subqueryExecutor == null) {
        throw new IllegalStateException("HAVING subqueries require a subquery executor");
      }
      return subqueryExecutor.executeCorrelated(subSelect, correlationQualifiers(), correlationColumns(),
          correlationContext, (qualifier, column) -> correlatedValue(qualifier, column));
    }

    private Object aggregateValue(Function func) {
      AggregateType type = AggregateType.from(func.getName());
      if (type == null) {
        throw new IllegalArgumentException("Unsupported function in HAVING clause: " + func.getName());
      }
      for (int i = 0; i < plan.specs().size(); i++) {
        AggregateSpec spec = plan.specs().get(i);
        if (spec.type() != type) {
          continue;
        }
        if (spec.countStar() != (type == AggregateType.COUNT && isCountStar(func))) {
          continue;
        }
        if (!spec.countStar()) {
          ExpressionList<?> params = func.getParameters();
          if (params == null || params.size() != spec.arguments().size()) {
            continue;
          }
          boolean matches = true;
          for (int j = 0; j < spec.arguments().size(); j++) {
            Expression expected = spec.arguments().get(j);
            Expression actual = params.get(j);
            if (!expressionsEquivalent(expected, actual)) {
              matches = false;
              break;
            }
          }
          if (!matches) {
            continue;
          }
        }
        return aggregateValues.get(i);
      }
      throw new IllegalArgumentException("HAVING references aggregate not present in SELECT: " + func);
    }

    private Object grouping(Function func) {
      List<Integer> indexes = groupingArgumentIndexes(func, groupIndexLookup, plan.groupExpressions());
      return groupingValue(plan, groupingSetIndex, indexes);
    }

    private Object correlatedValue(String qualifier, String column) {
      String normalizedQualifier = JParqUtil.normalizeQualifier(qualifier);
      String normalizedColumn = Identifier.lookupKey(column);
      if (normalizedQualifier != null && normalizedColumn != null && !correlationContext.isEmpty()) {
        Map<String, String> mapping = correlationContext.get(normalizedQualifier);
        if (mapping != null) {
          String canonical = mapping.get(normalizedColumn);
          if (canonical != null) {
            String lookupKey = Identifier.lookupKey(canonical);
            if (labelLookup.containsKey(lookupKey)) {
              return resolveScalarValue(labelLookup.get(lookupKey));
            }
          }
        }
      }
      return aliasValue(column);
    }

    private List<String> correlationQualifiers() {
      if (correlationContext.isEmpty()) {
        return correlatedQualifiers;
      }
      Set<String> qualifiers = new LinkedHashSet<>(correlatedQualifiers);
      qualifiers.addAll(correlationContext.keySet());
      return List.copyOf(qualifiers);
    }

    // Unlike ValueExpressionEvaluator/ExpressionEvaluator, we don't filter by
    // caseInsensitiveIndex because HAVING operates on aggregate results, not a
    // physical schema. All projection aliases should be available to subqueries.
    private Set<String> correlationColumns() {
      if (correlationContext.isEmpty()) {
        return Set.of();
      }
      Set<String> columns = new LinkedHashSet<>();
      for (Map<String, String> mapping : correlationContext.values()) {
        if (mapping == null) {
          continue;
        }
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
          String column = entry.getKey();
          String canonical = entry.getValue();
          String normalizedColumn = Identifier.lookupKey(column);
          String normalizedCanonical = Identifier.lookupKey(canonical);
          if ((normalizedColumn != null && labelLookup.containsKey(normalizedColumn))
              || (normalizedCanonical != null && labelLookup.containsKey(normalizedCanonical))) {
            columns.add(column);
          }
        }
      }
      return Set.copyOf(columns);
    }

    private Object aliasValue(String name) {
      if (name == null) {
        return null;
      }
      String key = Identifier.lookupKey(name);
      if (key == null || !labelLookup.containsKey(key)) {
        throw syntaxFailure("Unknown or ungrouped column in grouped expression: " + name);
      }
      return resolveScalarValue(labelLookup.get(key));
    }
  }

  /**
   * Perform the calculation suitable for the operation.
   *
   * @param leftVal
   *          the value on the left
   * @param rightVal
   *          the value on the right
   * @param op
   *          the operation to perform (ADD, SUB, MUL etc.)
   * @return the numeric result, or null for NULL operands or a zero divisor
   * @throws IllegalArgumentException
   *           if BIGINT arithmetic overflows, carrying SQLState 22003
   */
  public static Object calculate(Object leftVal, Object rightVal, Operation op) {
    return calculate(leftVal, rightVal, op, null);
  }

  /**
   * Calculates arithmetic in the declared numeric family, falling back to operand
   * representations when a declaration is unavailable.
   *
   * @param leftVal
   *          evaluated left operand
   * @param rightVal
   *          evaluated right operand
   * @param op
   *          arithmetic operator
   * @param resultType
   *          statically resolved result type, or null
   * @return the numeric result, or null for NULL operands or a zero divisor
   * @throws IllegalArgumentException
   *           if BIGINT arithmetic overflows, carrying SQLState 22003
   */
  public static Object calculate(Object leftVal, Object rightVal, Operation op, NumericType resultType) {
    if (leftVal == null || rightVal == null) {
      return null;
    }
    if (resultType != null
        ? resultType.jdbcType() == Types.DOUBLE
        : leftVal instanceof Double || leftVal instanceof Float || rightVal instanceof Double
            || rightVal instanceof Float) {
      double left = leftVal instanceof Number number ? number.doubleValue() : Double.parseDouble(leftVal.toString());
      double right = rightVal instanceof Number number ? number.doubleValue() : Double.parseDouble(rightVal.toString());
      return switch (op) {
        case ADD -> left + right;
        case SUB -> left - right;
        case MUL -> left * right;
        case DIV -> right == 0 ? null : left / right;
        case MOD -> right == 0 ? null : left % right;
      };
    }
    if (resultType != null
        ? resultType.jdbcType() == Types.BIGINT
        : isWholeNumber(leftVal) && isWholeNumber(rightVal) && op != Operation.DIV) {
      long left = ((Number) leftVal).longValue();
      long right = ((Number) rightVal).longValue();
      try {
        return switch (op) {
          case ADD -> Math.addExact(left, right);
          case SUB -> Math.subtractExact(left, right);
          case MUL -> Math.multiplyExact(left, right);
          case MOD -> right == 0 ? null : left % right;
          default -> throw new IllegalStateException("Unexpected whole-number operation: " + op);
        };
      } catch (ArithmeticException failure) {
        throw new IllegalArgumentException("BIGINT arithmetic overflow",
            new SQLDataException("BIGINT arithmetic overflow", "22003", failure));
      }
    }
    BigDecimal leftNum = toBigDecimal(leftVal);
    BigDecimal rightNum = toBigDecimal(rightVal);
    return switch (op) {
      case ADD -> leftNum.add(rightNum);
      case SUB -> leftNum.subtract(rightNum);
      case MUL -> leftNum.multiply(rightNum);
      case DIV -> rightNum.compareTo(BigDecimal.ZERO) == 0 ? null : leftNum.divide(rightNum, MathContext.DECIMAL64);
      case MOD -> rightNum.compareTo(BigDecimal.ZERO) == 0 ? null : leftNum.remainder(rightNum);
    };
  }

  /**
   * Identifies Java whole-number operands eligible for exact long arithmetic.
   *
   * @param value
   *          evaluated operand
   * @return whether the operand has an integral Java type
   */
  private static boolean isWholeNumber(Object value) {
    return value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long;
  }

}
