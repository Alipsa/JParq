package se.alipsa.jparq.engine;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Date;
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
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.engine.SqlParser.OrderKey;
import se.alipsa.jparq.helper.JParqUtil;
import se.alipsa.jparq.helper.LiteralConverter;
import se.alipsa.jparq.helper.StringExpressions;

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
   *          {@code -1} when the column originates from the GROUP BY clause
   * @param groupIndex
   *          index into the {@link GroupExpression} list describing the grouping
   *          value used for this column, or {@code -1} when the column is an
   *          aggregate
   * @param kind
   *          origin of the column within the aggregate plan
   * @param groupingIndexes
   *          grouping expression indexes referenced by a GROUPING() invocation
   * @param expressionText
   *          textual representation of the GROUPING() expression for error
   *          reporting
  */
  public record ResultColumn(String label, ColumnKind kind, int aggregateIndex, int groupIndex,
      List<Integer> groupingIndexes, String expressionText) {
    /**
     * Canonical constructor normalising the provided column metadata.
     *
     * @param label
     *          column label exposed to JDBC clients
     * @param aggregateIndex
     *          index of the aggregate specification backing the column, or
     *          {@code -1} when the column originates from the GROUP BY clause
     * @param groupIndex
     *          index into the {@link GroupExpression} list describing the grouping
     *          value used for this column, or {@code -1} when the column is an
     *          aggregate
     * @param kind
     *          origin of the column within the aggregate plan
     * @param groupingIndexes
     *          grouping expression indexes referenced by a GROUPING() invocation
     * @param expressionText
     *          textual representation of the GROUPING() expression for error
     *          reporting
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
        default -> throw new IllegalArgumentException("Unsupported result column kind: " + kind);
      }
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
    GROUPING
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
     * Determine whether the grouping set includes the specified grouping expression.
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
   * Attempt to build an {@link AggregatePlan} for the provided SELECT. Returns
   * {@code null} if the statement contains no aggregates and no GROUP BY clause.
   *
   * @param select
   *          parsed SELECT statement
   * @return aggregate plan for the SELECT list, or {@code null} when the query
   *         should be handled by the non-aggregate execution path
   */
  public static AggregatePlan plan(SqlParser.Select select) {
    List<Expression> expressions = select.expressions();
    if (expressions.isEmpty()) {
      return null;
    }

    boolean containsAggregate = expressions.stream()
        .anyMatch(expr -> expr instanceof Function func && AggregateType.from(func.getName()) != null);
    List<GroupExpression> groupExpressions = buildGroupExpressions(select.groupByExpressions());
    Map<String, Integer> groupIndexByText = groupIndexLookup(groupExpressions);
    List<GroupingSet> groupingSets = buildGroupingSets(select.groupingSets(), groupExpressions.size(),
        containsAggregate);

    boolean hasGroupBy = !groupExpressions.isEmpty() || !groupingSets.isEmpty();
    if (!hasGroupBy && !containsAggregate) {
      return null;
    }

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
          resultColumns.add(new ResultColumn(label, ColumnKind.AGGREGATE, aggIndex, -1, List.of(), null));
          continue;
        }
        if (isGroupingFunction(func)) {
          List<Integer> groupingIndexes = groupingArgumentIndexes(func, groupIndexByText);
          resultColumns.add(
              new ResultColumn(label, ColumnKind.GROUPING, -1, -1, groupingIndexes, func.toString()));
          continue;
        }
      }

      if (!hasGroupBy && specs.isEmpty()) {
        return null; // simple projection without grouping/aggregates
      }

      Integer groupIndex = null;
      for (String key : expressionKeys(expr)) {
        if (key == null) {
          continue;
        }
        groupIndex = groupIndexByText.get(key);
        if (groupIndex != null) {
          break;
        }
      }
      if (groupIndex == null) {
        throw new IllegalArgumentException(
            "SELECT expression '" + expr + "' must appear in the GROUP BY clause when aggregates are present");
      }
      resultColumns.add(new ResultColumn(label, ColumnKind.GROUP, -1, groupIndex, List.of(), null));
    }

    Expression having = select.having();
    if (having != null) {
      for (Function func : aggregateFunctions(having)) {
        AggregateSpec spec = aggregateSpec(func, func.toString());
        if (spec != null && !containsEquivalentAggregate(specs, spec)) {
          specs.add(spec);
        }
      }
    }

    return new AggregatePlan(specs, resultColumns, groupExpressions, groupingSets);
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
      String columnName = JParqUtil.normalizeQualifier(column.getColumnName());
      if (columnName != null) {
        keys.add(columnName);
        addTableQualifiers(keys, columnName, column.getTable());
      }
    }
    return List.copyOf(keys);
  }

  private static AggregateSpec aggregateSpec(Function func, String label) {
    AggregateType type = AggregateType.from(func.getName());
    if (type == null) {
      return null;
    }
    if (func.isDistinct()) {
      throw new IllegalArgumentException("DISTINCT aggregates are not supported yet");
    }

    boolean countStar = false;
    List<Expression> args = List.of();

    if (type == AggregateType.COUNT && func.isAllColumns()) {
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
   * Create {@link GroupingSet} instances from the parsed grouping set
   * definition.
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
   * @return immutable list of grouping expression indexes
   */
  private static List<Integer> groupingArgumentIndexes(Function func, Map<String, Integer> groupIndexByText) {
    ExpressionList<?> parameters = func.getParameters();
    if (parameters == null || parameters.isEmpty()) {
      throw new IllegalArgumentException("GROUPING function requires at least one argument");
    }
    List<Integer> indexes = new ArrayList<>(parameters.size());
    for (Expression argument : parameters) {
      Integer match = null;
      for (String key : expressionKeys(argument)) {
        if (key == null) {
          continue;
        }
        match = groupIndexByText.get(key);
        if (match != null) {
          break;
        }
      }
      if (match == null) {
        throw new IllegalArgumentException(
            "GROUPING argument '" + argument + "' must appear in the GROUP BY clause");
      }
      indexes.add(match);
    }
    return List.copyOf(indexes);
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
    expression.accept(new ExpressionVisitorAdapter<Void>() {
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
    List<GroupExpression> groupExpressions = plan.groupExpressions();
    List<GroupingSet> groupingSets = plan.groupingSets();
    List<GroupTypeTracker> groupTrackers = new ArrayList<>(groupExpressions.size());
    for (int i = 0; i < groupExpressions.size(); i++) {
      groupTrackers.add(new GroupTypeTracker());
    }

    int aggregateCount = plan.specs().size();
    int[] aggregateSqlTypes = new int[aggregateCount];
    for (int i = 0; i < aggregateCount; i++) {
      aggregateSqlTypes[i] = AggregateAccumulator.create(plan.specs().get(i)).sqlType();
    }

    Map<GroupKey, GroupState> states = new LinkedHashMap<>();

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
          valueEval = new ValueExpressionEvaluator(schema, subqueryExecutor, outerQualifiers, qualifierColumnMapping,
              unqualifiedColumnMapping);
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

    List<List<Object>> rows = new ArrayList<>();
    Expression normalizedHaving = ExpressionEvaluator.unwrapParenthesis(having);

    for (GroupState state : states.values()) {
      List<Object> aggregateValues = state.results();
      List<Object> row = buildRow(plan, state, aggregateValues);
      Map<String, Object> labelLookup = buildLabelLookup(plan, row, state.groupValues());
      boolean include = true;
      if (normalizedHaving != null) {
        HavingEvaluator evaluator = new HavingEvaluator(plan, aggregateValues, labelLookup, subqueryExecutor,
            outerQualifiers, state.groupingSetIndex());
        include = evaluator.eval(normalizedHaving);
      }
      if (include) {
        rows.add(row);
      }
    }

    if (orderBy != null && !orderBy.isEmpty()) {
      sortAggregatedRows(rows, plan, orderBy);
    }

    List<Integer> columnSqlTypes = new ArrayList<>(plan.resultColumns().size());
    for (ResultColumn column : plan.resultColumns()) {
      if (column.isAggregate()) {
        columnSqlTypes.add(aggregateSqlTypes[column.aggregateIndex()]);
      } else if (column.isGrouping()) {
        columnSqlTypes.add(Types.INTEGER);
      } else {
        columnSqlTypes.add(groupSqlTypes.isEmpty() ? Types.OTHER : groupSqlTypes.get(column.groupIndex()));
      }
    }

    return new AggregateResult(List.copyOf(rows), List.copyOf(columnSqlTypes));
  }

  private static void sortAggregatedRows(List<List<Object>> rows, AggregatePlan plan, List<OrderKey> orderBy) {
    Map<String, Integer> indexByColumn = buildOrderIndex(plan);
    Comparator<List<Object>> comparator = (left, right) -> {
      for (OrderKey key : orderBy) {
        Integer idx = indexByColumn.get(key.column().toLowerCase(Locale.ROOT));
        if (idx == null) {
          throw new IllegalArgumentException(
              "ORDER BY column '" + key.column() + "' is not present in the SELECT list");
        }
        Object lv = left.get(idx);
        Object rv = right.get(idx);
        if (lv == null || rv == null) {
          int nullCmp = (lv == null ? 1 : 0) - (rv == null ? 1 : 0);
          if (!key.asc()) {
            nullCmp = -nullCmp;
          }
          if (nullCmp != 0) {
            return nullCmp;
          }
          continue;
        }
        int cmp = ExpressionEvaluator.typedCompare(lv, rv);
        if (cmp != 0) {
          return key.asc() ? cmp : -cmp;
        }
      }
      return 0;
    };
    rows.sort(comparator);
  }

  private static Map<String, Integer> buildOrderIndex(AggregatePlan plan) {
    Map<String, Integer> mapping = new HashMap<>();
    List<ResultColumn> columns = plan.resultColumns();
    for (int i = 0; i < columns.size(); i++) {
      ResultColumn column = columns.get(i);
      registerOrderKey(mapping, column.label(), i);
      if (column.isAggregate()) {
        AggregateSpec spec = plan.specs().get(column.aggregateIndex());
        registerOrderKey(mapping, spec.label(), i);
        registerOrderKey(mapping, aggregateExpressionText(spec), i);
      } else if (column.isGrouping()) {
        registerOrderKey(mapping, column.expressionText(), i);
      } else {
        GroupExpression group = plan.groupExpressions().get(column.groupIndex());
        registerOrderKey(mapping, group.label(), i);
        registerOrderKey(mapping, group.expression().toString(), i);
      }
    }
    return mapping;
  }

  private static void registerOrderKey(Map<String, Integer> mapping, String key, int index) {
    if (key == null || key.isBlank()) {
      return;
    }
    mapping.putIfAbsent(key.toLowerCase(Locale.ROOT), index);
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

  private static List<Object> buildRow(AggregatePlan plan, GroupState state, List<Object> aggregateValues) {
    List<Object> row = new ArrayList<>(plan.resultColumns().size());
    for (ResultColumn column : plan.resultColumns()) {
      if (column.isAggregate()) {
        row.add(aggregateValues.get(column.aggregateIndex()));
      } else if (column.isGrouping()) {
        row.add(groupingValue(plan, state.groupingSetIndex(), column.groupingIndexes()));
      } else {
        row.add(state.groupValues().get(column.groupIndex()));
      }
    }
    return Collections.unmodifiableList(new ArrayList<>(row));
  }

  private static Map<String, Object> buildLabelLookup(AggregatePlan plan, List<Object> rowValues,
      List<Object> groupValues) {
    Map<String, Object> map = new HashMap<>();
    List<ResultColumn> columns = plan.resultColumns();
    for (int i = 0; i < columns.size(); i++) {
      String label = columns.get(i).label();
      if (label != null && !label.isBlank()) {
        map.put(label.toLowerCase(Locale.ROOT), rowValues.get(i));
      }
    }
    List<GroupExpression> groups = plan.groupExpressions();
    for (int i = 0; i < groups.size(); i++) {
      String label = groups.get(i).label();
      if (label != null && !label.isBlank()) {
        map.putIfAbsent(label.toLowerCase(Locale.ROOT), groupValues.get(i));
      }
    }
    return map;
  }

  private static final class GroupState {
    private final List<Object> groupValues;
    private final AggregateAccumulator[] accumulators;
    private final int groupingSetIndex;

    GroupState(List<Object> groupValues, List<AggregateSpec> specs, int groupingSetIndex) {
      this.groupValues = Collections.unmodifiableList(new ArrayList<>(groupValues));
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
      return Collections.unmodifiableList(values);
    }
  }

  private static final class GroupKey {
    private final int groupingSetIndex;
    private final List<Object> values;
    private final int hash;

    GroupKey(int groupingSetIndex, List<Object> values) {
      this.groupingSetIndex = groupingSetIndex;
      this.values = Collections.unmodifiableList(new ArrayList<>(values));
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
      int cmp = ExpressionEvaluator.typedCompare(value, extremum);
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

  private static final class HavingEvaluator {
    private final AggregatePlan plan;
    private final List<Object> aggregateValues;
    private final SubqueryExecutor subqueryExecutor;
    private final Map<String, Object> labelLookup;
    private final List<String> correlatedQualifiers;
    private final int groupingSetIndex;
    private final Map<String, Integer> groupIndexLookup;

    HavingEvaluator(AggregatePlan plan, List<Object> aggregateValues, Map<String, Object> labelLookup,
        SubqueryExecutor subqueryExecutor, List<String> correlatedQualifiers, int groupingSetIndex) {
      this.plan = plan;
      this.aggregateValues = aggregateValues;
      this.labelLookup = Collections.unmodifiableMap(new HashMap<>(labelLookup));
      this.subqueryExecutor = subqueryExecutor;
      this.correlatedQualifiers = correlatedQualifiers == null ? List.of() : List.copyOf(correlatedQualifiers);
      this.groupingSetIndex = groupingSetIndex;
      this.groupIndexLookup = groupIndexLookup(plan.groupExpressions());
    }

    boolean eval(Expression expression) {
      Expression expr = ExpressionEvaluator.unwrapParenthesis(expression);
      if (expr == null) {
        return true;
      }
      if (expr instanceof AndExpression and) {
        return eval(and.getLeftExpression()) && eval(and.getRightExpression());
      }
      if (expr instanceof OrExpression or) {
        return eval(or.getLeftExpression()) || eval(or.getRightExpression());
      }
      if (expr instanceof NotExpression not) {
        return !eval(not.getExpression());
      }
      if (expr instanceof IsNullExpression isNull) {
        Object operand = value(isNull.getLeftExpression());
        boolean isNullVal = operand == null;
        return isNull.isNot() != isNullVal;
      }
      if (expr instanceof Between between) {
        Object val = value(between.getLeftExpression());
        Object low = value(between.getBetweenExpressionStart());
        Object high = value(between.getBetweenExpressionEnd());
        if (val == null || low == null || high == null) {
          return false;
        }
        int cmpLow = ExpressionEvaluator.typedCompare(val, low);
        int cmpHigh = ExpressionEvaluator.typedCompare(val, high);
        boolean in = cmpLow >= 0 && cmpHigh <= 0;
        return between.isNot() != in;
      }
      if (expr instanceof InExpression in) {
        return evalIn(in);
      }
      if (expr instanceof ExistsExpression exists) {
        return evalExists(exists);
      }
      if (expr instanceof EqualsTo eq) {
        return compare(eq.getLeftExpression(), eq.getRightExpression()) == 0;
      }
      if (expr instanceof GreaterThan gt) {
        return compare(gt.getLeftExpression(), gt.getRightExpression()) > 0;
      }
      if (expr instanceof GreaterThanEquals ge) {
        return compare(ge.getLeftExpression(), ge.getRightExpression()) >= 0;
      }
      if (expr instanceof MinorThan lt) {
        return compare(lt.getLeftExpression(), lt.getRightExpression()) < 0;
      }
      if (expr instanceof MinorThanEquals le) {
        return compare(le.getLeftExpression(), le.getRightExpression()) <= 0;
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
      throw new IllegalArgumentException("Unsupported HAVING expression: " + expr);
    }

    private boolean evalIn(InExpression in) {
      Object leftVal = value(in.getLeftExpression());
      if (leftVal == null) {
        return false;
      }
      Expression right = in.getRightExpression();
      if (right instanceof ExpressionList<?> list) {
        boolean found = false;
        for (Expression e : list) {
          Object candidate = value(e);
          if (candidate != null && ExpressionEvaluator.typedCompare(leftVal, candidate) == 0) {
            found = true;
            break;
          }
        }
        return in.isNot() != found;
      }
      if (right instanceof Select subSelect) {
        if (subqueryExecutor == null) {
          throw new IllegalStateException("IN subqueries require a subquery executor");
        }
        List<Object> subqueryValues = subqueryExecutor.execute(subSelect).firstColumnValues();
        boolean found = false;
        for (Object candidate : subqueryValues) {
          if (candidate != null && ExpressionEvaluator.typedCompare(leftVal, candidate) == 0) {
            found = true;
            break;
          }
        }
        return in.isNot() != found;
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
      CorrelatedSubqueryRewriter.Result rewritten = CorrelatedSubqueryRewriter.rewrite(subSelect, correlatedQualifiers,
          this::aliasValue);
      SubqueryExecutor.SubqueryResult result = rewritten.correlated()
          ? subqueryExecutor.executeRaw(rewritten.sql())
          : subqueryExecutor.execute(subSelect);
      boolean hasRows = !result.rows().isEmpty();
      return exists.isNot() != hasRows;
    }

    private boolean evalLike(LikeExpression like) {
      Object left = value(like.getLeftExpression());
      Object right = value(like.getRightExpression());
      if (left == null || right == null) {
        return false;
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
        matches = StringExpressions.similarTo(leftText, pattern, escapeChar);
      } else {
        boolean caseInsensitive = effective == LikeExpression.KeyWord.ILIKE;
        matches = StringExpressions.like(leftText, pattern, caseInsensitive, escapeChar);
      }
      return like.isNot() != matches;
    }

    private boolean evalBinary(BinaryExpression be) {
      String op = be.getStringExpression();
      int cmp = compare(be.getLeftExpression(), be.getRightExpression());
      return switch (op) {
        case "=" -> cmp == 0;
        case "<" -> cmp < 0;
        case ">" -> cmp > 0;
        case "<=" -> cmp <= 0;
        case ">=" -> cmp >= 0;
        case "<>", "!=" -> cmp != 0;
        default -> throw new IllegalArgumentException("Unsupported operator in HAVING clause: " + op);
      };
    }

    private int compare(Expression left, Expression right) {
      Object leftVal = value(left);
      Object rightVal = value(right);
      if (leftVal == null || rightVal == null) {
        return -1;
      }
      return ExpressionEvaluator.typedCompare(leftVal, rightVal);
    }

    private Object value(Expression expression) {
      Expression expr = ExpressionEvaluator.unwrapParenthesis(expression);
      if (expr instanceof Select subSelect) {
        return evaluateScalarSubquery(subSelect);
      }
      if (expr instanceof SignedExpression signed) {
        Object inner = value(signed.getExpression());
        if (inner == null) {
          return null;
        }
        BigDecimal numeric = toBigDecimal(inner);
        return signed.getSign() == '-' ? numeric.negate() : numeric;
      }
      if (expr instanceof Addition add) {
        return arithmetic(add.getLeftExpression(), add.getRightExpression(), Operation.ADD);
      }
      if (expr instanceof Subtraction sub) {
        return arithmetic(sub.getLeftExpression(), sub.getRightExpression(), Operation.SUB);
      }
      if (expr instanceof Multiplication mul) {
        return arithmetic(mul.getLeftExpression(), mul.getRightExpression(), Operation.MUL);
      }
      if (expr instanceof Division div) {
        return arithmetic(div.getLeftExpression(), div.getRightExpression(), Operation.DIV);
      }
      if (expr instanceof Modulo mod) {
        return arithmetic(mod.getLeftExpression(), mod.getRightExpression(), Operation.MOD);
      }
      if (expr instanceof Function func) {
        if (isGroupingFunction(func)) {
          return grouping(func);
        }
        return aggregateValue(func);
      }
      if (expr instanceof Column col) {
        return aliasValue(col.getColumnName());
      }
      return LiteralConverter.toLiteral(expr);
    }

    private Object arithmetic(Expression left, Expression right, Operation op) {
      Object leftVal = value(left);
      Object rightVal = value(right);
      if (leftVal == null || rightVal == null) {
        return null;
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

    private Object evaluateScalarSubquery(Select subSelect) {
      if (subqueryExecutor == null) {
        throw new IllegalStateException("Scalar subqueries require a subquery executor");
      }
      SubqueryExecutor.SubqueryResult result = subqueryExecutor.execute(subSelect);
      if (result.rows().isEmpty()) {
        return null;
      }
      if (result.rows().size() > 1) {
        throw new IllegalArgumentException("Scalar subquery returned more than one row");
      }
      List<Object> row = result.rows().getFirst();
      if (row.isEmpty()) {
        return null;
      }
      if (row.size() > 1) {
        throw new IllegalArgumentException("Scalar subquery returned more than one column");
      }
      return row.getFirst();
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
        if (spec.countStar() != (type == AggregateType.COUNT && func.isAllColumns())) {
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
            if (!expected.toString().equals(actual.toString())) {
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
      List<Integer> indexes = groupingArgumentIndexes(func, groupIndexLookup);
      return groupingValue(plan, groupingSetIndex, indexes);
    }

    private Object aliasValue(String name) {
      if (name == null) {
        return null;
      }
      return labelLookup.get(name.toLowerCase(Locale.ROOT));
    }

    private BigDecimal toBigDecimal(Object value) {
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

    private enum Operation {
      ADD, SUB, MUL, DIV, MOD
    }
  }

}

