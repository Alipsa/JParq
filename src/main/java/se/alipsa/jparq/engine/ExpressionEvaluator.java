package se.alipsa.jparq.engine;

import static se.alipsa.jparq.engine.AvroCoercions.coerceLiteral;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.AnyType;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.engine.function.StringFunctions;
import se.alipsa.jparq.engine.window.WindowState;

/**
 * Evaluates SQL expressions (via JSqlParser) against Avro
 * {@link GenericRecord}s.
 *
 * <p>
 * Predicates follow SQL three-valued logic. UNKNOWN is preserved inside an
 * expression and excluded only at the row-selection boundary.
 * </p>
 */
public final class ExpressionEvaluator {

  /** exact field name -> schema */
  private final Map<String, Schema> fieldSchemas;
  /** lower(field name) -> canonical field name */
  private final Map<String, String> caseInsensitiveIndex;
  private final ValueExpressionEvaluator literalEvaluator;
  private final SubqueryExecutor subqueryExecutor;
  private final List<String> outerQualifiers;
  private final Map<String, Map<String, String>> qualifierColumnMapping;
  private final Map<String, String> unqualifiedColumnMapping;

  /**
   * Creates a new evaluator for the provided Avro {@link Schema}.
   *
   * @param schema
   *          the Avro schema of the records to evaluate against; must not be
   *          {@code null}
   */
  public ExpressionEvaluator(Schema schema) {
    this(schema, null, List.of(), Map.of(), Map.of());
  }

  /**
   * Creates a new evaluator for the provided Avro {@link Schema} and optional sub
   * query executor.
   *
   * @param schema
   *          the Avro schema of the records to evaluate against; must not be
   *          {@code null}
   * @param subqueryExecutor
   *          executor used for sub queries (may be {@code null})
   */
  public ExpressionEvaluator(Schema schema, SubqueryExecutor subqueryExecutor) {
    this(schema, subqueryExecutor, List.of(), Map.of(), Map.of());
  }

  /**
   * Creates a new evaluator that supports correlated sub queries.
   *
   * @param schema
   *          the Avro schema of the records to evaluate against; must not be
   *          {@code null}
   * @param subqueryExecutor
   *          executor used for sub queries (may be {@code null})
   * @param outerQualifiers
   *          table names or aliases that belong to the outer query scope
   * @param qualifierColumnMapping
   *          mapping of qualifier (table/alias) to canonical column names used
   *          during expression resolution (may be {@code null})
   * @param unqualifiedColumnMapping
   *          mapping of unqualified column names to canonical names for
   *          expressions where the column is unique across all tables (may be
   *          {@code null})
   */
  public ExpressionEvaluator(Schema schema, SubqueryExecutor subqueryExecutor, List<String> outerQualifiers,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping) {
    this(schema, subqueryExecutor, outerQualifiers, qualifierColumnMapping, unqualifiedColumnMapping,
        WindowState.empty());
  }

  /**
   * Creates a new evaluator that supports correlated sub queries and precomputed
   * analytic window state.
   *
   * @param schema
   *          the Avro schema of the records to evaluate against; must not be
   *          {@code null}
   * @param subqueryExecutor
   *          executor used for sub queries (may be {@code null})
   * @param outerQualifiers
   *          table names or aliases that belong to the outer query scope
   * @param qualifierColumnMapping
   *          mapping of qualifier (table/alias) to canonical column names used
   *          during expression resolution (may be {@code null})
   * @param unqualifiedColumnMapping
   *          mapping of unqualified column names to canonical names for
   *          expressions where the column is unique across all tables (may be
   *          {@code null})
   * @param windowState
   *          precomputed analytic function results available to value expressions
   *          (may be {@code null})
   */
  ExpressionEvaluator(Schema schema, SubqueryExecutor subqueryExecutor, List<String> outerQualifiers,
      Map<String, Map<String, String>> qualifierColumnMapping, Map<String, String> unqualifiedColumnMapping,
      WindowState windowState) {
    Map<String, Schema> fs = new HashMap<>();
    Map<String, String> ci = new HashMap<>();
    for (Schema.Field f : schema.getFields()) {
      fs.put(f.name(), f.schema());
      String key = Identifier.lookupKey(f.name());
      if (key != null) {
        ci.put(key, f.name());
      }
    }
    this.fieldSchemas = Collections.unmodifiableMap(fs);
    this.caseInsensitiveIndex = Collections.unmodifiableMap(ci);
    List<String> qualifiers = outerQualifiers == null ? List.of() : List.copyOf(outerQualifiers);
    this.qualifierColumnMapping = ColumnMappingUtil.normaliseQualifierMapping(qualifierColumnMapping);
    this.unqualifiedColumnMapping = ColumnMappingUtil.normaliseUnqualifiedMapping(unqualifiedColumnMapping);
    CorrelationMappings mappings = new CorrelationMappings(qualifiers, this.qualifierColumnMapping,
        this.unqualifiedColumnMapping, this.qualifierColumnMapping);
    this.literalEvaluator = new ValueExpressionEvaluator(schema, subqueryExecutor, mappings,
        windowState == null ? WindowState.empty() : windowState);
    this.subqueryExecutor = subqueryExecutor;
    this.outerQualifiers = qualifiers;
  }

  /**
   * Evaluates a predicate at a WHERE, ON, or CASE WHEN boundary.
   * 
   * @param expression
   *          the predicate
   * @param rec
   *          the current record
   * @return true only for SQL TRUE; UNKNOWN excludes the row
   * @throws IllegalArgumentException
   *           if the expression is unsupported
   */
  public boolean eval(Expression expression, GenericRecord rec) {
    return Boolean.TRUE.equals(evalTruth(expression, rec));
  }

  /**
   * Evaluates a predicate without collapsing SQL UNKNOWN to false.
   * 
   * @param expression
   *          the predicate
   * @param rec
   *          the current record
   * @return true, false, or null for UNKNOWN
   * @throws IllegalArgumentException
   *           if the expression is unsupported
   */
  public Boolean evalTruth(Expression expression, GenericRecord rec) {
    // Always strip all layers of (...) first
    final Expression expr = unwrapParenthesis(expression);

    if (expr instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      return evalTruth(list.getFirst(), rec);
    }
    if (expr instanceof net.sf.jsqlparser.expression.NullValue) {
      return null;
    }
    if (expr instanceof BooleanValue booleanValue) {
      return booleanValue.getValue();
    }

    // Boolean connectives
    if (expr instanceof AndExpression and) {
      return evalAnd(and, rec);
    }
    if (expr instanceof OrExpression or) {
      return evalOr(or, rec);
    }
    if (expr instanceof NotExpression not) {
      return evalNot(not, rec);
    }

    // Null checks, LIKE, BETWEEN, IN
    if (expr instanceof IsNullExpression isNull) {
      return evalIsNull(isNull, rec);
    }
    if (expr instanceof IsBooleanExpression isBoolean) {
      return evalIsBoolean(isBoolean, rec);
    }
    if (expr instanceof LikeExpression like) {
      return evalLike(like, rec);
    }
    if (expr instanceof SimilarToExpression similar) {
      return evalSimilar(similar, rec);
    }
    if (expr instanceof Between between) {
      return evalBetween(between, rec);
    }
    if (expr instanceof InExpression in) {
      return evalIn(in, rec);
    }
    if (expr instanceof ExistsExpression exists) {
      return evalExists(exists, rec);
    }

    if (expr instanceof BinaryExpression be) {
      if (involvesAnyComparison(be.getLeftExpression(), be.getRightExpression())) {
        return evalAnyComparison(be, rec);
      }
      return evalBinary(be, rec);
    }

    if (expr instanceof Column column && !isBooleanColumn(column)) {
      throw new IllegalArgumentException("Unsupported predicate: " + expr);
    }

    // Fallback for value expressions used as predicates (e.g. boolean columns):
    // evaluate the value and interpret it as a SQL truth value. Anything that
    // does not evaluate to TRUE, FALSE, or NULL is not a valid predicate.
    Object value = literalEvaluator.eval(expr, rec);
    if (value == null || value instanceof Boolean) {
      return (Boolean) value;
    }
    throw new IllegalArgumentException("Unsupported predicate: " + expr);
  }

  /**
   * Checks the declared type of a column, including nullable Boolean unions.
   *
   * @param column
   *          the column reference to resolve in this evaluator's schema
   * @return whether the resolved column has Boolean type
   */
  private boolean isBooleanColumn(Column column) {
    String qualifier = column.getTable() == null ? null : column.getTable().getName();
    Schema columnSchema = fieldSchemas.get(canonicalFieldName(qualifier, column.getColumnName()));
    return columnSchema != null && AvroCoercions.effectiveSchema(columnSchema).getType() == Schema.Type.BOOLEAN;
  }

  /**
   * Evaluate an {@code IS [NOT] TRUE} or {@code IS [NOT] FALSE} predicate.
   *
   * @param isBoolean
   *          the IS TRUE/FALSE expression
   * @param rec
   *          the current record
   * @return whether the operand's truth value matches the target value; never
   *         UNKNOWN
   */
  private Boolean evalIsBoolean(IsBooleanExpression isBoolean, GenericRecord rec) {
    Boolean truth = evalTruth(isBoolean.getLeftExpression(), rec);
    return SqlTruth.is(truth, isBoolean.isTrue(), isBoolean.isNot());
  }

  // -------------------------
  // Operator-specific handlers
  // -------------------------

  private Boolean evalAnd(AndExpression and, GenericRecord rec) {
    return SqlTruth.and(evalTruth(and.getLeftExpression(), rec), evalTruth(and.getRightExpression(), rec));
  }

  private Boolean evalOr(OrExpression or, GenericRecord rec) {
    return SqlTruth.or(evalTruth(or.getLeftExpression(), rec), evalTruth(or.getRightExpression(), rec));
  }

  private Boolean evalNot(NotExpression not, GenericRecord rec) {
    return SqlTruth.not(evalTruth(not.getExpression(), rec));
  }

  private boolean evalIsNull(IsNullExpression isNull, GenericRecord rec) {
    Operand op = operand(isNull.getLeftExpression(), rec);
    boolean isNullVal = (op.value == null);
    return isNull.isNot() != isNullVal;
  }

  private Boolean evalLike(LikeExpression like, GenericRecord rec) {
    Operand leftOperand = operand(like.getLeftExpression(), rec);
    Operand rightOperand = operand(like.getRightExpression(), rec);

    String left = (leftOperand.value == null) ? null : leftOperand.value.toString();
    String pat = (rightOperand.value == null) ? null : rightOperand.value.toString();

    if (left == null || pat == null) {
      return null;
    }

    LikeExpression.KeyWord keyWord = like.getLikeKeyWord();
    LikeExpression.KeyWord effectiveKeyword = keyWord == null ? LikeExpression.KeyWord.LIKE : keyWord;
    Character escapeChar = null;
    if (like.getEscape() != null) {
      Operand escapeOperand = operand(like.getEscape(), rec);
      String escape = escapeOperand.value == null ? null : escapeOperand.value.toString();
      if (escape != null && !escape.isEmpty()) {
        if (escape.length() != 1) {
          throw new IllegalArgumentException("LIKE escape clause must be a single character");
        }
        escapeChar = escape.charAt(0);
      }
    }
    boolean matches;
    if (effectiveKeyword == LikeExpression.KeyWord.SIMILAR_TO) {
      matches = StringFunctions.similarTo(left, pat, escapeChar);
    } else {
      boolean caseInsensitive = effectiveKeyword == LikeExpression.KeyWord.ILIKE;
      matches = StringFunctions.like(left, pat, caseInsensitive, escapeChar);
    }
    return like.isNot() != matches;
  }

  private Boolean evalSimilar(SimilarToExpression similar, GenericRecord rec) {
    Operand leftOperand = operand(similar.getLeftExpression(), rec);
    Operand rightOperand = operand(similar.getRightExpression(), rec);

    String left = (leftOperand.value == null) ? null : leftOperand.value.toString();
    String pattern = (rightOperand.value == null) ? null : rightOperand.value.toString();

    if (left == null || pattern == null) {
      return null;
    }

    String escape = similar.getEscape();
    Character escapeChar = null;
    if (escape != null && !escape.isEmpty()) {
      if (escape.length() != 1) {
        throw new IllegalArgumentException("SIMILAR TO escape clause must be a single character");
      }
      escapeChar = escape.charAt(0);
    }

    boolean matches = StringFunctions.similarTo(left, pattern, escapeChar);
    return similar.isNot() != matches;
  }

  private Boolean evalBetween(Between between, GenericRecord rec) {
    Operand val = operand(between.getLeftExpression(), rec);
    Operand lo = operand(between.getBetweenExpressionStart(), rec);
    Operand hi = operand(between.getBetweenExpressionEnd(), rec);

    Object curVal = val.value;
    Object lowVal = coerceLiteralToColumnType(lo.value, val.schemaOrNull);
    Object highVal = coerceLiteralToColumnType(hi.value, val.schemaOrNull);

    Boolean in = SqlTruth.and(SqlComparisons.compare(">=", SqlComparisons.compareOrUnknown(curVal, lowVal)),
        SqlComparisons.compare("<=", SqlComparisons.compareOrUnknown(curVal, highVal)));
    return between.isNot() ? SqlTruth.not(in) : in;
  }

  private Boolean evalIn(InExpression in, GenericRecord rec) {
    Operand left = operand(in.getLeftExpression(), rec);
    Expression right = in.getRightExpression();

    if (right instanceof ExpressionList<?> list) {
      return SqlTruth.in(left.value, list, e -> coerceLiteralToColumnType(operand(e, rec).value, left.schemaOrNull),
          in.isNot());
    }
    if (right instanceof net.sf.jsqlparser.statement.select.Select subSelect) {
      if (subqueryExecutor == null) {
        throw new IllegalStateException("IN subqueries require a subquery executor");
      }
      List<Object> values = subqueryExecutor.execute(subSelect).firstColumnValues();
      return SqlTruth.in(left.value, values, value -> coerceLiteralToColumnType(value, left.schemaOrNull), in.isNot());
    }
    return false;
  }

  private boolean evalExists(ExistsExpression exists, GenericRecord rec) {
    if (subqueryExecutor == null) {
      throw new IllegalStateException("EXISTS subqueries require a subquery executor");
    }
    if (!(exists.getRightExpression() instanceof net.sf.jsqlparser.statement.select.Select subSelect)) {
      throw new IllegalArgumentException("EXISTS requires a subquery");
    }
    CorrelatedSubqueryRewriter.Result rewritten = SubqueryCorrelatedFiltersIsolator.isolate(subSelect, outerQualifiers,
        correlationColumns(), qualifierColumnMapping,
        (qualifier, column) -> resolveColumnValue(qualifier, column, rec));
    SubqueryExecutor.SubqueryResult result = rewritten.correlated()
        ? subqueryExecutor.executeRaw(rewritten.sql())
        : subqueryExecutor.execute(subSelect);
    boolean hasRows = !result.rows().isEmpty();
    return exists.isNot() != hasRows;
  }

  private Boolean evalBinary(BinaryExpression be, GenericRecord rec) {
    Operand left = operand(be.getLeftExpression(), rec);
    Operand right = operand(be.getRightExpression(), rec);
    if (be instanceof net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression distinct) {
      return distinct.isNot() != SqlTruth.isDistinctFrom(coerceToMatch(left, right), coerceToMatch(right, left));
    }
    return SqlComparisons.compare(be.getStringExpression(), compare(left, right));
  }

  /**
   * Determine whether the provided expressions involve an {@code ANY},
   * {@code SOME}, or {@code ALL} comparison.
   *
   * @param left
   *          the left-hand expression
   * @param right
   *          the right-hand expression
   * @return {@code true} if either side is an {@link AnyComparisonExpression}
   */
  private static boolean involvesAnyComparison(Expression left, Expression right) {
    return left instanceof AnyComparisonExpression || right instanceof AnyComparisonExpression;
  }

  /**
   * Evaluate comparison expressions that use the SQL
   * {@code ANY}/{@code SOME}/{@code ALL} syntax.
   *
   * @param be
   *          the comparison expression to evaluate
   * @param rec
   *          the current record being processed
   * @return {@code true} or {@code false} when the comparison is decisive,
   *         {@code null} for UNKNOWN
   */
  private Boolean evalAnyComparison(BinaryExpression be, GenericRecord rec) {
    Expression left = be.getLeftExpression();
    Expression right = be.getRightExpression();

    if (left instanceof AnyComparisonExpression any) {
      return evalAnyComparison(any, right, be.getStringExpression(), rec, true);
    }
    if (right instanceof AnyComparisonExpression any) {
      return evalAnyComparison(any, left, be.getStringExpression(), rec, false);
    }
    throw new IllegalArgumentException("ANY/ALL evaluation requested without AnyComparisonExpression");
  }

  /**
   * Evaluate a single {@link AnyComparisonExpression} against the provided
   * expression.
   *
   * @param anyExpr
   *          the ANY/SOME/ALL expression
   * @param otherExpr
   *          the expression to compare with the results of {@code anyExpr}
   * @param operator
   *          the comparison operator ({@code =}, {@code <>}, {@code >},
   *          {@code >=}, {@code <}, or {@code <=})
   * @param rec
   *          the record currently under evaluation
   * @param anyOnLeft
   *          {@code true} if {@code anyExpr} appeared on the left-hand side of
   *          the operator
   * @return {@code true} or {@code false} when the quantified comparison is
   *         decisive, {@code null} for UNKNOWN
   */
  private Boolean evalAnyComparison(AnyComparisonExpression anyExpr, Expression otherExpr, String operator,
      GenericRecord rec, boolean anyOnLeft) {
    if (subqueryExecutor == null) {
      throw new IllegalStateException("ANY/ALL subqueries require a subquery executor");
    }

    Operand otherOperand = operand(otherExpr, rec);
    Object otherValue = otherOperand.value;

    List<Object> values = fetchAnyAllValues(anyExpr, rec);
    boolean isAll = anyExpr.getAnyType() == AnyType.ALL;
    Boolean result = isAll;
    for (Object rawValue : values) {
      Object candidate = coerceLiteralToColumnType(rawValue, otherOperand.schemaOrNull);
      Integer cmp = SqlComparisons.compareOrUnknown(anyOnLeft ? candidate : otherValue,
          anyOnLeft ? otherValue : candidate);
      Boolean comparison = SqlComparisons.compare(operator, cmp);
      result = isAll ? SqlTruth.and(result, comparison) : SqlTruth.or(result, comparison);
      if (isAll && Boolean.FALSE.equals(result) || !isAll && Boolean.TRUE.equals(result)) {
        return result;
      }
    }
    return result;
  }

  /**
   * Execute the subquery associated with an {@link AnyComparisonExpression} and
   * return the values of its first column.
   *
   * @param anyExpr
   *          the comparison expression whose subquery should be evaluated
   * @param rec
   *          the current record, used when rewriting correlated subqueries
   * @return the list of values from the first column of the subquery result
   */
  private List<Object> fetchAnyAllValues(AnyComparisonExpression anyExpr, GenericRecord rec) {
    net.sf.jsqlparser.statement.select.Select subSelect = anyExpr.getSelect();
    // For correlated subqueries, resolveColumnValue fetches the value of a
    // referenced column
    // from the current outer record (rec). This is critical for correct ANY/ALL
    // evaluation semantics,
    // as correlated column references must be resolved in the context of the
    // current row.
    CorrelatedSubqueryRewriter.Result rewritten = CorrelatedSubqueryRewriter.rewrite(subSelect, outerQualifiers,
        correlationColumns(), (qualifier, column) -> resolveColumnValue(qualifier, column, rec));

    SubqueryExecutor.SubqueryResult result = rewritten.correlated()
        ? subqueryExecutor.executeRaw(rewritten.sql())
        : subqueryExecutor.execute(subSelect);
    return result.firstColumnValues();
  }

  // -------------------------
  // Utilities
  // -------------------------

  /**
   * Recursively unwrap all {@link Parenthesis} layers from an expression.
   *
   * @param expr
   *          the expression to unwrap; must not be {@code null}
   * @return the unwrapped expression
   */
  @SuppressWarnings("removal")
  public static Expression unwrapParenthesis(Expression expr) {
    Expression unwrapped = expr;
    while (unwrapped instanceof Parenthesis p) {
      unwrapped = p.getExpression();
    }
    return unwrapped;
  }

  private record Operand(Object value, Schema schemaOrNull) {
  }

  private Object resolveColumnValue(String qualifier, String columnName, GenericRecord rec) {
    String canonical = canonicalFieldName(qualifier, columnName);
    return AvroCoercions.resolveColumnValue(canonical, rec, fieldSchemas, caseInsensitiveIndex);
  }

  private String canonicalFieldName(String qualifier, String columnName) {
    return ColumnMappingUtil.canonicalFieldName(qualifier, columnName, qualifierColumnMapping, unqualifiedColumnMapping,
        caseInsensitiveIndex);
  }

  private Set<String> correlationColumns() {
    if (qualifierColumnMapping.isEmpty()) {
      return Set.of();
    }
    Set<String> columns = new LinkedHashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : qualifierColumnMapping.entrySet()) {
      Map<String, String> mapping = entry.getValue();
      if (mapping != null) {
        for (String key : mapping.keySet()) {
          if (!caseInsensitiveIndex.containsKey(key)) {
            columns.add(key);
          }
        }
      }
    }
    return Set.copyOf(columns);
  }

  private Operand operand(Expression e, GenericRecord rec) {
    Expression expr = unwrapParenthesis(e);
    if (expr instanceof ParenthesedExpressionList<?> pel) {
      if (!pel.isEmpty()) {
        return operand(pel.getFirst(), rec);
      }
      return new Operand(null, null);
    }
    if (expr instanceof Column c) {
      String qualifier = c.getTable() == null ? null : c.getTable().getName();
      String name = c.getColumnName();
      String lookupName = canonicalFieldName(qualifier, name);
      Schema colSchema = fieldSchemas.get(lookupName);
      if (colSchema == null) {
        return new Operand(null, null);
      }
      Object v = AvroCoercions.unwrap(rec.get(lookupName), colSchema);
      return new Operand(v, colSchema);
    }
    Object literal = literalEvaluator.eval(expr, rec);
    return new Operand(literal, null);
  }

  /**
   * Coerce a literal value to the provided column schema, if present.
   *
   * @param value
   *          the literal value to coerce (may be {@code null})
   * @param columnSchemaOrNull
   *          the schema of the comparison column, or {@code null} when comparing
   *          two literals
   * @return the coerced literal when a schema is supplied; otherwise the original
   *         value
   */
  private static Object coerceLiteralToColumnType(Object value, Schema columnSchemaOrNull) {
    return columnSchemaOrNull == null ? value : coerceLiteral(value, columnSchemaOrNull);
  }

  /**
   * Compare two values using SQL-style type coercion rules. This compatibility
   * entry point delegates to {@link SqlComparisons}.
   *
   * @param l
   *          left-hand value
   * @param r
   *          right-hand value
   * @return negative when {@code l < r}, zero when equal, positive when greater
   * @throws IllegalArgumentException
   *           if the values cannot be compared
   * @throws NullPointerException
   *           if either value is null
   * @see SqlComparisons#typedCompare(Object, Object)
   */
  public static int typedCompare(Object l, Object r) {
    return SqlComparisons.typedCompare(l, r);
  }

  /**
   * Coerces the source operand's value to the target operand's column schema when
   * the target is a column and the source is not.
   *
   * @param source
   *          the operand supplying the value
   * @param target
   *          the operand whose column schema drives the coercion
   * @return the coerced source value, or the value unchanged when no coercion
   *         applies
   */
  private static Object coerceToMatch(Operand source, Operand target) {
    if (target.schemaOrNull != null && source.schemaOrNull == null) {
      return coerceLiteral(source.value, target.schemaOrNull);
    }
    return source.value;
  }

  /**
   * Compare two pre-evaluated operands using SQL-style type coercion rules.
   *
   * @param leftOperand
   *          the evaluated left-hand operand
   * @param rightOperand
   *          the evaluated right-hand operand
   * @return negative when {@code left < right}, zero when equal, positive when
   *         greater, or {@code null} when the comparison is UNKNOWN (a null or
   *         uncomparable operand)
   */
  private Integer compare(Operand leftOperand, Operand rightOperand) {
    return SqlComparisons.compareOrUnknown(coerceToMatch(leftOperand, rightOperand),
        coerceToMatch(rightOperand, leftOperand));
  }
}
