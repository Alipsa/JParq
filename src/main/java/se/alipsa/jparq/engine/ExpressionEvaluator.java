package se.alipsa.jparq.engine;

import static se.alipsa.jparq.engine.AvroCoercions.coerceLiteral;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.AnyType;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.Parenthesis;
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
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.helper.StringExpressions;

/**
 * Evaluates SQL expressions (via JSqlParser) against Avro
 * {@link GenericRecord}s.
 *
 * <p>
 * This class performs a <em>no-semantics-change</em> evaluation compared to the
 * original implementation: it preserves the existing coercion rules, comparison
 * behavior (including the treatment of {@code null}), and special parsing of
 * fully parenthesized expressions, while improving readability and cohesion.
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
    Map<String, Schema> fs = new HashMap<>();
    Map<String, String> ci = new HashMap<>();
    for (Schema.Field f : schema.getFields()) {
      fs.put(f.name(), f.schema());
      ci.put(f.name().toLowerCase(Locale.ROOT), f.name());
    }
    this.fieldSchemas = Collections.unmodifiableMap(fs);
    this.caseInsensitiveIndex = Collections.unmodifiableMap(ci);
    List<String> qualifiers = outerQualifiers == null ? List.of() : List.copyOf(outerQualifiers);
    this.qualifierColumnMapping = ColumnMappingUtil.normaliseQualifierMapping(qualifierColumnMapping);
    this.unqualifiedColumnMapping = ColumnMappingUtil.normaliseUnqualifiedMapping(unqualifiedColumnMapping);
    this.literalEvaluator = new ValueExpressionEvaluator(schema, subqueryExecutor, qualifiers,
        this.qualifierColumnMapping, this.unqualifiedColumnMapping);
    this.subqueryExecutor = subqueryExecutor;
    this.outerQualifiers = qualifiers;
  }

  /**
   * Evaluate the given SQL expression against the provided {@link GenericRecord}.
   *
   * <p>
   * Notes on behavior (unchanged from the original):
   * <ul>
   * <li>All layers of parentheses are unwrapped before evaluation.</li>
   * <li>If the expression text is fully parenthesized (e.g., {@code "(...)"}) it
   * is re-parsed with {@code CCJSqlParserUtil.parseCondExpression} as a
   * best-effort and then evaluated.</li>
   * <li>When comparing a column to a literal, the literal is coerced to the
   * column's Avro type.</li>
   * <li>{@code null} participates as "non-match" in comparisons (returns
   * {@code -1} internally).</li>
   * <li>LIKE supports {@code %} (any sequence) and {@code _} (single char); ILIKE
   * is case-insensitive.</li>
   * </ul>
   *
   * @param expression
   *          the expression to evaluate; must not be {@code null}
   * @param rec
   *          the Avro record to evaluate against; must not be {@code null}
   * @return {@code true} if the expression evaluates to {@code true}; otherwise
   *         {@code false}
   * @throws IllegalArgumentException
   *           if the expression type/operator is unsupported
   */
  @SuppressWarnings({
      "PMD.LooseCoupling", "PMD.EmptyCatchBlock"
  })
  public boolean eval(Expression expression, GenericRecord rec) {
    // Always strip all layers of (...) first
    final Expression expr = unwrapParenthesis(expression);

    // Special-case: best-effort to re-parse "( ... )" to a condition expression
    // (kept for exact behavioral parity with the original implementation)
    if (isFullyParenthesized(expr)) {
      try {
        Expression inner = CCJSqlParserUtil.parseCondExpression(stripOuterParens(expr.toString()));
        return eval(inner, rec);
      } catch (JSQLParserException ignore) {
        // fall back to standard handlers below
      }
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

    // Comparison operators
    if (expr instanceof EqualsTo e) {
      if (involvesAnyComparison(e.getLeftExpression(), e.getRightExpression())) {
        return evalAnyComparison(e, rec);
      }
      return compare(e.getLeftExpression(), e.getRightExpression(), rec) == 0;
    }
    if (expr instanceof GreaterThan gt) {
      if (involvesAnyComparison(gt.getLeftExpression(), gt.getRightExpression())) {
        return evalAnyComparison(gt, rec);
      }
      return compare(gt.getLeftExpression(), gt.getRightExpression(), rec) > 0;
    }
    if (expr instanceof MinorThan lt) {
      if (involvesAnyComparison(lt.getLeftExpression(), lt.getRightExpression())) {
        return evalAnyComparison(lt, rec);
      }
      return compare(lt.getLeftExpression(), lt.getRightExpression(), rec) < 0;
    }
    if (expr instanceof GreaterThanEquals ge) {
      if (involvesAnyComparison(ge.getLeftExpression(), ge.getRightExpression())) {
        return evalAnyComparison(ge, rec);
      }
      return compare(ge.getLeftExpression(), ge.getRightExpression(), rec) >= 0;
    }
    if (expr instanceof MinorThanEquals le) {
      if (involvesAnyComparison(le.getLeftExpression(), le.getRightExpression())) {
        return evalAnyComparison(le, rec);
      }
      return compare(le.getLeftExpression(), le.getRightExpression(), rec) <= 0;
    }

    // Generic fallback for binary comparisons (covers parenthesized variants too)
    if (expr instanceof BinaryExpression be) {
      if (involvesAnyComparison(be.getLeftExpression(), be.getRightExpression())) {
        return evalAnyComparison(be, rec);
      }
      return evalBinary(be, rec);
    }

    throw new IllegalArgumentException("Unsupported WHERE expression: " + expr);
  }

  // -------------------------
  // Operator-specific handlers
  // -------------------------

  private boolean evalAnd(AndExpression and, GenericRecord rec) {
    return eval(and.getLeftExpression(), rec) && eval(and.getRightExpression(), rec);
  }

  private boolean evalOr(OrExpression or, GenericRecord rec) {
    return eval(or.getLeftExpression(), rec) || eval(or.getRightExpression(), rec);
  }

  private boolean evalNot(NotExpression not, GenericRecord rec) {
    // unwrap happens in the next eval call
    return !eval(not.getExpression(), rec);
  }

  private boolean evalIsNull(IsNullExpression isNull, GenericRecord rec) {
    Operand op = operand(isNull.getLeftExpression(), rec);
    boolean isNullVal = (op.value == null);
    return isNull.isNot() != isNullVal;
  }

  private boolean evalLike(LikeExpression like, GenericRecord rec) {
    Operand leftOperand = operand(like.getLeftExpression(), rec);
    Operand rightOperand = operand(like.getRightExpression(), rec);

    String left = (leftOperand.value == null) ? null : leftOperand.value.toString();
    String pat = (rightOperand.value == null) ? null : rightOperand.value.toString();

    if (left == null || pat == null) {
      return false;
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
      matches = StringExpressions.similarTo(left, pat, escapeChar);
    } else {
      boolean caseInsensitive = effectiveKeyword == LikeExpression.KeyWord.ILIKE;
      matches = StringExpressions.like(left, pat, caseInsensitive, escapeChar);
    }
    return like.isNot() != matches;
  }

  private boolean evalSimilar(SimilarToExpression similar, GenericRecord rec) {
    Operand leftOperand = operand(similar.getLeftExpression(), rec);
    Operand rightOperand = operand(similar.getRightExpression(), rec);

    String left = (leftOperand.value == null) ? null : leftOperand.value.toString();
    String pattern = (rightOperand.value == null) ? null : rightOperand.value.toString();

    if (left == null || pattern == null) {
      return false;
    }

    String escape = similar.getEscape();
    Character escapeChar = null;
    if (escape != null && !escape.isEmpty()) {
      if (escape.length() != 1) {
        throw new IllegalArgumentException("SIMILAR TO escape clause must be a single character");
      }
      escapeChar = escape.charAt(0);
    }

    boolean matches = StringExpressions.similarTo(left, pattern, escapeChar);
    return similar.isNot() != matches;
  }

  private boolean evalBetween(Between between, GenericRecord rec) {
    Operand val = operand(between.getLeftExpression(), rec);
    Operand lo = operand(between.getBetweenExpressionStart(), rec);
    Operand hi = operand(between.getBetweenExpressionEnd(), rec);

    Object curVal = val.value;
    Object lowVal = coerceLiteralToColumnType(lo.value, val.schemaOrNull);
    Object highVal = coerceLiteralToColumnType(hi.value, val.schemaOrNull);

    if (curVal == null || lowVal == null || highVal == null) {
      return false;
    }

    int cmpLo = typedCompare(curVal, lowVal);
    int cmpHi = typedCompare(curVal, highVal);
    boolean in = (cmpLo >= 0 && cmpHi <= 0);
    return between.isNot() != in;
  }

  private boolean evalIn(InExpression in, GenericRecord rec) {
    Operand left = operand(in.getLeftExpression(), rec);
    Expression right = in.getRightExpression();

    if (right instanceof ExpressionList<?> list) {
      boolean found = false;
      for (Expression e : list) {
        Operand rightOperand = operand(e, rec);

        Object leftVal = left.value;
        Object rightVal = coerceLiteralToColumnType(rightOperand.value, left.schemaOrNull);

        if (leftVal != null && rightVal != null && typedCompare(leftVal, rightVal) == 0) {
          found = true;
          break;
        }
      }
      return in.isNot() != found;
    }
    if (right instanceof net.sf.jsqlparser.statement.select.Select subSelect) {
      if (subqueryExecutor == null) {
        throw new IllegalStateException("IN subqueries require a subquery executor");
      }
      List<Object> values = subqueryExecutor.execute(subSelect).firstColumnValues();
      boolean found = false;
      for (Object value : values) {
        Object rightVal = coerceLiteralToColumnType(value, left.schemaOrNull);
        Object leftVal = left.value;
        if (leftVal != null && rightVal != null && typedCompare(leftVal, rightVal) == 0) {
          found = true;
          break;
        }
      }
      return in.isNot() != found;
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
    CorrelatedSubqueryRewriter.Result rewritten = CorrelatedSubqueryRewriter.rewrite(subSelect, outerQualifiers,
        column -> resolveColumnValue(column, rec));
    SubqueryExecutor.SubqueryResult result = rewritten.correlated()
        ? subqueryExecutor.executeRaw(rewritten.sql())
        : subqueryExecutor.execute(subSelect);
    boolean hasRows = !result.rows().isEmpty();
    return exists.isNot() != hasRows;
  }

  private boolean evalBinary(BinaryExpression be, GenericRecord rec) {
    int cmp = compare(be.getLeftExpression(), be.getRightExpression(), rec);
    String op = be.getStringExpression(); // "=", "<", ">", "<=", ">=", "<>", "!="
    return switch (op) {
      case "=" -> cmp == 0;
      case "<" -> cmp < 0;
      case ">" -> cmp > 0;
      case "<=" -> cmp <= 0;
      case ">=" -> cmp >= 0;
      case "<>", "!=" -> cmp != 0;
      default -> throw new IllegalArgumentException("Unsupported operator: " + op);
    };
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
   * @return {@code true} if the comparison evaluates to {@code true}, otherwise
   *         {@code false}
   */
  private boolean evalAnyComparison(BinaryExpression be, GenericRecord rec) {
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
   * @return {@code true} if the comparison succeeds, otherwise {@code false}
   */
  private boolean evalAnyComparison(AnyComparisonExpression anyExpr, Expression otherExpr, String operator,
      GenericRecord rec, boolean anyOnLeft) {
    if (subqueryExecutor == null) {
      throw new IllegalStateException("ANY/ALL subqueries require a subquery executor");
    }

    Operand otherOperand = operand(otherExpr, rec);
    Object otherValue = otherOperand.value;

    if (otherValue == null) {
      return false;
    }

    List<Object> values = fetchAnyAllValues(anyExpr, rec);
    if (values.isEmpty()) {
      // Per SQL semantics, ALL comparisons over an empty set return TRUE (vacuously
      // true),
      // while ANY/SOME comparisons over an empty set return FALSE.
      // See SQL:2016, section 8.2 <quantified comparison predicate> and 8.3 <in
      // predicate>.
      return anyExpr.getAnyType() == AnyType.ALL;
    }

    boolean isAll = anyExpr.getAnyType() == AnyType.ALL;
    boolean anyMatch = false;
    boolean allMatch = true;
    Schema comparisonSchema = otherOperand.schemaOrNull;

    for (Object rawValue : values) {
      Object candidate = comparisonSchema == null ? rawValue : coerceLiteralToColumnType(rawValue, comparisonSchema);

      if (candidate == null) {
        allMatch = false;
        continue;
      }

      Object leftVal = anyOnLeft ? candidate : otherValue;
      Object rightVal = anyOnLeft ? otherValue : candidate;

      if (rightVal == null) {
        allMatch = false;
        continue;
      }

      int cmp;
      try {
        cmp = typedCompare(leftVal, rightVal);
      } catch (Exception ex) {
        allMatch = false;
        continue;
      }

      boolean comparisonResult = applyComparisonOperator(operator, cmp);
      if (comparisonResult) {
        anyMatch = true;
      } else {
        allMatch = false;
      }

      if (!isAll && anyMatch) {
        return true;
      }
    }

    return isAll ? allMatch : anyMatch;
  }

  /**
   * Apply the provided comparison operator to a {@code compareTo}-style result.
   *
   * @param operator
   *          textual representation of the operator
   * @param cmp
   *          comparison result ({@code <0}, {@code 0}, {@code >0})
   * @return {@code true} if the operator holds for {@code cmp}, otherwise
   *         {@code false}
   */
  private static boolean applyComparisonOperator(String operator, int cmp) {
    return switch (operator) {
      case "=" -> cmp == 0;
      case "<" -> cmp < 0;
      case ">" -> cmp > 0;
      case "<=" -> cmp <= 0;
      case ">=" -> cmp >= 0;
      case "<>", "!=" -> cmp != 0;
      default -> throw new IllegalArgumentException("Unsupported operator for ANY/ALL: " + operator);
    };
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
        column -> resolveColumnValue(column, rec));

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

  private static boolean isFullyParenthesized(Expression expr) {
    String txt = expr.toString().trim();
    return txt.length() >= 2 && txt.charAt(0) == '(' && txt.charAt(txt.length() - 1) == ')';
  }

  private static String stripOuterParens(String s) {
    return s.substring(1, s.length() - 1);
  }

  private record Operand(Object value, Schema schemaOrNull) {
  }

  private Object resolveColumnValue(String columnName, GenericRecord rec) {
    if (columnName == null) {
      return null;
    }
    String qualifier = null;
    String name = columnName;
    int dot = columnName.indexOf('.');
    if (dot > 0) {
      qualifier = columnName.substring(0, dot);
      name = columnName.substring(dot + 1);
    }
    String canonical = canonicalFieldName(qualifier, name);
    return AvroCoercions.resolveColumnValue(canonical, rec, fieldSchemas, caseInsensitiveIndex);
  }

  private String canonicalFieldName(String qualifier, String columnName) {
    return ColumnMappingUtil.canonicalFieldName(qualifier, columnName, qualifierColumnMapping, unqualifiedColumnMapping,
        caseInsensitiveIndex);
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

  static int typedCompare(Object l, Object r) {
    if (l instanceof Number && r instanceof Number) {
      return new BigDecimal(l.toString()).compareTo(new BigDecimal(r.toString()));
    }
    if (l instanceof Boolean && r instanceof Boolean) {
      return Boolean.compare((Boolean) l, (Boolean) r);
    }
    if (l instanceof byte[] && r instanceof byte[]) {
      return compareBinary((byte[]) l, (byte[]) r);
    }
    if (l instanceof java.nio.ByteBuffer lb && r instanceof java.nio.ByteBuffer rb) {
      byte[] la = new byte[lb.remaining()];
      byte[] ra = new byte[rb.remaining()];
      lb.duplicate().get(la);
      rb.duplicate().get(ra);
      return compareBinary(la, ra);
    }
    if (l instanceof Timestamp && r instanceof Timestamp) {
      return Long.compare(((Timestamp) l).getTime(), ((Timestamp) r).getTime());
    }
    if (l instanceof Date && r instanceof Date) {
      return Long.compare(((Date) l).getTime(), ((Date) r).getTime());
    }
    if (l instanceof se.alipsa.jparq.helper.TemporalInterval li
        && r instanceof se.alipsa.jparq.helper.TemporalInterval ri) {
      return li.compareTo(ri);
    }
    return l.toString().compareTo(r.toString());
  }

  /**
   * Compare two byte arrays using lexicographic ordering.
   *
   * @param left
   *          left-hand value, may be {@code null}
   * @param right
   *          right-hand value, may be {@code null}
   * @return negative when {@code left < right}, zero when equal, otherwise positive
   */
  private static int compareBinary(byte[] left, byte[] right) {
    if (left == right) {
      return 0;
    }
    if (left == null) {
      return -1;
    }
    if (right == null) {
      return 1;
    }
    int cmp = Arrays.compare(left, right);
    if (cmp != 0) {
      return cmp;
    }
    return Integer.compare(left.length, right.length);
  }

  private int compare(Expression leftExpr, Expression rightExpr, GenericRecord rec) {
    Operand leftOperand = operand(leftExpr, rec);
    Operand rightOperand = operand(rightExpr, rec);

    Object leftVal = leftOperand.value;
    Object rightVal = rightOperand.value;

    // If one side is a column (has schema) and the other is a literal,
    // then coerce literal to column type
    if (leftOperand.schemaOrNull != null && rightOperand.schemaOrNull == null) {
      rightVal = coerceLiteral(rightVal, leftOperand.schemaOrNull);
    } else if (rightOperand.schemaOrNull != null && leftOperand.schemaOrNull == null) {
      leftVal = coerceLiteral(leftVal, rightOperand.schemaOrNull);
    }

    if (leftVal == null || rightVal == null) {
      // original behavior: nulls don't match
      return -1;
    }

    try {
      return typedCompare(leftVal, rightVal);
    } catch (Exception e) {
      // original behavior: swallow and treat as non-match
      return -1;
    }
  }
}
