package se.alipsa.jparq.engine;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.TrimFunction;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.StringProvider;
import net.sf.jsqlparser.parser.TokenMgrException;
import net.sf.jsqlparser.parser.feature.Feature;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PivotVisitorAdapter;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.Values;
import se.alipsa.jparq.helper.TemporalEscapeValidation;

/** Shares identifier-aware JSqlParser entry points across query planning. */
public final class JSqlParserSupport {
  private JSqlParserSupport() {
  }

  /**
   * Identifies quantified subqueries and array operands, including function calls
   * produced by JSqlParser for ANY, SOME, and ALL.
   *
   * @param expression
   *          comparison operand, or null
   * @return whether the expression is a quantified operand
   */
  public static boolean isQuantifiedOperand(Expression expression) {
    if (expression instanceof AnyComparisonExpression) {
      return true;
    }
    if (expression instanceof Function function) {
      String name = function.getName();
      return "ANY".equalsIgnoreCase(name) || "SOME".equalsIgnoreCase(name) || "ALL".equalsIgnoreCase(name);
    }
    return false;
  }

  /**
   * Visits SELECT expressions, including nested sources, CTEs, table-function
   * arguments, and LIMIT, OFFSET, and FETCH operands.
   *
   * @param statement
   *          parsed SELECT to traverse
   * @param expressions
   *          expression visitor to connect to the SELECT traversal
   */
  public static void visitExpressions(Select statement, StatementExpressionVisitor expressions) {
    FromItemVisitorAdapter<Void> sources = new FromItemVisitorAdapter<>() {
      /** {@inheritDoc} */
      @Override
      public <S> Void visit(ParenthesedSelect select, S context) {
        return select.accept(getSelectVisitor(), context);
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(LateralSubSelect select, S context) {
        return select.accept(getSelectVisitor(), context);
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(ParenthesedFromItem source, S context) {
        super.visit(source, context);
        return visitJoins(source.getJoins(), context);
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(TableFunction function, S context) {
        return function.getFunction().accept(getExpressionVisitor(), context);
      }
    };
    SelectVisitorAdapter<Void> selects = new SelectVisitorAdapter<>(expressions, new PivotVisitorAdapter<>(expressions),
        new SelectItemVisitorAdapter<>(expressions), sources) {
      /** {@inheritDoc} */
      @Override
      public <S> Void visit(SetOperationList set, S context) {
        inspectRowCounts(set, expressions);
        visitWithItems(set.getWithItemsList(), context);
        super.visit(set, context);
        expressions.visitOrderBy(set.getOrderByElements(), context);
        if (set.getOffset() != null) {
          expressions.visitExpression(set.getOffset().getOffset(), context);
        }
        if (set.getFetch() != null) {
          expressions.visitExpression(set.getFetch().getExpression(), context);
        }
        return expressions.visitLimit(set.getLimit(), context);
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(PlainSelect select, S context) {
        inspectRowCounts(select, expressions);
        super.visit(select, context);
        return expressions.visitLimit(select.getLimit(), context);
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(Values values, S context) {
        visitWithItems(values.getWithItemsList(), context);
        return expressions.visitExpressions(values.getExpressions(), context);
      }
    };
    sources.setSelectVisitor(selects);
    sources.setExpressionVisitor(expressions);
    expressions.setSelectVisitor(selects);
    statement.accept((SelectVisitor<Void>) selects, null);
  }

  /**
   * Reports row-count operands without changing expression traversal order.
   *
   * @param select
   *          the query owning the clauses
   * @param expressions
   *          the visitor receiving row-count context
   */
  private static void inspectRowCounts(Select select, StatementExpressionVisitor expressions) {
    if (select.getLimit() != null) {
      expressions.inspectRowCount(select.getLimit().getRowCount(), "LIMIT");
    }
    if (select.getOffset() != null) {
      expressions.inspectRowCount(select.getOffset().getOffset(), "OFFSET");
    }
    if (select.getFetch() != null) {
      expressions.inspectRowCount(select.getFetch().getExpression(), "FETCH");
    }
  }

  /**
   * Expression visitor that includes named function arguments such as POSITION's
   * IN operands and TRIM's FROM expression, which JSqlParser's default adapter
   * does not traverse. Subclasses retain control over nested query scope
   * boundaries. TRIM operands are visited in source order, skipping omitted trim
   * characters.
   */
  public abstract static class NamedArgumentVisitor extends ExpressionVisitorAdapter<Void> {
    /** Creates a visitor that traverses named and positional function arguments. */
    protected NamedArgumentVisitor() {
    }

    /** {@inheritDoc} */
    @Override
    public <S> Void visit(Function function, S context) {
      if (function.getNamedParameters() != null) {
        for (Expression argument : function.getNamedParameters()) {
          argument.accept(this, context);
        }
      }
      return super.visit(function, context);
    }

    /** {@inheritDoc} */
    @Override
    public <S> Void visit(TrimFunction trim, S context) {
      if (trim.getExpression() != null) {
        trim.getExpression().accept(this, context);
      }
      if (trim.getFromExpression() != null) {
        trim.getFromExpression().accept(this, context);
      }
      return null;
    }

  }

  /**
   * Base visitor for statement expressions that traverses named function
   * arguments and descends into quantified subqueries. Use with
   * {@link #visitExpressions(Select, StatementExpressionVisitor)} to connect
   * nested SELECT traversal automatically.
   */
  public abstract static class StatementExpressionVisitor extends NamedArgumentVisitor {
    /** Creates a visitor whose SELECT traversal is connected by the helper. */
    protected StatementExpressionVisitor() {
    }

    /**
     * Inspects an operand with its row-limiting clause context.
     *
     * @param expression
     *          the row-count operand, possibly null for default FETCH
     * @param clause
     *          the LIMIT, OFFSET, or FETCH clause name
     */
    protected void inspectRowCount(Expression expression, String clause) {
      // Subclasses may collect clause context independently of expression traversal.
    }

    /** {@inheritDoc} */
    @Override
    public final <S> Void visit(AnyComparisonExpression comparison, S context) {
      return comparison.getSelect().accept(getSelectVisitor(), context);
    }
  }

  /**
   * Creates a parser that preserves identifiers named after SQL types.
   *
   * @param sql
   *          statement or expression text
   * @return a fresh identifier-aware parser
   */
  public static CCJSqlParser newParser(String sql) {
    return new IdentifierAwareParser(sql);
  }

  /**
   * Parses a statement with a bounded simple-to-complex retry. JDBC temporal
   * escape literals are validated with the JDBC escape shape and strict temporal
   * conversion after a successful parse, because the parser normalizes
   * out-of-range fields and the strict helpers accept more text shapes than the
   * lenient {@code valueOf} parsers reject. When the parser rejects a literal's
   * text, the statement is re-parsed with valid placeholder escape values through
   * the same retry: a successful re-parse confirms the statement syntax and
   * reclassifies the failure as 22007 or 22008, while a failing re-parse reports
   * the syntax error instead, so syntax errors precede temporal data errors.
   *
   * @param sql
   *          statement text
   * @return the parsed statement
   * @throws JSQLParserException
   *           if parsing fails
   * @throws IllegalArgumentException
   *           if a temporal escape literal carries an invalid value, with a cause
   *           carrying SQLState 22007 or 22008
   */
  public static Statement parseStatement(String sql) throws JSQLParserException {
    Statement stmt;
    try (var executor = Executors.newSingleThreadExecutor()) {
      try {
        stmt = parseWithRetry(sql, executor);
      } catch (JSQLParserException failure) {
        if (isTemporalValueFailure(failure, sql)) {
          classifyTemporalValueFailure(sql, failure, executor);
        }
        throw failure;
      }
    }
    TemporalEscapeValidation.validate(sql);
    return stmt;
  }

  /**
   * Parses with the bounded simple-to-complex retry, rethrowing failures that
   * must not be retried: interrupted, timed-out, or cancelled parses, and parses
   * that exceed the configured nesting-depth limit.
   *
   * @param sql
   *          statement text
   * @param executor
   *          the parse executor
   * @return the parsed statement
   * @throws JSQLParserException
   *           if both parse attempts fail
   */
  private static Statement parseWithRetry(String sql, ExecutorService executor) throws JSQLParserException {
    var parser = newParser(sql);
    int allowedDepth = parser.getAsInt(Feature.allowedNestingDepth);
    try {
      return CCJSqlParserUtil.parseStatement(parser.withAllowComplexParsing(false), executor);
    } catch (JSQLParserException failure) {
      if (isRetryBlocked(failure, allowedDepth, sql)) {
        throw failure;
      }
      return CCJSqlParserUtil.parseStatement(newParser(sql).withAllowComplexParsing(true), executor);
    }
  }

  /**
   * Reports whether a parse failure must not be retried with complex parsing.
   *
   * @param failure
   *          the parser failure to inspect
   * @param allowedDepth
   *          the configured nesting-depth limit, negative for unlimited
   * @param sql
   *          the statement text that failed
   * @return whether the retry would repeat the same bounded failure
   */
  private static boolean isRetryBlocked(JSQLParserException failure, int allowedDepth, String sql) {
    Throwable cause = failure.getCause();
    if (cause instanceof InterruptedException || cause instanceof TimeoutException
        || cause instanceof CancellationException) {
      return true;
    }
    return allowedDepth >= 0 && CCJSqlParserUtil.getNestingDepth(sql) > allowedDepth;
  }

  /**
   * Reports whether a parse failure came from a temporal escape value rather than
   * from the statement syntax. The failure's cause chain must carry an
   * {@link IllegalArgumentException}, and the statement must contain a temporal
   * escape literal. The placeholder re-parse in
   * {@link #classifyTemporalValueFailure} then confirms the attribution without
   * relying on the value parser's stack frames.
   *
   * @param failure
   *          the parser failure to inspect
   * @param sql
   *          the statement text that failed
   * @return whether a temporal escape value rejected the literal text
   */
  private static boolean isTemporalValueFailure(JSQLParserException failure, String sql) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof IllegalArgumentException) {
        return TemporalEscapeValidation.hasTemporalEscape(sql);
      }
    }
    return false;
  }

  /**
   * Classifies a parse failure attributed to a temporal escape value. The
   * statement is re-parsed with valid placeholder escape values that retain every
   * token except the literal text, through the same simple-to-complex retry as
   * the main parse. When the re-parse succeeds, the syntax is valid and the
   * escape's strict validation failure is rethrown with the parse failure
   * suppressed. When the re-parse fails, a syntax error elsewhere in the
   * statement precedes the data error, so the caller reports the parse failure
   * instead.
   *
   * @param sql
   *          the statement text that failed
   * @param failure
   *          the parse failure from the lenient parse
   * @param executor
   *          the parse executor for the re-parse
   * @throws IllegalArgumentException
   *           if the syntax is valid and an escape literal fails validation, with
   *           a cause carrying SQLState 22007 or 22008
   */
  private static void classifyTemporalValueFailure(String sql, JSQLParserException failure, ExecutorService executor) {
    try {
      parseWithRetry(TemporalEscapeValidation.withPlaceholderValues(sql), executor);
    } catch (JSQLParserException syntaxError) {
      return;
    }
    try {
      TemporalEscapeValidation.validate(sql);
    } catch (IllegalArgumentException invalid) {
      invalid.addSuppressed(failure);
      throw invalid;
    }
  }

  /**
   * Parses and normalizes a complete scalar expression or condition using the
   * shared identifier lookahead. Normalization also applies to expressions
   * serialized and reparsed during grouping and predicate pushdown. Complex
   * retries honor the configured nesting-depth limit; successful simple parses
   * are not restricted by that retry limit.
   *
   * @param sql
   *          scalar expression or condition text
   * @return the parsed and normalized expression
   * @throws JSQLParserException
   *           if parsing fails or leaves trailing tokens
   */
  public static Expression parseExpression(String sql) throws JSQLParserException {
    try {
      return parseExpression(newParser(sql).withAllowComplexParsing(false));
    } catch (JSQLParserException e) {
      var parser = newParser(sql);
      int allowedDepth = parser.getAsInt(Feature.allowedNestingDepth);
      if (allowedDepth >= 0 && CCJSqlParserUtil.getNestingDepth(sql) > allowedDepth) {
        throw e;
      }
      return parseExpression(parser.withAllowComplexParsing(true));
    }
  }

  /**
   * Parses an expression and verifies that every token was consumed.
   *
   * @param parser
   *          a fresh configured parser
   * @return the parsed and normalized expression
   * @throws JSQLParserException
   *           if parsing fails or leaves trailing tokens
   */
  private static Expression parseExpression(CCJSqlParser parser) throws JSQLParserException {
    try {
      Expression expression = parser.Expression();
      if (parser.getNextToken().kind != CCJSqlParser.EOF) {
        throw new JSQLParserException("Unexpected trailing tokens after expression: " + expression);
      }
      return SqlExpressionNormalizer.normalizeExpression(expression);
    } catch (ParseException | TokenMgrException e) {
      throw new JSQLParserException(e);
    }
  }

  /**
   * Preserves type-named identifiers when no literal follows an implicit cast
   * prefix.
   */
  private static final class IdentifierAwareParser extends CCJSqlParser {
    /**
     * Creates a parser for one SQL statement.
     *
     * @param sql
     *          statement text
     */
    private IdentifierAwareParser(String sql) {
      super(new StringProvider(sql));
    }

    /**
     * Distinguishes an implicit literal cast from an identifier followed by an
     * operator.
     *
     * @return whether a supported literal or precision clause follows the type name
     */
    @Override
    protected boolean isImplicitCastAhead() {
      try {
        if (!super.isImplicitCastAhead()) {
          return false;
        }
        int next = getToken(2).kind;
        return next == S_CHAR_LITERAL || next == S_LONG || next == S_DOUBLE || next == OPENING_BRACKET;
      } catch (TokenMgrException e) {
        return false;
      }
    }
  }

}
