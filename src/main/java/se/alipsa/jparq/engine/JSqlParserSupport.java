package se.alipsa.jparq.engine;

import java.util.concurrent.CancellationException;
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
   * Expression visitor that includes named function arguments such as POSITION's
   * IN operands and TRIM's FROM expression, which JSqlParser's default adapter
   * does not traverse. Subclasses retain control over nested query scope
   * boundaries.
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
      if (trim.getFromExpression() != null) {
        trim.getFromExpression().accept(this, context);
      }
      return super.visit(trim, context);
    }

  }

  /**
   * Base visitor for statement expressions that also descends into quantified
   * subqueries. Use with
   * {@link #visitExpressions(Select, StatementExpressionVisitor)} to connect
   * nested SELECT traversal automatically.
   */
  public abstract static class StatementExpressionVisitor extends ExpressionVisitorAdapter<Void> {
    /** Creates a visitor whose SELECT traversal is connected by the helper. */
    protected StatementExpressionVisitor() {
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
   * Parses a statement with a bounded simple-to-complex retry.
   *
   * @param sql
   *          statement text
   * @return the parsed statement
   * @throws JSQLParserException
   *           if parsing fails
   */
  public static Statement parseStatement(String sql) throws JSQLParserException {
    Statement stmt;
    try (var executor = Executors.newSingleThreadExecutor()) {
      var parser = newParser(sql);
      int allowedDepth = parser.getAsInt(Feature.allowedNestingDepth);
      try {
        stmt = CCJSqlParserUtil.parseStatement(parser.withAllowComplexParsing(false), executor);
      } catch (JSQLParserException e) {
        Throwable cause = e.getCause();
        if (cause instanceof InterruptedException || cause instanceof TimeoutException
            || cause instanceof CancellationException
            || (allowedDepth >= 0 && CCJSqlParserUtil.getNestingDepth(sql) > allowedDepth)) {
          throw e;
        }
        stmt = CCJSqlParserUtil.parseStatement(newParser(sql).withAllowComplexParsing(true), executor);
      }
    }
    return stmt;
  }

  /**
   * Parses a complete condition using the shared identifier lookahead.
   *
   * @param sql
   *          condition text
   * @return the parsed condition
   * @throws JSQLParserException
   *           if parsing fails or leaves trailing tokens
   */
  public static Expression parseCondition(String sql) throws JSQLParserException {
    try {
      return parseCondition(newParser(sql).withAllowComplexParsing(false));
    } catch (JSQLParserException e) {
      var parser = newParser(sql);
      int allowedDepth = parser.getAsInt(Feature.allowedNestingDepth);
      if (allowedDepth >= 0 && CCJSqlParserUtil.getNestingDepth(sql) > allowedDepth) {
        throw e;
      }
      return parseCondition(parser.withAllowComplexParsing(true));
    }
  }

  /**
   * Parses a condition and verifies that every token was consumed.
   *
   * @param parser
   *          a fresh configured parser
   * @return the parsed condition
   * @throws JSQLParserException
   *           if parsing fails or leaves trailing tokens
   */
  private static Expression parseCondition(CCJSqlParser parser) throws JSQLParserException {
    try {
      Expression expression = parser.Expression();
      if (parser.getNextToken().kind != CCJSqlParser.EOF) {
        throw new JSQLParserException("Unexpected trailing tokens after condition: " + expression);
      }
      return expression;
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
