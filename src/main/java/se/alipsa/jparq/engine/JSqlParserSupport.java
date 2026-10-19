package se.alipsa.jparq.engine;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.StringProvider;
import net.sf.jsqlparser.parser.TokenMgrException;
import net.sf.jsqlparser.parser.feature.Feature;
import net.sf.jsqlparser.statement.Statement;

/** Shares identifier-aware JSqlParser entry points across query planning. */
public final class JSqlParserSupport {
  private JSqlParserSupport() {
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
