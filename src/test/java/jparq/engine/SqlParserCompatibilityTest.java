package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.JSqlParserSupport;
import se.alipsa.jparq.engine.JoinPredicatePlanner;
import se.alipsa.jparq.engine.ParquetFilterBuilder;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.helper.LiteralConverter;

/**
 * Checks identifier lookahead and the bounded simple-to-complex parser
 * fallback.
 */
class SqlParserCompatibilityTest {
  /**
   * Keeps type-named columns available as identifiers.
   *
   * @param name
   *          a column name also recognized as a SQL type
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "number", "integer", "double", "varchar"
  })
  void typeNamedColumnsRemainIdentifiers(String name) {
    var select = SqlParser.parseSelect("SELECT " + name + " FROM input ORDER BY " + name + " DESC");
    var column = assertInstanceOf(Column.class, select.expressions().getFirst());
    assertEquals(name, column.getColumnName());
    assertEquals(name, select.orderBy().getFirst().column());
  }

  /** Retries Boolean CASE branches with a fresh parser in complex mode. */
  @Test
  void complexCaseBranchesRemainAvailable() {
    assertNotNull(SqlParser.parseSelect("SELECT CASE WHEN id=1 THEN id=2 ELSE id=3 END FROM input"));
  }

  /**
   * Collects row-count bindings while validating through the shared complex
   * parser retry.
   */
  @Test
  void parameterCollectionUsesSharedValidation() {
    String sql = "WITH c AS (SELECT ? AS n) "
        + "SELECT CASE WHEN n = 1 THEN n = 2 ELSE n = 3 END FROM c LIMIT ? OFFSET ? ROWS";
    assertEquals(Map.of(2, "LIMIT", 3, "OFFSET"), SqlParser.rowCountParameters(sql));
    assertTrue(SqlParser.rowCountParameters("SELECT ? AS n").isEmpty());
    assertThrows(IllegalArgumentException.class, () -> SqlParser.rowCountParameters("SELECT 1 LIMIT ~1"));
    assertThrows(IllegalArgumentException.class,
        () -> SqlParser.rowCountParameters("SELECT CAST(? AS DECIMAL(2,5)) LIMIT ?"));
  }

  /**
   * Rejects rewritten row-count indexes without modifying the caller's parameter
   * context.
   */
  @Test
  void cteRewriteRejectsUnsafeRowCountParameterIndexes() {
    String sql = "WITH c AS (SELECT model FROM mtcars LIMIT ?) SELECT ? AS tag, model FROM c";
    assertEquals(Map.of(1, "LIMIT"), SqlParser.rowCountParameters(sql));
    Map<Integer, String> clauses = new LinkedHashMap<>(Map.of(99, "OFFSET"));
    var error = assertThrows(IllegalArgumentException.class,
        () -> SqlParser.tryParseWithCteWorkaround(sql, true, clauses));
    var classified = assertInstanceOf(SQLFeatureNotSupportedException.class, SqlExceptions.findSqlStateFailure(error));
    assertEquals("0A000", classified.getSQLState());
    assertTrue(classified.getMessage().contains("reorder parameter indexes"));
    assertEquals(Map.of(99, "OFFSET"), clauses);
  }

  /**
   * Accepts parameterized rewrites whose markers are not row counts.
   */
  @Test
  void cteRewriteAcceptsOrdinaryParameterIndexes() {
    Map<Integer, String> clauses = new LinkedHashMap<>();
    String sql = "WITH c AS (SELECT ? AS n) SELECT ? AS tag, n FROM c";
    assertNotNull(SqlParser.tryParseWithCteWorkaround(sql, true, clauses));
    assertNull(SqlParser.tryParseWithCteWorkaround("SELECT 1", true, clauses));
    assertTrue(clauses.isEmpty());
  }

  /**
   * Rewrites quoted and unquoted declarations with implicit or explicit aliases
   * and literal counts.
   *
   * @param name
   *          the CTE name as written in SQL
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "c", "\"c\"", "\"MyC\""
  })
  void cteRewritePreservesNamesAndAliases(String name) {
    for (String alias : List.of("", " x", " AS x")) {
      String sql = "WITH " + name + " AS (SELECT ? AS n LIMIT 1) SELECT ? AS tag, n FROM " + name + alias;
      Map<Integer, String> clauses = new LinkedHashMap<>();
      assertNotNull(SqlParser.tryParseWithCteWorkaround(sql, true, clauses), sql);
      assertTrue(clauses.isEmpty(), sql);
    }
  }

  /** Keeps replacement-sensitive literal text intact during the rewrite. */
  @Test
  void cteRewritePreservesLiteralText() {
    var query = assertInstanceOf(SqlParser.Select.class, SqlParser
        .tryParseWithCteWorkaround("WITH c AS (SELECT '$1' AS n) SELECT n FROM c", true, new LinkedHashMap<>()));
    var literal = assertInstanceOf(StringValue.class,
        query.tableReferences().getFirst().subquery().expressions().getFirst());
    assertEquals("$1", literal.getValue());
  }

  /** Limits complex retries without restricting successful simple parses. */
  @Test
  void complexRetryHonorsNestingLimit() {
    String opening = "(".repeat(11);
    String closing = ")".repeat(11);
    assertNotNull(SqlParser.parseSelect("SELECT " + opening + "id + 1" + closing + " FROM input"));
    String complex = "SELECT " + opening + "CASE WHEN id=1 THEN id=2 ELSE id=3 END" + closing + " FROM input";
    assertThrows(IllegalArgumentException.class, () -> SqlParser.parseSelect(complex));
  }

  /** Reports malformed tokens as parse failures. */
  @Test
  void malformedImplicitCastFailsClearly() {
    assertThrows(IllegalArgumentException.class,
        () -> SqlParser.parseSelect("SELECT INTEGER 'unterminated FROM input"));
  }

  /**
   * Rejects unterminated quotes even when a valid table-free prefix was parsed.
   *
   * @param sql
   *          malformed SQL containing an unclosed literal or identifier
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT 1, 'unterminated", "SELECT \"unterminated", "SELECT 'escaped''quote"
  })
  void tableFreeSelectRejectsUnterminatedQuotes(String sql) {
    assertThrows(IllegalArgumentException.class, () -> SqlParser.parseSelect(sql));
  }

  /**
   * Keeps type-named columns eligible for join and negated-NULL pushdown.
   *
   * @throws Exception
   *           if condition parsing fails
   */
  @Test
  void typeNamedColumnsKeepPushdown() throws Exception {
    var schema = SchemaBuilder.record("input").fields().optionalInt("number").endRecord();
    var other = SchemaBuilder.record("other").fields().requiredInt("id").endRecord();
    var query = SqlParser.parseSelect("SELECT number FROM input i JOIN other o ON i.number = o.id WHERE number > 3");
    assertTrue(
        JoinPredicatePlanner.plan(query.where(), query.tableReferences(), List.of(schema, other), 0).isPresent());
    var condition = JSqlParserSupport.parseCondition("NOT (number IS NULL)");
    assertTrue(ParquetFilterBuilder.build(schema, condition).isPresent());
    assertEquals("number IS NULL", JSqlParserSupport.parseCondition("number IS NULL").toString());
    assertThrows(net.sf.jsqlparser.JSQLParserException.class,
        () -> JSqlParserSupport.parseCondition("number > 3 trailing"));
  }

  /** Preserves literal casts with and without precision parameters. */
  @Test
  void implicitCastsRemainAvailable() {
    var select = SqlParser.parseSelect("SELECT INTEGER '42', DECIMAL(8, 2) '3.14' FROM input");
    var integer = assertInstanceOf(CastExpression.class, select.expressions().getFirst());
    var decimal = assertInstanceOf(CastExpression.class, select.expressions().get(1));
    assertEquals("INTEGER", integer.getColDataType().getDataType());
    assertEquals("DECIMAL (8, 2)", decimal.getColDataType().getDataType());
    assertEquals(42, assertInstanceOf(Number.class, LiteralConverter.toLiteral(integer)).intValue());
    assertEquals(new BigDecimal("3.14"), LiteralConverter.toLiteral(decimal));
  }
}
