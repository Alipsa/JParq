package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.JSqlParserSupport;
import se.alipsa.jparq.engine.JoinPredicatePlanner;
import se.alipsa.jparq.engine.ParquetFilterBuilder;
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
    var condition = JSqlParserSupport.parseExpression("NOT (number IS NULL)");
    assertTrue(ParquetFilterBuilder.build(schema, condition).isPresent());
    assertEquals("number IS NULL", JSqlParserSupport.parseExpression("number IS NULL").toString());
    assertThrows(net.sf.jsqlparser.JSQLParserException.class,
        () -> JSqlParserSupport.parseExpression("number > 3 trailing"));
  }

  /**
   * Keeps deep simple scalar expressions available while bounding complex retries
   * with the same policy used for complete statements.
   *
   * @throws Exception
   *           if a simple expression cannot be parsed
   */
  @Test
  void expressionRetryHonorsNestingLimit() throws Exception {
    String opening = "(".repeat(11);
    String closing = ")".repeat(11);
    assertNotNull(JSqlParserSupport.parseExpression(opening + "id + 1" + closing));
    String complex = opening + "CASE WHEN id=1 THEN id=2 ELSE id=3 END" + closing;
    assertThrows(net.sf.jsqlparser.JSQLParserException.class, () -> JSqlParserSupport.parseExpression(complex));
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
