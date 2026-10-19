package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
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
