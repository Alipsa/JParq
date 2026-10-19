package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.math.BigDecimal;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.schema.Column;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.helper.LiteralConverter;

/** Checks compatibility with JSqlParser's implicit cast lookahead. */
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
