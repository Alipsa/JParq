package jparq;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.ArrayQueryValidation;
import se.alipsa.jparq.engine.Identifier;
import se.alipsa.jparq.engine.MaterializedNames;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/**
 * Tests declaration binding directly, independently of derived-table execution.
 */
class ArrayQueryValidationTest {
  private final Map<String, Schema> tables = Map.of("s",
      SchemaBuilder.record("outer").fields().requiredInt("id").name("items").type().array().items().stringType()
          .noDefault().endRecord(),
      "t2", SchemaBuilder.record("inner").fields().requiredInt("k").endRecord(), "t3",
      SchemaBuilder.record("other").fields().requiredInt("id").endRecord(), "lefts",
      SchemaBuilder.record("lefts").fields().requiredInt("id").requiredString("text").endRecord(), "rights",
      SchemaBuilder.record("rights").fields().requiredInt("id").requiredString("text").endRecord());

  /**
   * Wildcards expose only their own inputs and do not hide later explicit items.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void wildcardMissesContinueThroughTheProjection() throws Exception {
    for (String wildcard : List.of("*", "t2.*")) {
      for (String projection : List.of(wildcard + ", 'y' AS id", "'y' AS id, " + wildcard)) {
        String sql = "SELECT (SELECT ARRAY[d.id, 'x'] FROM (SELECT " + projection + " FROM t2) d) AS v FROM s";
        Select query = parse(sql);
        assertDoesNotThrow(() -> ArrayQueryValidation.validate(query, table -> tables.get(table.getName())), sql);
        assertEquals(Types.VARCHAR, elementType(query), sql);
      }
    }
    Select qualified = parse(
        "SELECT (SELECT ARRAY[d.id, 'x'] " + "FROM (SELECT t2.*, 'y' AS id FROM t2 CROSS JOIN t3) d) FROM s");
    assertEquals(Types.VARCHAR, elementType(qualified));
    Select cte = parse("WITH c AS (SELECT *, 'y' AS id FROM t2) " + "SELECT (SELECT ARRAY[d.id, 'x'] FROM c d) FROM s");
    assertEquals(Types.VARCHAR, elementType(cte));
  }

  /**
   * A derived wildcard cannot export an outer name; ordinary scalar expressions
   * can still correlate unqualified references.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void wildcardOwnershipDiffersFromExpressionCorrelation() throws Exception {
    for (String wildcard : List.of("*", "t2.*")) {
      Select missing = parse("SELECT (SELECT d.id FROM (SELECT " + wildcard + " FROM t2) d) FROM s");
      assertNull(schema(missing));
      Select correlated = parse("SELECT (SELECT id FROM (SELECT " + wildcard + " FROM t2) d) FROM s");
      assertEquals(Types.INTEGER, JdbcTypeMapper.mapSchemaToJdbcType(schema(correlated)));
    }
    Select localWildcard = parse(
        "SELECT (SELECT ARRAY[d.id, 1] FROM (SELECT * FROM t3) d) FROM " + "(SELECT 'outer' AS id) s");
    assertEquals(Types.INTEGER, elementType(localWildcard));
    Select duplicate = parse("SELECT d.id FROM (SELECT *, 'y' AS id FROM t3) d");
    assertNull(schema(duplicate));
  }

  /**
   * Positional output aliases must not be assigned by unexpanded select-item
   * indexes.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void wildcardOutputAliasesDoNotGuessPositions() throws Exception {
    Schema input = SchemaBuilder.record("two_columns").fields().requiredString("a").requiredString("b").endRecord();
    Select query = parse("SELECT ARRAY[d.second, 'x'] FROM " + "(SELECT *, 1 AS extra FROM t) d(first, second, third)");
    assertDoesNotThrow(() -> ArrayQueryValidation.validate(query, table -> input));
    Select column = parse("SELECT d.second FROM (SELECT *, 1 AS extra FROM t) d(first, second, third)");
    assertNull(ArrayQueryValidation.projectionSchema(column, table -> input));
  }

  /**
   * USING and NATURAL common columns use the same joined-schema binding as
   * execution, while unrelated duplicate names remain uncertain.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void commonJoinColumnsUseEngineMetadata() throws Exception {
    for (String join : List.of("JOIN rights USING (id)", "LEFT JOIN rights USING (id)", "FULL JOIN rights USING (id)",
        "NATURAL JOIN rights")) {
      Select query = parse("SELECT id FROM lefts " + join);
      assertEquals(Types.INTEGER, JdbcTypeMapper.mapSchemaToJdbcType(schema(query)), join);
      Select invalid = parse("SELECT ARRAY[id, 'x'] FROM lefts " + join);
      assertThrows(IllegalArgumentException.class,
          () -> ArrayQueryValidation.validate(invalid, table -> tables.get(table.getName())), join);
      Select qualified = parse("SELECT ARRAY[lefts.id, rights.id] FROM lefts " + join);
      assertEquals(Types.INTEGER, elementType(qualified), join);
    }
    Select ambiguous = parse("SELECT id FROM lefts CROSS JOIN rights");
    assertNull(schema(ambiguous));
  }

  /**
   * UNNEST output aliases, ordinality, and correlated argument declarations use
   * the existing UNNEST schema builder.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void unnestOutputNamesShadowOuterColumns() throws Exception {
    Select local = parse("SELECT (SELECT ARRAY[u.id, 'x'] " + "FROM UNNEST(ARRAY['inner']) u(id)) FROM s");
    assertEquals(Types.VARCHAR, elementType(local));
    Select correlated = parse("SELECT (SELECT ARRAY[u.id, 'x'] FROM UNNEST(s.items) u(id)) FROM s");
    assertEquals(Types.VARCHAR, elementType(correlated));
    Select ordinal = parse("SELECT position FROM UNNEST(ARRAY['inner']) WITH ORDINALITY u(value, position)");
    assertEquals(Types.INTEGER, JdbcTypeMapper.mapSchemaToJdbcType(schema(ordinal)));
    Select invalid = parse("SELECT ARRAY[u.id, 1] FROM UNNEST(ARRAY['inner']) u(id)");
    assertThrows(IllegalArgumentException.class,
        () -> ArrayQueryValidation.validate(invalid, table -> tables.get(table.getName())));
  }

  /**
   * Materialized SQL labels and internal names follow the engine's mapping rules.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void materializedNamesUseSharedEngineMappings() throws Exception {
    Schema input = SchemaBuilder.record("labeled").fields().requiredString("_c0").endRecord();
    Schema.Field field = input.getField("_c0");
    field.addProp(MaterializedNames.SQL_LABEL, "\"Display Name\"");
    field.addProp(MaterializedNames.LOOKUP_KEY, Identifier.lookupKey("\"Display Name\""));
    Schema projected = ArrayQueryValidation.projectionSchema(parse("SELECT t.\"Display Name\" FROM labeled t"),
        table -> input);
    assertEquals(Types.VARCHAR, JdbcTypeMapper.mapSchemaToJdbcType(projected));
    assertNull(ArrayQueryValidation.projectionSchema(parse("SELECT t._c0 FROM labeled t"), table -> input));
  }

  /**
   * Parses SQL without invoking the engine's derived-table materializer.
   *
   * @param sql
   *          query to inspect
   * @return parsed query
   * @throws Exception
   *           if parsing fails
   */
  private static Select parse(String sql) throws Exception {
    return (Select) CCJSqlParserUtil.parse(sql);
  }

  /**
   * Resolves the projected value using fixture input schemas.
   *
   * @param query
   *          scalar query
   * @return projected declaration, or null
   */
  private Schema schema(Select query) {
    return ArrayQueryValidation.projectionSchema(query, table -> tables.get(table.getName()));
  }

  /**
   * Reads the JDBC type of a projected ARRAY element declaration.
   *
   * @param query
   *          ARRAY projection
   * @return declared JDBC element type
   */
  private int elementType(Select query) {
    return JdbcTypeMapper.mapSchemaToJdbcType(schema(query).getElementType());
  }
}
