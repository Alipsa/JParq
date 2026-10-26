package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.engine.RecordReader;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.engine.SqlParser;

/**
 * Verifies SQLState classification without losing nested diagnostic context.
 */
class SqlExceptionsTest {
  @TempDir
  Path directory;

  /**
   * Redacts every binding inside nested SQL while retaining ordinary expression
   * text.
   *
   * @throws Exception
   *           if parsing fails
   */
  @Test
  void describesExpressionsWithoutPrivateBindings() throws Exception {
    String sql = "-(SELECT __JPARQ_CORRELATED_VALUE('private-key', 0) "
        + "+ __JPARQ_CORRELATED_VALUE('another-key', 12))";
    var expression = CCJSqlParserUtil.parseExpression(sql);
    assertEquals("-(SELECT <outer value> + <outer value>)", SqlExceptions.describe(expression));
    assertTrue(expression.toString().contains("private-key"));
    var ordinary = CCJSqlParserUtil.parseExpression("cyl & 2");
    assertEquals(ordinary.toString(), SqlExceptions.describe(ordinary));
  }

  /**
   * Uses one predicate diagnostic contract and redacts private operand bindings.
   *
   * @throws Exception
   *           if expression parsing fails
   */
  @Test
  void unsupportedPredicateFactoryClassifiesAndRedacts() throws Exception {
    var ordinary = SqlExceptions.unsupportedPredicate(CCJSqlParserUtil.parseExpression("cyl"));
    assertEquals("Unsupported predicate: cyl", ordinary.getMessage());
    assertEquals("42000", SqlExceptions.findSqlStateFailure(ordinary).getSQLState());
    var bound = SqlExceptions
        .unsupportedPredicate(CCJSqlParserUtil.parseExpression("__JPARQ_CORRELATED_VALUE('private-key', 0)"));
    assertEquals("Unsupported predicate: <outer value>", bound.getMessage());
    assertInstanceOf(SQLSyntaxErrorException.class, bound.getCause());
    assertEquals("42000", SqlExceptions.findSqlStateFailure(bound).getSQLState());
  }

  /**
   * Documents the JDBC syntax error contract, including preparation context.
   * 
   * @throws Exception
   *           if opening the connection fails
   */
  @Test
  void malformedSqlRetainsPreparationContext() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      var failure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement("SELEC 1"));
      assertEquals("42000", failure.getSQLState());
      assertTrue(failure.getMessage().contains("Failed to prepare query: SELEC 1"));
      assertTrue(failure.getMessage().contains("Failed to parse SQL: SELEC 1"));
    }
  }

  /**
   * Preserves every engine wrapper when a classified error crosses a JDBC
   * boundary.
   * 
   * @param state
   *          the syntax or data SQLState
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "42000", "22003", "22007", "22008", "22018"
  })
  void wrappingRetainsSubqueryContext(String state) {
    SQLException classified = new SQLException("Inner query failed", state, 7);
    var outer = new SQLException("Failed to execute subquery for join: SELECT broken", classified);
    var failure = SqlExceptions.wrap("Failed to prepare query: SELECT outer_query", outer);
    assertEquals(state, failure.getSQLState());
    assertEquals(7, failure.getErrorCode());
    assertTrue(failure.getMessage().contains("Failed to prepare query: SELECT outer_query"));
    assertTrue(failure.getMessage().contains("Failed to execute subquery for join: SELECT broken"));
    assertSame(outer, failure.getCause());
    assertSame(classified, failure.getCause().getCause());
    assertSame(failure, SqlExceptions.wrap(failure.getMessage(), failure));
    if ("42000".equals(state)) {
      assertInstanceOf(SQLSyntaxErrorException.class, failure);
    } else {
      assertInstanceOf(SQLDataException.class, failure);
    }
  }

  /**
   * Retains nested-subquery context when iteration fails after returning a row.
   * 
   * @param state
   *          the syntax or data SQLState
   * @throws Exception
   *           if fixture construction fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "42000", "22003", "22007", "22008", "22018"
  })
  void iterationRetainsSubqueryContext(String state) throws Exception {
    var schema = SchemaBuilder.record("Row").fields().requiredInt("id").endRecord();
    var row = new GenericData.Record(schema);
    row.put("id", 1);
    var classified = new SQLException("Nested expression failed", state);
    var readFailure = new IOException("Failed to execute subquery for join: SELECT broken", classified);
    var reader = mock(RecordReader.class);
    when(reader.read()).thenReturn(row).thenThrow(readFailure);
    var select = SqlParser.parseSelect("SELECT id FROM input");
    try (var result = new JParqResultSet(reader, select, "input", "PUBLIC", null, null, List.of("id"), List.of("id"),
        null)) {
      assertTrue(result.next());
      SQLException failure = assertThrows(SQLException.class, result::next);
      assertEquals(state, failure.getSQLState());
      assertTrue(failure.getMessage().contains("Failed to execute subquery for join: SELECT broken"));
      assertSame(readFailure, failure.getCause());
    }
  }

  /** Checks truncation boundaries without splitting supplementary characters. */
  @Test
  void diagnosticValuesPreserveSurrogatePairs() {
    assertEquals("x".repeat(64), SqlExceptions.diagnosticValue("x".repeat(64)));
    assertEquals("x".repeat(64) + "…", SqlExceptions.diagnosticValue("x".repeat(65)));
    assertEquals("x".repeat(63) + "…", SqlExceptions.diagnosticValue("x".repeat(63) + "😀tail"));
    assertEquals("x".repeat(62) + "😀…", SqlExceptions.diagnosticValue("x".repeat(62) + "😀tail"));
    assertEquals("😀".repeat(32), SqlExceptions.diagnosticValue("😀".repeat(32)));
  }

  /** Ensures unclassified wrappers do not hide a classified nested failure. */
  @Test
  void nullSqlStateDoesNotHideClassifiedCause() {
    var classified = new SQLException("Invalid temporal value", "22008");
    var wrapper = new SQLException("Unclassified wrapper", classified);
    assertSame(classified, SqlExceptions.findSqlStateFailure(wrapper));
    var unclassified = new SQLException("Unclassified failure");
    assertNull(SqlExceptions.findSqlStateFailure(unclassified));
    assertNull(SqlExceptions.wrap("Evaluation failed", unclassified).getSQLState());
  }

  /**
   * Preserves ordinary exceptions without claiming a syntax or numeric SQLState.
   */
  @Test
  void unrelatedErrorsKeepTheirOriginalClassification() {
    var original = new SQLException("Input unavailable", "08006");
    assertNull(SqlExceptions.findSqlStateFailure(original));
    var failure = SqlExceptions.wrap("Failed to prepare query", original);
    assertNull(failure.getSQLState());
    assertSame(original, failure.getCause());
    assertEquals("Failed to prepare query", failure.getMessage());
    var unexpected = new IllegalArgumentException("Unexpected engine failure");
    var wrapped = SqlExceptions.wrap("Evaluation failed", unexpected);
    assertNull(wrapped.getSQLState());
    assertSame(unexpected, wrapped.getCause());
  }
}
