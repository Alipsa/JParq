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
   *          the syntax or numeric SQLState
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "42000", "22003"
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
   *          the syntax or numeric SQLState
   * @throws Exception
   *           if fixture construction fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "42000", "22003"
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
  }
}
