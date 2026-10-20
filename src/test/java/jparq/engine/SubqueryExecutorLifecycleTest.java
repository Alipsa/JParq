package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqPreparedStatement;
import se.alipsa.jparq.engine.SubqueryExecutor;

/**
 * Counts nested preparations and verifies cached-statement resource ownership.
 */
class SubqueryExecutorLifecycleTest {
  /**
   * Reuses a safe prepared shape while consuming and closing fresh results.
   *
   * @throws SQLException
   *           if mocked JDBC setup fails
   */
  @Test
  void cachedPreparationDoesNotCacheCorrelatedResults() throws SQLException {
    var nested = mock(JParqPreparedStatement.class);
    when(nested.isReusableSubqueryShape()).thenReturn(true);
    var first = result(11);
    var second = result(22);
    when(nested.executeQuery()).thenReturn(first, second);
    var preparations = new AtomicInteger();
    var executor = new SubqueryExecutor(mock(JParqConnection.class), sql -> {
      preparations.incrementAndGet();
      return nested;
    });
    try (executor) {
      assertEquals(11, executor.executeRaw("SELECT x FROM small").rows().getFirst().getFirst());
      assertEquals(22, executor.executeRaw("SELECT x FROM small").rows().getFirst().getFirst());
      assertEquals(1, preparations.get());
      verify(first).close();
      verify(second).close();
      verify(nested, never()).close();
    }
    verify(nested).close();
  }

  /**
   * Closes every cached statement even if one close reports an error.
   *
   * @throws SQLException
   *           if mocked JDBC setup fails
   */
  @Test
  void failedIterationAndFailedCloseReleaseOwnedResources() throws SQLException {
    var first = mock(JParqPreparedStatement.class);
    var second = mock(JParqPreparedStatement.class);
    when(first.isReusableSubqueryShape()).thenReturn(true);
    when(second.isReusableSubqueryShape()).thenReturn(true);
    var result = result(11);
    when(result.next()).thenThrow(new SQLException("iteration failed"));
    when(first.executeQuery()).thenReturn(result);
    var secondResult = result(22);
    when(second.executeQuery()).thenReturn(secondResult);
    var executor = new SubqueryExecutor(mock(JParqConnection.class), sql -> "first".equals(sql) ? first : second);
    assertThrows(IllegalStateException.class, () -> executor.executeRaw("first"));
    verify(result).close();
    executor.executeRaw("second");
    doThrow(new SQLException("close failed")).when(first).close();
    assertThrows(SQLException.class, executor::close);
    verify(first).close();
    verify(second).close();
    executor.close();
  }

  /**
   * Reports the original query and fresh outer values without internal lookup
   * keys.
   *
   * @param qualifier
   *          quoted or unquoted outer qualifier
   * @throws Exception
   *           if query parsing or mocked JDBC setup fails
   */
  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {
      "o", "\"Mixed\""
  })
  void correlatedFailuresDescribeOuterBindings(String qualifier) throws Exception {
    var nested = mock(JParqPreparedStatement.class);
    when(nested.isReusableSubqueryShape()).thenReturn(true);
    var cause = new SQLException("nested failure", "42000");
    when(nested.executeQuery()).thenThrow(cause);
    var query = (net.sf.jsqlparser.statement.select.Select) net.sf.jsqlparser.parser.CCJSqlParserUtil
        .parse("SELECT 1 WHERE " + qualifier + ".id = 1");
    try (var executor = new SubqueryExecutor(mock(JParqConnection.class), sql -> nested)) {
      for (int value : java.util.List.of(7, 9)) {
        var failure = assertThrows(IllegalStateException.class, () -> executor.executeCorrelated(query,
            java.util.List.of(qualifier), java.util.Set.of("id"), java.util.Map.of(), (table, column) -> value));
        assertTrue(failure.getMessage().contains("SELECT 1 WHERE " + qualifier + ".id = 1"));
        assertTrue(
            failure.getMessage().contains("[outer bindings: " + qualifier.replace("\"", "") + ".id=" + value + "]"),
            failure.getMessage());
        assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"));
        assertSame(cause, failure.getCause());
        assertEquals("42000", ((SQLException) failure.getCause()).getSQLState());
      }
    }
  }

  /**
   * Creates a one-row result with deterministic metadata and value.
   *
   * @param value
   *          first-column value
   * @return mock result
   * @throws SQLException
   *           if mocked JDBC setup fails
   */
  private se.alipsa.jparq.JParqResultSet result(int value) throws SQLException {
    var result = mock(se.alipsa.jparq.JParqResultSet.class);
    var metadata = mock(ResultSetMetaData.class);
    when(metadata.getColumnCount()).thenReturn(1);
    when(metadata.getColumnLabel(1)).thenReturn("x");
    when(result.getMetaData()).thenReturn(metadata);
    when(result.next()).thenReturn(true, false);
    when(result.getObject(1)).thenReturn(value);
    return result;
  }
}
