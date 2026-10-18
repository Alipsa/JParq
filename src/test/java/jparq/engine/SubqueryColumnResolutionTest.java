package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.Select;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.engine.SubqueryExecutor;

/**
 * Verifies virtual schemas and conservative binding when input names are
 * unknown.
 */
class SubqueryColumnResolutionTest {
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT 1 WHERE id = 7", "SELECT 1 FROM (SELECT 1) d WHERE id = 7"
  })
  void knownInputsWithoutTheNameStillAllowOuterFallback(String sql) throws Exception {
    var executor = new SubqueryExecutor(mock(JParqConnection.class));
    var select = (Select) CCJSqlParserUtil.parse(sql);
    var result = executor.rewrite(select, List.of("b"), Set.of("id"), Map.of(), (qualifier, column) -> 7);
    assertEquals(Set.of("id"), result.correlatedColumns());
    assertTrue(result.sql().contains("7 = 7"), result.sql());
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "tables", "columns"
  })
  void virtualColumnResolutionDoesNotScanTheCatalog(String view) throws Exception {
    var connection = mock(JParqConnection.class);
    var executor = new SubqueryExecutor(connection);
    var select = (Select) CCJSqlParserUtil
        .parse("SELECT 1 FROM information_schema." + view + " WHERE table_name = 'small' AND x = 7");
    var result = executor.rewrite(select, List.of("t"), Set.of("table_name", "x"), Map.of(), (qualifier, column) -> {
      assertEquals("x", column);
      return 7;
    });
    assertTrue(result.correlated());
    assertEquals(Set.of("x"), result.correlatedColumns());
    assertTrue(result.sql().contains("table_name = 'small'"));
    verifyNoInteractions(connection);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "missing", "known k JOIN missing u ON k.id = u.id", "missing u JOIN known k ON k.id = u.id",
      "(known k JOIN missing u ON k.id = u.id)", "(SELECT * FROM missing) d", "(VALUES (1)) v", "UNNEST(ARRAY[1,2]) u"
  })
  void unknownInputsKeepUnqualifiedNamesInside(String from) throws Exception {
    var executor = new SubqueryExecutor(mock(JParqConnection.class), null,
        table -> table.getName().equals("known") ? Set.of("other") : null);
    var select = (Select) CCJSqlParserUtil.parse("SELECT 1 FROM " + from + " WHERE id = b.x");
    var result = executor.rewrite(select, List.of("b"), Set.of("id", "x"), Map.of(), (qualifier, column) -> {
      assertEquals("x", column, "Unknown inner names must not be replaced");
      return 7;
    });
    assertEquals(Set.of("x"), result.correlatedColumns());
    assertTrue(result.sql().contains("id = 7"), result.sql());
  }

  @Test
  void unresolvedPhysicalColumnsRemainInnerAndTheFailureIsCached() throws Exception {
    var connection = mock(JParqConnection.class);
    when(connection.resolveTable(null, "missing")).thenThrow(new SQLException("Table not found"));
    var executor = new SubqueryExecutor(connection);
    var select = (Select) CCJSqlParserUtil.parse("SELECT 1 FROM missing WHERE id = 1");
    for (int index = 0; index < 10; index++) {
      assertFalse(executor.rewrite(select, List.of("b"), Set.of("id"), Map.of(),
          (qualifier, column) -> fail("Unresolved input must keep its names")).correlated());
    }
    verify(connection).resolveTable(null, "missing");
  }

  @Test
  void unknownCallbackResultsAreCached() throws Exception {
    var lookups = new AtomicInteger();
    var executor = new SubqueryExecutor(mock(JParqConnection.class), null, table -> {
      lookups.incrementAndGet();
      return null;
    });
    var select = (Select) CCJSqlParserUtil.parse("SELECT 1 FROM missing WHERE id = 1");
    for (int index = 0; index < 10; index++) {
      assertFalse(executor.rewrite(select, List.of("b"), Set.of("id"), Map.of(),
          (qualifier, column) -> fail("Unknown input must keep its names")).correlated());
    }
    assertEquals(1, lookups.get());
  }
}
