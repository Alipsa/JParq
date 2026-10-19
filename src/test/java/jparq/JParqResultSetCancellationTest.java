package jparq;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.engine.QueryCancellation;
import se.alipsa.jparq.engine.RecordReader;
import se.alipsa.jparq.engine.SqlParser;

/**
 * Verifies JDBC error reporting when cancellation races with a reader failure.
 */
class JParqResultSetCancellationTest {
  /**
   * Reports racing read failures as cancellation while preserving ordinary
   * failures.
   *
   * @param cancel
   *          whether to cancel before releasing the failing read
   * @throws Exception
   *           if setup or synchronization fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void readFailuresRetainCancellationState(boolean cancel) throws Exception {
    var schema = SchemaBuilder.record("Row").fields().requiredInt("id").endRecord();
    var row = new GenericData.Record(schema);
    row.put("id", 0);
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var readFailure = new NullPointerException("Reader closed during iteration");
    var reader = mock(RecordReader.class);
    when(reader.read()).thenReturn(row).thenAnswer(invocation -> {
      started.countDown();
      assertTrue(release.await(10, TimeUnit.SECONDS));
      throw readFailure;
    });
    var select = SqlParser.parseSelect("SELECT id FROM input");
    try (var result = new JParqResultSet(reader, select, "input", "PUBLIC", null, null, List.of("id"), List.of("id"),
        null)) {
      assertTrue(result.next());
      var reading = new FutureTask<>(result::next);
      Thread.ofPlatform().start(reading);
      try {
        assertTrue(started.await(10, TimeUnit.SECONDS));
        if (cancel) {
          result.cancel();
        }
      } finally {
        release.countDown();
      }
      var failure = assertThrows(ExecutionException.class, () -> reading.get(10, TimeUnit.SECONDS));
      var sql = assertInstanceOf(SQLException.class, failure.getCause());
      if (cancel) {
        assertEquals(QueryCancellation.SQL_STATE, sql.getSQLState());
        assertEquals("Statement was canceled", sql.getMessage());
        assertArrayEquals(new Throwable[]{
            readFailure
        }, sql.getSuppressed());
        assertNoSuppressedBackReferences(sql);
        assertEquals(QueryCancellation.SQL_STATE, assertThrows(SQLException.class, result::next).getSQLState());
      } else {
        assertNull(sql.getSQLState());
        assertSame(readFailure, sql.getCause());
      }
      assertTrue(result.isClosed());
    }
  }
  /**
   * Preserves cleanup diagnostics across nested result boundaries without adding
   * wrappers that refer back to the cancellation marker.
   *
   * @param cleanupFailure
   *          whether the first read failure carries a cleanup failure
   * @throws Exception
   *           if result setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void wrappedCancellationRetainsCleanupWithoutCycles(boolean cleanupFailure) throws Exception {
    var schema = SchemaBuilder.record("Row").fields().requiredInt("id").endRecord();
    var row = new GenericData.Record(schema);
    row.put("id", 0);
    var cancellation = QueryCancellation.canceledException();
    var closeFailure = new SQLException("Close failed", new IOException("Reader close failed"));
    var select = SqlParser.parseSelect("SELECT id FROM input");
    for (int level = 0; level < 3; level++) {
      var readFailure = new IOException("Read failed", cancellation);
      if (cleanupFailure && level == 0) {
        readFailure.addSuppressed(closeFailure);
      }
      var reader = mock(RecordReader.class);
      when(reader.read()).thenReturn(row).thenThrow(readFailure);
      try (var result = new JParqResultSet(reader, select, "input", "PUBLIC", null, null, List.of("id"), List.of("id"),
          null)) {
        assertTrue(result.next());
        assertSame(cancellation, assertThrows(SQLException.class, result::next));
        assertEquals(QueryCancellation.SQL_STATE, cancellation.getSQLState());
        assertArrayEquals(cleanupFailure ? new Throwable[]{
            closeFailure
        } : new Throwable[0], cancellation.getSuppressed());
        assertNoSuppressedBackReferences(cancellation);
        assertTrue(result.isClosed());
      }
    }
  }

  /**
   * Checks all cause and suppressed links reachable from the suppressed list.
   *
   * @param failure
   *          exception that must not be reachable from its own suppressed entries
   */
  private static void assertNoSuppressedBackReferences(Throwable failure) {
    var pending = new ArrayDeque<Throwable>(List.of(failure.getSuppressed()));
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    while (!pending.isEmpty()) {
      Throwable current = pending.removeFirst();
      assertNotSame(failure, current, "Suppressed diagnostics must not refer back to the thrown exception");
      if (visited.add(current)) {
        if (current.getCause() != null) {
          pending.add(current.getCause());
        }
        pending.addAll(List.of(current.getSuppressed()));
      }
    }
  }
}
