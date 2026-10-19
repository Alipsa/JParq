package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.QueryCancellation;
import se.alipsa.jparq.engine.RecordReader;

/** Checks registration races and ownership of canceled query inputs. */
class QueryCancellationTest {
  @Test
  void cancellationMarkersSurviveWrappersAndDoNotMisclassifyRealFailures() {
    SQLException original = QueryCancellation.canceledException();
    assertEquals("57014", original.getSQLState());
    assertSame(original,
        QueryCancellation
            .findCancellation(new IllegalStateException("Failed to execute sub query", new IOException(original)))
            .orElseThrow());
    assertTrue(QueryCancellation.findCancellation(new SQLException("Statement was canceled", "42000")).isEmpty());
    Exception first = new Exception();
    Exception second = new Exception(first);
    first.initCause(second);
    assertTrue(QueryCancellation.findCancellation(first).isEmpty());
  }

  @Test
  void lateRegistrationClosesTheInputAndFailsBeforeAnyRowsAreRead() throws Exception {
    var cancellation = new QueryCancellation();
    var input = mock(RecordReader.class);
    cancellation.cancel();
    assertEquals("Statement was canceled",
        assertThrows(SQLException.class, () -> cancellation.track(input)).getMessage());
    verify(input).close();
    verify(input, never()).read();
  }

  @Test
  void canceledInputsThrowInsteadOfReturningEofAndCloseOnlyOnce() throws Exception {
    var cancellation = new QueryCancellation();
    var input = mock(RecordReader.class);
    try (var tracked = cancellation.track(input)) {
      cancellation.cancel();
      cancellation.cancel();
      assertEquals("Statement was canceled", assertThrows(IOException.class, tracked::read).getMessage());
      verify(input).close();
      verify(input, never()).read();
    }
    verify(input).close();
  }

  @Test
  void closingOneInputDoesNotUnregisterOtherActiveInputs() throws Exception {
    var cancellation = new QueryCancellation();
    var first = mock(RecordReader.class);
    var second = mock(RecordReader.class);
    try (var trackedFirst = cancellation.track(first); var trackedSecond = cancellation.track(second)) {
      trackedFirst.close();
      cancellation.cancel();
      verify(first).close();
      verify(second).close();
      assertThrows(IOException.class, trackedSecond::read);
    }
  }
}
