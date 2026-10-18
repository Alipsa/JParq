package se.alipsa.jparq.engine;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.generic.GenericRecord;

/**
 * Shares cancellation across planning, execution, nested queries, and returned
 * results.
 */
public final class QueryCancellation {
  private static final String CANCELED_MESSAGE = "Statement was canceled";
  private final Set<RecordReader> readers = new HashSet<>();
  private volatile boolean canceled;

  /**
   * Rejects continued execution after a cancellation request.
   *
   * @throws SQLException
   *           if this execution was canceled
   */
  public void checkCanceled() throws SQLException {
    if (canceled) {
      throw new SQLException(CANCELED_MESSAGE);
    }
  }

  /**
   * Registers an input and checks cancellation before and after every read.
   *
   * @param reader
   *          input whose ownership is transferred to the returned wrapper
   * @return cancelable input retaining any join column mappings
   * @throws SQLException
   *           if cancellation already occurred
   */
  public RecordReader track(RecordReader reader) throws SQLException {
    var tracked = new Input(reader);
    synchronized (this) {
      if (!canceled) {
        readers.add(tracked);
        return tracked;
      }
    }
    try {
      tracked.close();
    } catch (IOException exception) {
      throw new SQLException(CANCELED_MESSAGE, exception);
    }
    throw new SQLException(CANCELED_MESSAGE);
  }

  /**
   * Publishes cancellation and closes registered inputs outside the state lock.
   *
   * @throws SQLException
   *           if an input cannot be closed
   */
  public void cancel() throws SQLException {
    List<RecordReader> inputs;
    synchronized (this) {
      canceled = true;
      inputs = List.copyOf(readers);
    }
    SQLException failure = null;
    for (RecordReader input : inputs) {
      try {
        input.close();
      } catch (IOException exception) {
        if (failure == null) {
          failure = new SQLException("Failed to close canceled input", exception);
        } else {
          failure.addSuppressed(exception);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  /** Checks cancellation at physical and materialized reader boundaries. */
  private final class Input implements RecordReader, ColumnMappingProvider {
    private final RecordReader delegate;
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Wraps one owned input.
     *
     * @param delegate
     *          input reader
     */
    private Input(RecordReader delegate) {
      this.delegate = delegate;
    }

    @Override
    public GenericRecord read() throws IOException {
      checkReadCanceled();
      GenericRecord row = delegate.read();
      checkReadCanceled();
      return row;
    }

    /**
     * Reports cancellation through the reader interface instead of returning EOF.
     *
     * @throws IOException
     *           if the execution was canceled
     */
    private void checkReadCanceled() throws IOException {
      if (canceled) {
        throw new IOException(CANCELED_MESSAGE);
      }
    }

    @Override
    public void close() throws IOException {
      if (closed.compareAndSet(false, true)) {
        synchronized (QueryCancellation.this) {
          readers.remove(this);
        }
        delegate.close();
      }
    }

    @Override
    public Map<String, Map<String, String>> qualifierColumnMapping() {
      return delegate instanceof ColumnMappingProvider provider ? provider.qualifierColumnMapping() : Map.of();
    }

    @Override
    public Map<String, String> unqualifiedColumnMapping() {
      return delegate instanceof ColumnMappingProvider provider ? provider.unqualifiedColumnMapping() : Map.of();
    }
  }
}
