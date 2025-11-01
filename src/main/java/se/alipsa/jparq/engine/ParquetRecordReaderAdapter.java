package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;

/**
 * Adapts a {@link ParquetReader} to the {@link RecordReader} abstraction.
 */
public final class ParquetRecordReaderAdapter implements RecordReader {

  private final ParquetReader<GenericRecord> delegate;

  /**
   * Create a new adapter for the supplied {@link ParquetReader}.
   *
   * @param delegate
   *          the reader to adapt
   */
  public ParquetRecordReaderAdapter(ParquetReader<GenericRecord> delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public GenericRecord read() throws IOException {
    return delegate.read();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
