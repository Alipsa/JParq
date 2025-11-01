package se.alipsa.jparq.engine;

import java.io.Closeable;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;

/**
 * Minimal abstraction for sequential access to {@link GenericRecord} instances.
 */
public interface RecordReader extends Closeable {

  /**
   * Read the next available record.
   *
   * @return the next {@link GenericRecord}, or {@code null} when exhausted
   * @throws IOException
   *           if reading fails
   */
  GenericRecord read() throws IOException;

  @Override
  void close() throws IOException;
}
