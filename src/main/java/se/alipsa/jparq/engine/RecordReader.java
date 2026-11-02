package se.alipsa.jparq.engine;

import java.io.Closeable;
import java.io.IOException;
import org.apache.avro.Schema;
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

  /**
   * Returns the schema of the records produced by this reader.
   *
   * @return the record schema, or {@code null} when the schema is not known
   *         before reading
   */
  default Schema schema() {
    return null;
  }

  @Override
  void close() throws IOException;
}
