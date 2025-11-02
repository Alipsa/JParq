package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;

/**
 * {@link RecordReader} implementation backed by an in-memory list of
 * {@link GenericRecord} instances. The reader iterates over the supplied rows
 * without performing any additional I/O which makes it suitable for temporary
 * data sets such as common table expression results.
 */
public final class InMemoryRecordReader implements RecordReader {

  private final List<GenericRecord> records;
  private int index;

  /**
   * Create a new reader that iterates over the provided records.
   *
   * @param records
   *          the records to expose through the reader
   */
  public InMemoryRecordReader(List<GenericRecord> records) {
    this.records = List.copyOf(Objects.requireNonNull(records, "records"));
    this.index = 0;
  }

  @Override
  public GenericRecord read() {
    if (index >= records.size()) {
      return null;
    }
    GenericRecord record = records.get(index);
    index++;
    return record;
  }

  @Override
  public void close() throws IOException {
    // No resources to release for in-memory data.
  }
}
