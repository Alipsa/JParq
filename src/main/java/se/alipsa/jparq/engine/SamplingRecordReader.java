package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SplittableRandom;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.engine.SqlParser.TableSampleDefinition;
import se.alipsa.jparq.engine.SqlParser.TableSampleDefinition.SampleMethod;
import se.alipsa.jparq.engine.SqlParser.TableSampleDefinition.SampleValueType;

/**
 * {@link RecordReader} decorator that enforces {@code TABLESAMPLE} semantics for
 * both percentage and row-count sampling strategies.
 */
public final class SamplingRecordReader implements RecordReader {

  private RecordReader delegate;
  private final TableSampleDefinition definition;
  private final SplittableRandom random;
  private final double bernoulliProbability;
  private final long systemInterval;
  private final long systemOffset;
  private long rowIndex;
  private final List<GenericRecord> bufferedRows;
  private int bufferedIndex;

  private SamplingRecordReader(RecordReader delegate, TableSampleDefinition definition, SplittableRandom random,
      double bernoulliProbability, long systemInterval, long systemOffset, List<GenericRecord> bufferedRows)
      throws IOException {
    this.definition = Objects.requireNonNull(definition, "definition");
    this.random = Objects.requireNonNull(random, "random");
    this.bernoulliProbability = bernoulliProbability;
    this.systemInterval = systemInterval;
    this.systemOffset = systemOffset;
    this.bufferedRows = bufferedRows;
    this.delegate = bufferedRows == null ? Objects.requireNonNull(delegate, "delegate") : null;
  }

  /**
   * Create a sampling reader that wraps the supplied {@link RecordReader}.
   *
   * @param reader
   *          source reader that should be sampled
   * @param definition
   *          sampling definition parsed from the SQL statement
   * @return a reader that emits rows according to {@code definition}
   * @throws IOException
   *           if reading or sampling fails
   */
  public static RecordReader wrap(RecordReader reader, TableSampleDefinition definition) throws IOException {
    if (reader == null || definition == null) {
      return reader;
    }
    return build(reader, definition);
  }

  /**
   * Apply the supplied sampling definition to an in-memory list of rows.
   *
   * @param rows
   *          rows that should be sampled
   * @param definition
   *          sampling definition parsed from the SQL statement
   * @return sampled rows or the original list when no sampling is required
   * @throws IOException
   *           if sampling fails
   */
  public static List<GenericRecord> sampleRows(List<GenericRecord> rows, TableSampleDefinition definition)
      throws IOException {
    if (definition == null || rows == null || rows.isEmpty()) {
      return rows;
    }
    try (SamplingRecordReader sampler = build(new InMemoryRecordReader(rows), definition)) {
      List<GenericRecord> sampled = new ArrayList<>();
      GenericRecord record = sampler.read();
      while (record != null) {
        sampled.add(record);
        record = sampler.read();
      }
      return List.copyOf(sampled);
    }
  }

  private static SamplingRecordReader build(RecordReader reader, TableSampleDefinition definition) throws IOException {
    SplittableRandom random = new SplittableRandom(seed(definition));
    if (definition.valueType() == SampleValueType.ROWS) {
      List<GenericRecord> buffered = sampleFixedRowCount(reader, definition, random);
      return new SamplingRecordReader(null, definition, random, 0D, 0L, 0L, buffered);
    }
    double probability = computeProbability(definition.value());
    boolean bernoulli = definition.method() == SampleMethod.BERNOULLI;
    long interval = bernoulli ? 0L : computeSystemInterval(definition.value());
    long offset = (!bernoulli && interval > 1) ? random.nextLong(interval) : 0L;
    return new SamplingRecordReader(reader, definition, random, probability, interval, offset, null);
  }

  @Override
  public GenericRecord read() throws IOException {
    if (bufferedRows != null) {
      if (bufferedIndex >= bufferedRows.size()) {
        return null;
      }
      GenericRecord record = bufferedRows.get(bufferedIndex);
      bufferedIndex++;
      return record;
    }
    if (delegate == null) {
      return null;
    }
    GenericRecord record = delegate.read();
    while (record != null) {
      long currentIndex = rowIndex;
      rowIndex++;
      if (shouldInclude(currentIndex)) {
        return record;
      }
      record = delegate.read();
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    if (delegate != null) {
      delegate.close();
      delegate = null;
    }
  }

  private boolean shouldInclude(long index) {
    if (definition.valueType() == SampleValueType.ROWS) {
      return true;
    }
    if (definition.method() == SampleMethod.BERNOULLI) {
      if (bernoulliProbability <= 0D) {
        return false;
      }
      if (bernoulliProbability >= 1D) {
        return true;
      }
      return random.nextDouble() < bernoulliProbability;
    }
    return includeSystem(index);
  }

  private boolean includeSystem(long index) {
    if (definition.value() <= 0D) {
      return false;
    }
    if (systemInterval <= 1) {
      return true;
    }
    if (index < systemOffset) {
      return false;
    }
    long relative = index - systemOffset;
    return relative % systemInterval == 0;
  }

  private static double computeProbability(double percentage) {
    if (percentage <= 0D) {
      return 0D;
    }
    if (percentage >= 100D) {
      return 1D;
    }
    return percentage / 100D;
  }

  private static long computeSystemInterval(double percentage) {
    if (percentage <= 0D) {
      return Long.MAX_VALUE;
    }
    if (percentage >= 100D) {
      return 1L;
    }
    double probability = percentage / 100D;
    long interval = Math.round(1D / probability);
    return Math.max(1L, interval);
  }

  private static long seed(TableSampleDefinition definition) {
    if (definition.repeatableSeed() != null) {
      return definition.repeatableSeed();
    }
    return ThreadLocalRandom.current().nextLong();
  }

  private static List<GenericRecord> sampleFixedRowCount(RecordReader source, TableSampleDefinition definition,
      SplittableRandom random) throws IOException {
    int target = toTargetCount(definition.value());
    List<GenericRecord> reservoir = new ArrayList<>(target);
    long seen = 0;
    try (RecordReader reader = source) {
      GenericRecord record = reader.read();
      while (record != null) {
        if (target > 0) {
          if (seen < target) {
            reservoir.add(record);
          } else {
            long replacement = random.nextLong(seen + 1);
            if (replacement < target) {
              reservoir.set((int) replacement, record);
            }
          }
        }
        seen++;
        record = reader.read();
      }
    }
    if (reservoir.isEmpty()) {
      return List.of();
    }
    return List.copyOf(reservoir);
  }

  private static int toTargetCount(double value) {
    if (value < 0D) {
      return 0;
    }
    double rounded = Math.rint(value);
    if (rounded > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("TABLESAMPLE ROWS exceeds supported range");
    }
    return (int) Math.round(rounded);
  }
}
