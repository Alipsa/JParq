package se.alipsa.jparq.helper;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Period;
import org.apache.avro.Schema;

/**
 * Preserves interval components in materialized Avro rows without text
 * coercion.
 */
public final class IntervalType {

  private static final String PROPERTY = "jparq.logicalType";
  private static final int BYTE_COUNT = 25;

  /** Prevents utility class instantiation. */
  private IntervalType() {
  }

  /**
   * Creates the internal interval schema; JDBC exposes it as OTHER.
   *
   * @return a bytes schema marked as an interval
   */
  public static Schema schema() {
    Schema schema = Schema.create(Schema.Type.BYTES);
    schema.addProp(PROPERTY, "interval");
    return schema;
  }

  /**
   * Identifies an interval schema, including nullable unions.
   *
   * @param schema
   *          schema to inspect, possibly null
   * @return whether the schema carries interval components
   */
  public static boolean matches(Schema schema) {
    Schema base = JdbcTypeMapper.nonNullSchema(schema);
    return base != null && "interval".equals(base.getProp(PROPERTY));
  }

  /**
   * Encodes calendar and duration components exactly, including provenance.
   *
   * @param interval
   *          interval to encode
   * @return a buffer positioned at the first encoded byte
   */
  public static ByteBuffer encode(TemporalInterval interval) {
    ByteBuffer bytes = ByteBuffer.allocate(BYTE_COUNT);
    bytes.putInt(interval.period().getYears()).putInt(interval.period().getMonths()).putInt(interval.period().getDays())
        .putLong(interval.duration().getSeconds()).putInt(interval.duration().getNano())
        .put((byte) (interval.isTemporalDifference() ? 1 : 0));
    return bytes.flip();
  }

  /**
   * Decodes materialized bytes or retains an already decoded UNNEST element.
   *
   * @param value
   *          interval or encoded buffer
   * @return the immutable interval
   * @throws IllegalArgumentException
   *           if the encoded representation is invalid
   */
  public static TemporalInterval decode(Object value) {
    if (value instanceof TemporalInterval interval) {
      return interval;
    }
    if (!(value instanceof ByteBuffer source) || source.remaining() != BYTE_COUNT) {
      throw new IllegalArgumentException("Invalid materialized interval value");
    }
    ByteBuffer bytes = source.duplicate();
    Period period = Period.of(bytes.getInt(), bytes.getInt(), bytes.getInt());
    Duration duration = Duration.ofSeconds(bytes.getLong(), bytes.getInt());
    return new TemporalInterval(period, duration, bytes.get() != 0);
  }
}
