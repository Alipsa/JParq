package se.alipsa.jparq.helper;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Period;
import java.util.List;
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
    return schema(IntervalQualifier.DAY_TO_SECOND, 0);
  }

  /**
   * Creates a column declaration with one field range and fractional precision.
   *
   * @param qualifier
   *          declared field range
   * @param precision
   *          declared fractional-second digits
   * @return interval column schema
   */
  public static Schema schema(IntervalQualifier qualifier, int precision) {
    Schema schema = Schema.create(Schema.Type.BYTES);
    schema.addProp(PROPERTY, "interval");
    schema.addProp("jparq.interval.fields", qualifier.name());
    schema.addProp("jparq.interval.precision", Integer.toString(precision));
    return schema;
  }

  /**
   * Combines interval column declarations before rows are formatted.
   *
   * @param left
   *          first declaration, possibly null
   * @param right
   *          second declaration, possibly null
   * @return common interval schema, or the known declaration for a NULL input
   */
  public static Schema common(Schema left, Schema right) {
    if (left == null) {
      return right;
    }
    if (right == null) {
      return left;
    }
    return schema(qualifier(left).common(qualifier(right)), Math.max(precision(left), precision(right)));
  }

  /**
   * Infers a common declaration for materialized interval inputs.
   *
   * @param values
   *          column values, including NULLs
   * @return common schema; wholly unknown inputs use DAY TO SECOND
   */
  public static Schema infer(List<?> values) {
    Schema declaration = null;
    for (Object value : values) {
      if (value instanceof TemporalInterval interval) {
        declaration = common(declaration, schema(interval.qualifier(), interval.fractionalPrecision()));
      }
    }
    return declaration == null ? schema() : declaration;
  }

  /**
   * Reads the column's common field range.
   *
   * @param schema
   *          interval schema, optionally nullable
   * @return declared field range
   */
  public static IntervalQualifier qualifier(Schema schema) {
    String fields = JdbcTypeMapper.nonNullSchema(schema).getProp("jparq.interval.fields");
    return fields == null ? IntervalQualifier.DAY_TO_SECOND : IntervalQualifier.valueOf(fields);
  }

  /**
   * Reads the column's common fractional-second precision.
   *
   * @param schema
   *          interval schema, optionally nullable
   * @return declared fractional digits
   */
  public static int precision(Schema schema) {
    String digits = JdbcTypeMapper.nonNullSchema(schema).getProp("jparq.interval.precision");
    return digits == null ? 0 : Integer.parseInt(digits);
  }

  /**
   * Applies a column declaration at an expression or JDBC output boundary.
   * Components are retained exactly; the declaration determines their text form.
   *
   * @param interval
   *          value to expose
   * @param schema
   *          common column declaration
   * @return interval with the output declaration, reusing an already bound value
   */
  public static TemporalInterval project(TemporalInterval interval, Schema schema) {
    if (interval.qualifier() == qualifier(schema) && interval.fractionalPrecision() == precision(schema)) {
      return interval;
    }
    return new TemporalInterval(interval.period(), interval.duration(), interval.isTemporalDifference(),
        qualifier(schema), precision(schema));
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
   * Encodes value components and provenance; column formatting stays in the
   * schema.
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
   * Decodes value components and applies their shared column declaration.
   *
   * @param value
   *          encoded components or an already decoded interval
   * @param schema
   *          column declaration controlling output formatting
   * @return interval bound to the common column type
   */
  public static TemporalInterval decode(Object value, Schema schema) {
    return project(decode(value), schema);
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
    boolean difference = bytes.get() != 0;
    return new TemporalInterval(period, duration, difference);
  }
}
