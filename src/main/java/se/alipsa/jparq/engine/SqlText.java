package se.alipsa.jparq.engine;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;
import se.alipsa.jparq.helper.TemporalInterval;

/**
 * Separates explicit SQL text conversion from implicit string operand coercion.
 */
public final class SqlText {

  /**
   * SQL timestamp character form, including seconds and significant fractions.
   * The offset follows the SQL time zone interval syntax, printing {@code +00:00}
   * for a zero offset.
   */
  private static final DateTimeFormatter OFFSET_TIMESTAMP = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral(' ').appendPattern("HH:mm:ss")
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).appendOffset("+HH:MM", "+00:00").toFormatter(Locale.ROOT);

  /**
   * SQL time character form with an explicit offset, printing {@code +00:00} for
   * a zero offset.
   */
  private static final DateTimeFormatter OFFSET_TIME = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).appendOffset("+HH:MM", "+00:00").toFormatter(Locale.ROOT);

  /** Prevents utility class instantiation. */
  private SqlText() {
  }

  /**
   * Converts a value to text while preserving SQL NULL.
   *
   * @param value
   *          the SQL value
   * @return its text, or null for SQL NULL
   * @throws IllegalArgumentException
   *           if binary data or an interval is used implicitly as text, carrying
   *           SQLState 42000
   */
  public static String valueOf(Object value) {
    validateStringOperand(value);
    return value == null ? null : value.toString();
  }

  /**
   * Converts a value for an explicit character CAST, preserving SQL NULL. Binary
   * inputs are decoded as UTF-8; reference arrays retain their element text
   * across materialization.
   *
   * @param value
   *          SQL value to convert
   *
   * @return UTF-8 binary text, SQL interval text, or the normal character
   *         representation
   * @throws IllegalArgumentException
   *           if binary data is not valid UTF-8, carrying SQLState 22021
   */
  public static String cast(Object value) {
    if (value instanceof byte[] bytes) {
      return decodeBinary(ByteBuffer.wrap(bytes));
    }
    if (value instanceof ByteBuffer buffer) {
      return decodeBinary(buffer.duplicate());
    }
    if (value instanceof Object[] array) {
      return arrayText(Arrays.asList(array));
    }
    if (value instanceof List<?> list) {
      return arrayText(list);
    }
    if (value instanceof OffsetDateTime timestamp) {
      return OFFSET_TIMESTAMP.format(timestamp);
    }
    if (value instanceof OffsetTime time) {
      return OFFSET_TIME.format(time);
    }
    return value == null ? null : value.toString();
  }

  /**
   * Renders ARRAY elements using the same explicit conversion as scalar values.
   *
   * @param values
   *          array elements
   * @return bracketed element text, including SQL NULL elements
   */
  private static String arrayText(List<?> values) {
    StringJoiner text = new StringJoiner(", ", "[", "]");
    for (Object value : values) {
      text.add(value == null ? "null" : cast(value));
    }
    return text.toString();
  }

  /**
   * Decodes UTF-8 bytes without replacing malformed or unmappable sequences.
   *
   * @param bytes
   *          binary input buffer
   * @return decoded character data
   * @throws IllegalArgumentException
   *           if bytes are not valid UTF-8, carrying SQLState 22021
   */
  private static String decodeBinary(ByteBuffer bytes) {
    try {
      return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT).decode(bytes).toString();
    } catch (CharacterCodingException failure) {
      throw new IllegalArgumentException("Binary data is not valid UTF-8",
          new SQLDataException("Character not in repertoire: invalid UTF-8", "22021", failure));
    }
  }

  /**
   * Rejects binary and interval operands in operations that require character
   * values.
   *
   * @param value
   *          the value, including nested SQL arrays
   * @throws IllegalArgumentException
   *           if binary data or an interval is used implicitly as text, carrying
   *           SQLState 42000
   */
  private static void validateStringOperand(Object value) {
    if (value instanceof byte[] || value instanceof ByteBuffer) {
      throw SqlExceptions.syntaxFailure("Binary data requires an explicit character cast for text operations");
    }
    if (value instanceof TemporalInterval) {
      throw SqlExceptions.syntaxFailure("INTERVAL requires an explicit character cast for text operations");
    }
    if (value instanceof Object[] array) {
      for (Object element : array) {
        validateStringOperand(element);
      }
    } else if (value instanceof Iterable<?> elements) {
      for (Object element : elements) {
        validateStringOperand(element);
      }
    }
  }
}
