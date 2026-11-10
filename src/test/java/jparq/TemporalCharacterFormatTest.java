package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.engine.SqlText;

/**
 * Verifies shared SQL character forms for scalar and ARRAY offset datetimes.
 */
class TemporalCharacterFormatTest {
  /**
   * Includes seconds and uses a space even when Java's default text omits them. A
   * zero offset prints as {@code +00:00}, the SQL time zone interval syntax.
   */
  @Test
  void formatsOffsetElementsWithScalarRules() {
    OffsetDateTime timestamp = OffsetDateTime.parse("2024-01-02T03:04+01:00");
    OffsetTime time = OffsetTime.parse("03:04+01:00");
    assertEquals("2024-01-02 03:04:00+01:00", SqlText.cast(timestamp));
    assertEquals("03:04:00+01:00", SqlText.cast(time));
    assertEquals("[[2024-01-02 03:04:00+01:00, null], [03:04:00+01:00]]", SqlText.cast(List.of(new Object[]{
        timestamp, null
    }, List.of(time))));
    assertEquals("2024-07-01 12:00:00+00:00", SqlText.cast(OffsetDateTime.parse("2024-07-01T12:00:00Z")));
    assertEquals("12:00:00+00:00", SqlText.cast(OffsetTime.parse("12:00:00Z")));
  }

  /**
   * Uses the same character form for CAST and JDBC getString at every nesting
   * depth.
   *
   * @param directory
   *          database directory
   * @throws Exception
   *           if querying fails
   */
  @Test
  void sharesCharacterFormsAcrossJdbcAndCasts(@TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String source : List.of("SELECT CURRENT_TIMESTAMP AS ts, CURRENT_TIME AS tm",
          "SELECT ts,tm FROM (SELECT CURRENT_TIMESTAMP AS ts,CURRENT_TIME AS tm) d",
          "WITH c AS (SELECT CURRENT_TIMESTAMP AS ts,CURRENT_TIME AS tm) SELECT ts,tm FROM c")) {
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT ts,CAST(ts AS VARCHAR(100)),ARRAY[ts],"
                + "CAST(ARRAY[ts] AS VARCHAR(200)),tm,CAST(tm AS VARCHAR(100)) FROM (" + source + ") s")) {
          assertTrue(result.next());
          OffsetDateTime timestamp = result.getObject(1, OffsetDateTime.class);
          String expectedTimestamp = sqlOffsetText(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(timestamp));
          assertEquals(expectedTimestamp, result.getString(1));
          assertEquals(expectedTimestamp, result.getString(2));
          assertEquals("[" + expectedTimestamp + "]", result.getString(3));
          assertEquals("[" + expectedTimestamp + "]", result.getString(4));
          OffsetTime time = result.getObject(5, OffsetTime.class);
          assertEquals(sqlOffsetText(DateTimeFormatter.ISO_OFFSET_TIME.format(time)), result.getString(5));
          assertEquals(result.getString(5), result.getString(6));
        }
      }
    }
  }

  /**
   * Prints a zero offset as {@code +00:00} in the SQL time zone interval syntax,
   * matching the scalar and ARRAY character CAST output.
   *
   * @param isoText
   *          ISO offset date-time or time text ending in {@code Z}
   * @return the SQL text form with an explicit zero offset
   */
  private static String sqlOffsetText(String isoText) {
    return isoText.endsWith("Z") ? isoText.substring(0, isoText.length() - 1) + "+00:00" : isoText.replace('T', ' ');
  }
}
