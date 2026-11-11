package jparq.engine.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.TimeZone;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import se.alipsa.jparq.engine.function.ArrayFunctions;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/** Tests temporal ARRAY promotion while changing both JVM time-zone sources. */
@Isolated("Changes the JVM default time zone and user.timezone")
class TemporalArrayTimeZoneTest {

  /**
   * Resolves local timestamp gaps and seasonal displacements using their dates.
   * Date-free TIME values continue to use the execution displacement.
   *
   * @param zoneName
   *          zone containing the daylight-saving gap
   * @param gapText
   *          local timestamp in that gap
   */
  @ParameterizedTest
  @CsvSource({
      "America/New_York, 2026-03-08T02:30:00.123456789", "Europe/Stockholm, 2026-03-29T02:30:00.123456789"
  })
  void resolvesTimestampDatesAndRetainsTimeSessionOffset(String zoneName, String gapText) {
    String previousProperty = System.getProperty("user.timezone");
    TimeZone previousDefault = TimeZone.getDefault();
    try {
      System.setProperty("user.timezone", zoneName);
      TimeZone.setDefault(TimeZone.getTimeZone(zoneName));
      var session = ZoneId.of(zoneName).getRules().getOffset(Instant.now());
      assertEquals(session, DateTimeFunctions.sessionOffset());
      var gap = LocalDateTime.parse(gapText);
      var winter = LocalDateTime.parse("2026-01-01T12:00:00.123456789");
      var summer = LocalDateTime.parse("2026-07-01T12:00:00.987654321");
      var explicit = OffsetDateTime.parse("2026-01-01T12:00:00+03:00");
      var expected = List.of(gap.atZone(ZoneId.of(zoneName)).toOffsetDateTime(),
          winter.atZone(ZoneId.of(zoneName)).toOffsetDateTime(), summer.atZone(ZoneId.of(zoneName)).toOffsetDateTime(),
          explicit);
      assertEquals(expected,
          ArrayFunctions.homogenizeArrayValues(List.of(gap, java.sql.Timestamp.valueOf(winter), summer, explicit)));
      assertEquals(List.of(List.of(gap.atZone(ZoneId.of(zoneName)).toOffsetDateTime()), List.of(explicit)),
          ArrayFunctions.homogenizeArrayValues(List.of(List.of(gap), List.of(explicit))));
      var time = java.time.LocalTime.of(10, 0, 0, 123456789);
      var zonedTime = java.time.OffsetTime.parse("12:00:00+03:00");
      assertEquals(List.of(time.atOffset(session), zonedTime),
          ArrayFunctions.homogenizeArrayValues(List.of(time, zonedTime)));
    } finally {
      TimeZone.setDefault(previousDefault);
      if (previousProperty == null) {
        System.clearProperty("user.timezone");
      } else {
        System.setProperty("user.timezone", previousProperty);
      }
    }
  }
}
