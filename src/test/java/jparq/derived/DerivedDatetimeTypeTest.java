package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Verifies datetime values and metadata through materialized SQL inputs. */
class DerivedDatetimeTypeTest {

  /**
   * Checks exact values against the same statement's clock, including empty
   * results.
   *
   * @param keyword
   *          datetime value function
   * @param jdbcType
   *          expected JDBC type
   * @param className
   *          expected Java class
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "LOCALTIME,92,java.time.LocalTime", "LOCALTIMESTAMP,93,java.time.LocalDateTime",
      "CURRENT_TIME,2013,java.time.OffsetTime", "CURRENT_TIMESTAMP,2014,java.time.OffsetDateTime"
  })
  void retainsDatetimeValues(String keyword, int jdbcType, String className) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath();
    try (var connection = DriverManager.getConnection(url)) {
      for (String predicate : List.of("", " WHERE 1 = 0")) {
        String body = "SELECT " + keyword + " AS t FROM mtcars" + predicate;
        for (String sql : List.of("SELECT t, " + keyword + " AS expected FROM (" + body + ") s",
            "WITH c AS (" + body + ") SELECT t, " + keyword + " AS expected FROM c",
            "SELECT s.t, " + keyword + " AS expected FROM (" + body + ") s JOIN (SELECT 1 AS x) j ON 1 = 1",
            "SELECT t, " + keyword + " AS expected FROM (SELECT t FROM (" + body + ") s) n")) {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertEquals(jdbcType, result.getMetaData().getColumnType(1), sql);
            assertEquals(className, result.getMetaData().getColumnClassName(1), sql);
            int count = 0;
            while (result.next()) {
              assertEquals(className, result.getObject(1).getClass().getName(), sql);
              assertEquals(result.getObject(2), result.getObject(1), sql);
              count++;
            }
            assertEquals(predicate.isEmpty() ? 32 : 0, count, sql);
          }
        }
      }
    }
  }

  /**
   * Checks value-based inference and compatible local JDBC representations.
   *
   * @param body
   *          value table projection
   * @param className
   *          expected Java class name
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "SELECT LOCALTIME UNION ALL SELECT TIME '10:00:00.123' | java.time.LocalTime",
      "SELECT LOCALTIMESTAMP UNION ALL SELECT TIMESTAMP '2026-01-01 10:00:00.123456789' | java.time.LocalDateTime",
      "VALUES (LOCALTIME) | java.time.LocalTime", "VALUES (LOCALTIMESTAMP) | java.time.LocalDateTime",
      "VALUES (CURRENT_TIME) | java.time.OffsetTime", "VALUES (CURRENT_TIMESTAMP) | java.time.OffsetDateTime",
      "VALUES (LOCALTIME), (TIME '10:00:00.123') | java.time.LocalTime",
      "VALUES (LOCALTIMESTAMP), (TIMESTAMP '2026-01-01 10:00:00.123456789') | java.time.LocalDateTime"
  })
  void valueTablesRetainTemporalTypes(String body, String className) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT t FROM (" + body + ") s(t)")) {
      assertEquals(className, result.getMetaData().getColumnClassName(1), body);
      assertTrue(result.next());
      do {
        assertEquals(className, result.getObject(1).getClass().getName(), body);
      } while (result.next());
    }
  }

  /**
   * Checks that downstream ARRAY construction accepts materialized timestamps.
   *
   * @param keyword
   *          datetime value function
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "LOCALTIMESTAMP", "CURRENT_TIMESTAMP"
  })
  void timestampArraysRemainTemporal(String keyword) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ARRAY[t, TIMESTAMP '2026-01-01 10:00:00'] FROM (SELECT " + keyword
            + " AS t FROM mtcars LIMIT 1) s")) {
      assertEquals(Types.ARRAY, result.getMetaData().getColumnType(1));
      assertTrue(result.next());
      Object[] values = assertInstanceOf(Object[].class, result.getObject(1));
      assertEquals(2, values.length);
      assertTrue(values[0] instanceof java.sql.Timestamp || values[0] instanceof OffsetDateTime);
      assertFalse(result.next());
    }
  }
}
