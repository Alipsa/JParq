package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

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
        var result = statement.executeQuery("SELECT t, ARRAY[t, TIMESTAMP '2026-01-01 10:00:00'] FROM (SELECT "
            + keyword + " AS t FROM mtcars LIMIT 1) s")) {
      assertEquals(Types.ARRAY, result.getMetaData().getColumnType(2));
      assertTrue(result.next());
      Object[] values = assertInstanceOf(Object[].class, result.getObject(2));
      assertEquals(2, values.length);
      LocalDateTime literal = LocalDateTime.of(2026, 1, 1, 10, 0);
      if ("CURRENT_TIMESTAMP".equals(keyword)) {
        OffsetDateTime expected = result.getObject(1, OffsetDateTime.class);
        assertEquals(OffsetDateTime.class, values[0].getClass());
        assertEquals(expected, values[0]);
        assertEquals(DateTimeFunctions.offsetTimestamp(literal), values[1]);
      } else {
        assertEquals(java.sql.Timestamp.class, values[0].getClass());
        assertEquals(java.sql.Timestamp.valueOf(result.getObject(1, LocalDateTime.class)), values[0]);
        assertEquals(java.sql.Timestamp.valueOf(literal), values[1]);
      }
      assertFalse(result.next());
    }
  }

  /**
   * Checks temporal declarations when a set branch contains only typed NULLs.
   *
   * @param body
   *          set query
   * @param jdbcType
   *          expected JDBC type
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "SELECT TIMESTAMP '2020-01-01 00:00:00' UNION ALL SELECT CAST(NULL AS TIMESTAMP) | 93",
      "SELECT DATE '2020-01-01' UNION ALL SELECT CAST(NULL AS DATE) | 91",
      "SELECT TIME '10:00:00' UNION ALL SELECT CAST(NULL AS TIME) | 92",
      "SELECT LOCALTIMESTAMP UNION ALL SELECT CAST(NULL AS TIMESTAMP) | 93",
      "SELECT x FROM (SELECT LOCALTIMESTAMP AS x UNION ALL SELECT CAST(NULL AS TIMESTAMP)) s | 93",
      "SELECT CAST(NULL AS TIMESTAMP WITH TIME ZONE) UNION ALL SELECT CURRENT_TIMESTAMP | 2014",
      "SELECT CAST(NULL AS TIME WITH TIME ZONE) UNION ALL SELECT CURRENT_TIME | 2013",
      "SELECT CAST(NULL AS TIMESTAMP(3)) UNION ALL SELECT LOCALTIMESTAMP | 93",
      "SELECT CAST(NULL AS TIME(3)) UNION ALL SELECT LOCALTIME | 92",
      "SELECT CAST(NULL AS TIMESTAMP(3) WITH TIME ZONE) UNION ALL SELECT LOCALTIMESTAMP | 2014",
      "SELECT CAST(NULL AS TIME(3) WITH TIME ZONE) UNION ALL SELECT LOCALTIME | 2013",
      "SELECT CAST(NULL AS TIMESTAMP WITHOUT TIME ZONE) UNION ALL SELECT LOCALTIMESTAMP | 93",
      "SELECT CAST(NULL AS TIME WITHOUT TIME ZONE) UNION ALL SELECT LOCALTIME | 92",
      "SELECT CAST(NULL AS TIMESTAMP(3) WITHOUT TIME ZONE) UNION ALL SELECT LOCALTIMESTAMP | 93",
      "SELECT CAST(NULL AS TIME(3) WITHOUT TIME ZONE) UNION ALL SELECT LOCALTIME | 92",
      "SELECT CAST(NULL AS DATETIME) UNION ALL SELECT LOCALTIMESTAMP | 93"
  })
  void temporalNullSetBranches(String body, int jdbcType) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
        var statement = connection.createStatement();
        var result = statement.executeQuery(body)) {
      assertEquals(jdbcType, result.getMetaData().getColumnType(1));
      int nullCount = 0;
      int valueCount = 0;
      while (result.next()) {
        Object value = result.getObject(1);
        if (value == null) {
          nullCount++;
        } else {
          assertEquals(value.getClass().getName(), result.getMetaData().getColumnClassName(1));
          valueCount++;
        }
      }
      assertEquals(1, nullCount);
      assertEquals(1, valueCount);
    }
  }

  /**
   * Checks offset promotion for both operand orders and materialization paths.
   *
   * @param local
   *          local expression
   * @param offset
   *          offset expression
   * @param jdbcType
   *          expected JDBC type
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "LOCALTIMESTAMP | CURRENT_TIMESTAMP | 2014",
      "TIMESTAMP '2026-01-01 10:00:00.123456789' | CURRENT_TIMESTAMP | 2014", "LOCALTIME | CURRENT_TIME | 2013",
      "TIME '10:00:00.123' | CURRENT_TIME | 2013"
  })
  void promotesOffsetTypes(String local, String offset, int jdbcType) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
        var statement = connection.createStatement()) {
      for (String pair : List.of(local + "), (" + offset, offset + "), (" + local)) {
        String[] operands = pair.split("\\), \\(");
        for (String body : List.of("VALUES (" + pair + ")",
            "SELECT " + operands[0] + " UNION ALL SELECT " + operands[1])) {
          try (var result = statement.executeQuery(
              "SELECT t, " + offset + " AS expected, " + local + " AS local_expected FROM (" + body + ") s(t)")) {
            assertEquals(jdbcType, result.getMetaData().getColumnType(1), body);
            int count = 0;
            while (result.next()) {
              Object actual = result.getObject(1);
              assertEquals(actual.getClass().getName(), result.getMetaData().getColumnClassName(1), body);
              boolean localRow = (count == 0) == operands[0].equals(local);
              Object expected = result.getObject(localRow ? 3 : 2);
              if (localRow) {
                if (result.getObject(2) instanceof OffsetDateTime) {
                  LocalDateTime localValue = expected instanceof java.sql.Timestamp value
                      ? value.toLocalDateTime()
                      : (LocalDateTime) expected;
                  expected = DateTimeFunctions.offsetTimestamp(localValue);
                } else {
                  LocalTime localValue = expected instanceof java.sql.Time value
                      ? DateTimeFunctions.toLocalTime(value)
                      : (LocalTime) expected;
                  expected = localValue.atOffset(((OffsetTime) result.getObject(2)).getOffset());
                }
              }
              assertEquals(expected, actual, body);
              count++;
            }
            assertEquals(2, count, body);
          }
        }
      }
    }
  }

  /**
   * Checks incompatible datetime fields are rejected consistently.
   *
   * @param time
   *          time expression
   * @param timestamp
   *          timestamp expression
   * @throws Exception
   *           if connection setup fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "LOCALTIME | TIMESTAMP '2026-01-01 10:00:00'", "TIME '10:00:00' | LOCALTIMESTAMP", "LOCALTIME | LOCALTIMESTAMP",
      "TIME '10:00:00' | TIMESTAMP '2026-01-01 10:00:00'", "CURRENT_TIME | CURRENT_TIMESTAMP"
  })
  void rejectsIncompatibleDatetimeFields(String time, String timestamp) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
        var statement = connection.createStatement()) {
      for (String body : List.of("VALUES (" + time + "), (" + timestamp + ")",
          "VALUES (" + timestamp + "), (" + time + ")")) {
        assertThrows(SQLException.class, () -> statement.executeQuery("SELECT t FROM (" + body + ") s(t)"));
      }
    }
  }

  /**
   * Checks aggregates retain temporal schemas, including empty derived inputs.
   *
   * @param keyword
   *          datetime function
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "LOCALTIME", "LOCALTIMESTAMP", "CURRENT_TIME", "CURRENT_TIMESTAMP"
  })
  void extremaRetainTemporalMetadata(String keyword) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
        var statement = connection.createStatement()) {
      for (String aggregate : List.of("MIN", "MAX")) {
        for (String predicate : List.of("", " WHERE 1 = 0")) {
          try (var result = statement.executeQuery("SELECT " + aggregate + "(t), " + keyword
              + " AS expected FROM (SELECT " + keyword + " AS t FROM mtcars" + predicate + ") s")) {
            assertEquals(result.getMetaData().getColumnType(2), result.getMetaData().getColumnType(1));
            assertEquals(result.getMetaData().getColumnClassName(2), result.getMetaData().getColumnClassName(1));
            assertTrue(result.next());
            assertEquals(predicate.isEmpty() ? result.getObject(2) : null, result.getObject(1));
            assertFalse(result.next());
          }
        }
      }
    }
  }

  /**
   * Checks inferred datetime errors describe the operand types outside VALUES.
   *
   * @throws Exception
   *           if connection setup fails
   */
  @Test
  void inferredDatetimeErrorsDescribeTypes() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
        var statement = connection.createStatement()) {
      SQLException exception = assertThrows(SQLException.class,
          () -> statement.executeQuery("WITH c AS (SELECT CASE WHEN cyl = 4 THEN DATE '2020-01-01' "
              + "ELSE TIMESTAMP '2020-01-01 10:00:00' END AS x FROM mtcars) SELECT COUNT(*) FROM c"));
      assertEquals("42000", exception.getSQLState());
      assertTrue(exception.getMessage().contains("Incompatible datetime fields:"));
      assertTrue(exception.getMessage().contains("DATE"));
      assertTrue(exception.getMessage().contains("TIMESTAMP"));
      assertFalse(exception.getMessage().contains("VALUES"));
    }
  }

  /**
   * Reports incompatible datetime fields through a multi-column derived
   * projection.
   *
   * @throws Exception
   *           if connection setup fails
   */
  @Test
  void derivedArithmeticErrorsDescribeDatetimeFields() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
        var statement = connection.createStatement()) {
      SQLException exception = assertThrows(SQLException.class,
          () -> statement.executeQuery("SELECT cyl,x FROM (SELECT cyl,CASE WHEN cyl = 4 THEN DATE '2020-01-01' "
              + "ELSE TIMESTAMP '2020-01-01 10:00:00' END + INTERVAL '1' DAY AS x FROM mtcars) q"));
      assertInstanceOf(java.sql.SQLSyntaxErrorException.class, exception);
      assertEquals("42000", exception.getSQLState());
      assertTrue(exception.getMessage().contains("Incompatible datetime fields:"), exception.getMessage());
      assertTrue(exception.getMessage().contains("DATE"));
      assertTrue(exception.getMessage().contains("TIMESTAMP"));
      assertFalse(exception.getMessage().contains("ARRAY"));
    }
  }

  /**
   * Checks all-NULL sets retain declared datetime types and Java classes.
   *
   * @param type
   *          temporal CAST declaration
   * @param jdbcType
   *          expected JDBC type
   * @param className
   *          expected Java class name
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "TIMESTAMP(3) WITH TIME ZONE | 2014 | java.time.OffsetDateTime",
      "TIME(3) WITH TIME ZONE | 2013 | java.time.OffsetTime", "TIMESTAMP WITHOUT TIME ZONE | 93 | java.sql.Timestamp",
      "TIME WITHOUT TIME ZONE | 92 | java.sql.Time", "TIMESTAMP(3) WITHOUT TIME ZONE | 93 | java.sql.Timestamp",
      "TIME(3) WITHOUT TIME ZONE | 92 | java.sql.Time", "DATETIME | 93 | java.sql.Timestamp"
  })
  void nullOnlyTemporalSetsRetainMetadata(String type, int jdbcType, String className) throws Exception {
    String branch = "SELECT CAST(NULL AS " + type + ") AS t";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
        var statement = connection.createStatement()) {
      for (String sql : List.of(branch + " UNION ALL " + branch,
          "SELECT t FROM (" + branch + " UNION ALL " + branch + ") s")) {
        try (var result = statement.executeQuery(sql)) {
          assertEquals(jdbcType, result.getMetaData().getColumnType(1), sql);
          assertEquals(className, result.getMetaData().getColumnClassName(1), sql);
          for (int row = 0; row < 2; row++) {
            assertTrue(result.next());
            assertNull(result.getObject(1));
          }
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Checks compatibility errors use SQL names for local and offset values.
   *
   * @param left
   *          first datetime expression
   * @param right
   *          second datetime expression
   * @param expected
   *          expected diagnostic detail
   * @throws Exception
   *           if connection setup fails
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "LOCALTIME | TIMESTAMP '2020-01-01 10:00:00' | TIME and TIMESTAMP",
      "LOCALTIMESTAMP | TIME '10:00:00' | TIMESTAMP and TIME",
      "CURRENT_TIMESTAMP | DATE '2020-01-01' | TIMESTAMP WITH TIME ZONE and DATE",
      "CURRENT_TIME | DATE '2020-01-01' | TIME WITH TIME ZONE and DATE"
  })
  void datetimeErrorsUseSqlNames(String left, String right, String expected) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath());
        var statement = connection.createStatement()) {
      SQLException exception = assertThrows(SQLException.class,
          () -> statement.executeQuery("SELECT t FROM (VALUES (" + left + "), (" + right + ")) s(t)"));
      assertEquals("42000", exception.getSQLState());
      assertTrue(exception.getMessage().contains("Incompatible datetime fields: " + expected));
      assertFalse(exception.getMessage().contains("LOCAL_"));
      assertFalse(exception.getMessage().contains("OFFSET_"));
    }
  }

}
