package jparq.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.function.StatementTime;

/** Verifies datetime snapshots across SQL execution and lazy JDBC reads. */
class StatementDatetimeTest {
  private static final String URL = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath();

  /**
   * Repeated references, arrays, predicates, groups, and rows share one value.
   *
   * @param keyword
   *          datetime value function
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "CURRENT_TIMESTAMP", "CURRENT_TIME", "CURRENT_DATE", "LOCALTIMESTAMP", "LOCALTIME"
  })
  void sharesValueAcrossExpressionsAndRows(String keyword) throws Exception {
    for (boolean pushdown : List.of(true, false)) {
      try (
          var connection = DriverManager
              .getConnection(URL + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown);
          var statement = connection.createStatement()) {
        String sql = "SELECT " + keyword + ", " + keyword + ", ARRAY[" + keyword + ", " + keyword + "], CASE WHEN "
            + keyword + " = " + keyword + " THEN 'same' ELSE 'different' END" + " FROM mtcars WHERE " + keyword + " = "
            + keyword;
        try (var result = statement.executeQuery(sql)) {
          Object first = null;
          int count = 0;
          while (result.next()) {
            Object value = result.getObject(1);
            if (count++ == 0) {
              first = value;
            }
            assertEquals(first, value, sql);
            assertEquals(value, result.getObject(2), sql);
            assertEquals(value, result.getObject(1), sql);
            Object[] array = (Object[]) result.getObject(3);
            assertEquals(array[0], array[1], sql);
            assertEquals("same", result.getString(4), sql);
          }
          assertEquals(32, count, sql);
        }
        try (var result = statement
            .executeQuery("SELECT COUNT(*) FROM mtcars GROUP BY " + keyword + " HAVING " + keyword + " = " + keyword)) {
          assertTrue(result.next());
          assertEquals(32, result.getInt(1));
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Nested queries and prepared materializations retain the enclosing snapshot.
   *
   * @param sql
   *          query comparing nested and outer timestamps
   * @throws Exception
   *           if JDBC operations fail
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT CURRENT_TIMESTAMP = (SELECT CURRENT_TIMESTAMP)",
      "SELECT CURRENT_TIMESTAMP = (SELECT CURRENT_TIMESTAMP FROM mtcars i WHERE i.cyl = m.cyl LIMIT 1) "
          + "FROM mtcars m",
      "WITH t AS (SELECT LOCALTIMESTAMP AS ts) SELECT ts = LOCALTIMESTAMP FROM t",
      "SELECT ts = LOCALTIMESTAMP FROM (SELECT LOCALTIMESTAMP AS ts) t",
      "SELECT LOCALTIMESTAMP AS ts UNION SELECT LOCALTIMESTAMP AS ts",
      "SELECT l.ts = r.ts FROM (SELECT LOCALTIMESTAMP AS ts) l CROSS JOIN (SELECT LOCALTIMESTAMP AS ts) r"
  })
  void sharesValueAcrossNestedQueries(String sql) throws Exception {
    try (var connection = DriverManager.getConnection(URL); var statement = connection.createStatement()) {
      try (var result = statement.executeQuery(sql)) {
        assertNestedResult(result, sql);
      }
      try (var prepared = connection.prepareStatement(sql)) {
        for (int iteration = 0; iteration < 2; iteration++) {
          try (var result = prepared.executeQuery()) {
            assertNestedResult(result, sql);
          }
        }
      }
    }
  }

  /**
   * Checks nested comparison results or the single row produced by UNION.
   *
   * @param result
   *          query result
   * @param sql
   *          original query
   * @throws Exception
   *           if reading fails
   */
  private static void assertNestedResult(ResultSet result, String sql) throws Exception {
    assertTrue(result.next(), sql);
    if (sql.contains("UNION")) {
      assertFalse(result.next(), sql);
    } else {
      do {
        assertTrue(result.getBoolean(1), sql);
      } while (result.next());
    }
  }

  /**
   * All datetime functions derive their fields from the same session datetime.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void sharesFieldsAcrossDatetimeFunctions() throws Exception {
    try (var connection = DriverManager.getConnection(URL);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT CURRENT_TIMESTAMP, CURRENT_TIME, CURRENT_DATE, "
            + "CAST(LOCALTIMESTAMP AS VARCHAR(64)), CAST(LOCALTIME AS VARCHAR(64))")) {
      assertTrue(result.next());
      OffsetDateTime timestamp = (OffsetDateTime) result.getObject(1);
      assertEquals(timestamp.toOffsetTime(), result.getObject(2));
      assertEquals(timestamp.toLocalDate(), result.getDate(3).toLocalDate());
      assertEquals(timestamp.toLocalDateTime(), java.time.LocalDateTime.parse(result.getString(4).replace(' ', 'T')));
      assertEquals(timestamp.toLocalTime(), java.time.LocalTime.parse(result.getString(5)));
    }
  }

  /**
   * Interleaved results and reads on another thread use their own execution time.
   *
   * @throws Exception
   *           if execution or asynchronous reading fails
   */
  @Test
  void isolatesInterleavedResultsAndThreads() throws Exception {
    try (var connection = DriverManager.getConnection(URL);
        var firstStatement = connection.createStatement();
        var secondStatement = connection.createStatement();
        var first = firstStatement.executeQuery("SELECT CURRENT_TIMESTAMP FROM mtcars")) {
      assertTrue(first.next());
      Object firstTime = first.getObject(1);
      awaitClockAdvance();
      try (var second = secondStatement.executeQuery("SELECT CURRENT_TIMESTAMP FROM mtcars")) {
        assertTrue(second.next());
        Object secondTime = second.getObject(1);
        assertNotEquals(firstTime, secondTime);
        assertEquals(firstTime, first.getObject(1));
        try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
          List<Object> values = executor.submit(() -> {
            List<Object> rows = new ArrayList<>();
            while (first.next()) {
              rows.add(first.getObject(1));
            }
            return rows;
          }).get();
          assertEquals(31, values.size());
          values.forEach(value -> assertEquals(firstTime, value));
        }
        assertEquals(secondTime, second.getObject(1));
      }
    }
  }

  /**
   * Every execution, including a parameterized CTE, captures a fresh timestamp.
   *
   * @param sql
   *          reusable query
   * @throws Exception
   *           if JDBC operations fail
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT CURRENT_TIMESTAMP", "WITH t AS (SELECT LOCALTIMESTAMP AS ts) SELECT ts FROM t",
      "SELECT ts FROM (SELECT LOCALTIMESTAMP AS ts) t",
      "WITH t AS (SELECT LOCALTIMESTAMP AS ts) SELECT ts FROM t WHERE ? = 1"
  })
  void refreshesPreparedExecution(String sql) throws Exception {
    try (var connection = DriverManager.getConnection(URL); var prepared = connection.prepareStatement(sql)) {
      if (sql.contains("?")) {
        prepared.setInt(1, 1);
      }
      Object previous = null;
      for (int iteration = 0; iteration < 3; iteration++) {
        awaitClockAdvance();
        try (var result = prepared.executeQuery()) {
          assertTrue(result.next());
          Object current = result.getObject(1);
          assertNotEquals(previous, current);
          previous = current;
        }
      }
    }
  }
  /**
   * Waits for a clock tick before checking that separate executions differ.
   *
   * @throws InterruptedException
   *           if the wait is interrupted
   */
  private static void awaitClockAdvance() throws InterruptedException {
    Instant initial = Instant.now();
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
    while (!Instant.now().isAfter(initial)) {
      assertTrue(System.nanoTime() < deadline, "Clock did not advance within five seconds");
      Thread.sleep(1);
    }
  }

  /**
   * Time-independent prepared inputs remain cached across executions.
   *
   * @param sql
   *          query with an observable nondatetime materialization
   * @throws Exception
   *           if JDBC operations fail
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "WITH c AS (SELECT RAND() AS v FROM mtcars) SELECT v FROM c", "SELECT v FROM (SELECT RAND() AS v FROM mtcars) t",
      "SELECT v FROM (VALUES (RAND())) t(v)", "SELECT v FROM UNNEST(ARRAY[RAND()]) t(v)"
  })
  void retainsTimeIndependentMaterializations(String sql) throws Exception {
    try (var connection = DriverManager.getConnection(URL); var prepared = connection.prepareStatement(sql)) {
      List<Object> expected = null;
      for (int execution = 0; execution < 3; execution++) {
        List<Object> actual = new ArrayList<>();
        try (var result = prepared.executeQuery()) {
          while (result.next()) {
            actual.add(result.getObject(1));
          }
        }
        assertFalse(actual.isEmpty());
        if (expected == null) {
          expected = actual;
        } else {
          assertEquals(expected, actual, sql);
        }
      }
    }
  }

  /**
   * Datetime-dependent inputs are not evaluated or retained during preparation.
   *
   * @throws Exception
   *           if setup fails
   */
  @Test
  void defersTimeDependentMaterialization() throws Exception {
    try (var connection = DriverManager.getConnection(URL);
        var prepared = connection.prepareStatement(
            "WITH c AS (SELECT CURRENT_TIMESTAMP AS ts, " + "CAST('bad' AS INTEGER) AS v) SELECT v FROM c")) {
      assertThrows(SQLException.class, prepared::executeQuery);
    }
  }

  /**
   * Successful and failing lazy reads restore the enclosing thread context.
   *
   * @throws Exception
   *           if setup or successful reads fail
   */
  @Test
  void restoresContextAfterExecutionAndFailedNext() throws Exception {
    try (var connection = DriverManager.getConnection(URL); var statement = connection.createStatement()) {
      StatementTime enclosing = new StatementTime();
      var scope = enclosing.open();
      try (scope) {
        OffsetDateTime outer = StatementTime.current();
        try (var result = statement.executeQuery("SELECT CURRENT_TIMESTAMP FROM mtcars WHERE "
            + "CASE WHEN model = 'Mazda RX4' THEN 1 ELSE CAST('bad' AS INTEGER) END = 1")) {
          assertSame(outer, StatementTime.current());
          assertTrue(result.next());
          Object snapshot = result.getObject(1);
          assertNotSame(outer, snapshot);
          assertSame(outer, StatementTime.current());
          assertThrows(SQLException.class, result::next);
          assertSame(outer, StatementTime.current());
        }
      }
      try (var result = statement.executeQuery("SELECT CURRENT_TIMESTAMP")) {
        assertTrue(result.next());
        Object snapshot = result.getObject(1);
        assertNotSame(snapshot, StatementTime.current());
      }
    }
  }

  /**
   * Typed local-time conversion uses the snapshot's zone even after overrides.
   *
   * @throws Exception
   *           if JDBC operations fail
   */
  @Test
  void convertsOffsetTimeInsideSnapshotScope() throws Exception {
    TimeZone savedDefault = TimeZone.getDefault();
    String savedProperty = System.getProperty("user.timezone");
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
      System.setProperty("user.timezone", "Asia/Kolkata");
      try (var connection = DriverManager.getConnection(URL);
          var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT CURRENT_TIMESTAMP, TIME '12:34:56' AS t")) {
        assertTrue(result.next());
        OffsetDateTime timestamp = (OffsetDateTime) result.getObject(1);
        System.setProperty("user.timezone", "America/New_York");
        OffsetTime expected = java.time.LocalTime.of(12, 34, 56).atOffset(timestamp.getOffset())
            .withOffsetSameInstant(java.time.ZoneOffset.UTC);
        assertEquals(expected, result.getObject(2, OffsetTime.class));
        assertEquals(expected, result.getObject("t", OffsetTime.class));
      }
    } finally {
      TimeZone.setDefault(savedDefault);
      if (savedProperty == null) {
        System.clearProperty("user.timezone");
      } else {
        System.setProperty("user.timezone", savedProperty);
      }
    }
  }

}
