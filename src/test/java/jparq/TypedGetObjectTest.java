package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** JDBC typed getters convert values using numeric and java.time mappings. */
class TypedGetObjectTest {

  @TempDir
  Path directory;

  /**
   * Converts SQL temporal values without losing milliseconds or nanoseconds.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void temporalConversions() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT DATE '2024-01-02' AS d, TIME '12:34:56.123' AS t, "
            + "TIMESTAMP '2024-01-02 12:34:56.123456789' AS ts")) {
      assertTrue(result.next());
      assertEquals(LocalDate.of(2024, 1, 2), result.getObject(1, LocalDate.class));
      assertEquals(LocalDate.of(2024, 1, 2), result.getObject("d", LocalDate.class));
      assertEquals(LocalTime.of(12, 34, 56, 123000000), result.getObject("t", LocalTime.class));
      assertEquals(OffsetTime.of(12, 34, 56, 123000000, ZoneOffset.UTC), result.getObject("t", OffsetTime.class));
      assertEquals(LocalDateTime.of(2024, 1, 2, 12, 34, 56, 123456789), result.getObject("ts", LocalDateTime.class));
      OffsetDateTime expected = ((Timestamp) result.getObject("ts")).toInstant().atOffset(ZoneOffset.UTC);
      assertEquals(expected, result.getObject("ts", OffsetDateTime.class));
    }
  }

  /**
   * Converts numeric widths and decimals while preserving NULL and wasNull.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void numericConversionsAndNulls() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT CAST(7 AS INTEGER) AS i, CAST(1.25 AS REAL) AS r, "
            + "CAST(12.50 AS DECIMAL(10,2)) AS n, '42' AS s, NULL AS missing, CAST(1.23 AS REAL) AS r2")) {
      assertTrue(result.next());
      assertEquals(7L, result.getObject(1, Long.class));
      assertEquals(7L, result.getObject("i", Long.class));
      assertEquals(7.0, result.getObject("i", Double.class));
      assertEquals(1.25, result.getObject("r", Double.class));
      assertEquals(((Float) result.getObject("r2")).doubleValue(), result.getObject("r2", Double.class));
      assertEquals(new BigDecimal("7"), result.getObject("i", BigDecimal.class));
      assertEquals(new BigDecimal("12.50"), result.getObject("n", BigDecimal.class));
      assertEquals(42L, result.getObject("s", Long.class));
      assertNull(result.getObject("missing", LocalDate.class));
      assertTrue(result.wasNull());
      assertNull(result.getObject("missing", Long.class));
      assertTrue(result.wasNull());
    }
  }

  /**
   * Preserves classified conversion errors instead of leaking Java exceptions.
   *
   * @throws Exception
   *           if query execution fails
   */
  @Test
  void unsupportedAndOutOfRangeConversions() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT 'invalid' AS s, 1000000000000 AS n")) {
      assertTrue(result.next());
      assertEquals("22005",
          assertThrows(SQLDataException.class, () -> result.getObject("s", UUID.class)).getSQLState());
      assertEquals("22005",
          assertThrows(SQLDataException.class, () -> result.getObject(1, (Class<?>) null)).getSQLState());
      assertEquals("22018",
          assertThrows(SQLDataException.class, () -> result.getObject("s", Long.class)).getSQLState());
      assertEquals("22003",
          assertThrows(SQLDataException.class, () -> result.getObject("n", Integer.class)).getSQLState());
    }
  }
}
