package jparq.window.navigation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.helper.TemporalInterval;

/**
 * Verifies navigation defaults whose SQL type has multiple engine
 * representations.
 */
class NavigationDefaultTypeTest {
  /**
   * Early interval and schema-bound default errors share preparation diagnostics.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void incompatibleDefaultsSharePreparationContext() throws Exception {
    Path fixtures = Path.of("src/test/resources").toAbsolutePath();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + fixtures)) {
      for (String function : List.of("LAG", "LEAD")) {
        for (String arguments : List.of("INTERVAL '1' DAY,1,INTERVAL '1' YEAR", "gear,1,INTERVAL '1' DAY")) {
          String query = "SELECT " + function + "(" + arguments + ") OVER (ORDER BY gear) FROM mtcars";
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(query));
          assertEquals("42000", failure.getSQLState());
          assertTrue(failure.getMessage().startsWith("Failed to prepare query: "), failure.getMessage());
          assertTrue(failure.getMessage().contains(function + " default type INTERVAL"), failure.getMessage());
          assertFalse(failure.getMessage().contains("Failed to parse SQL"), failure.getMessage());
        }
      }
    }
  }

  /**
   * Reports nested ARRAY element types instead of only repeating ARRAY.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void describesArrayDefaultElementTypes() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:.")) {
      for (String function : List.of("LAG", "LEAD")) {
        for (boolean nested : List.of(false, true)) {
          String value = "ARRAY[TIMESTAMP '2024-07-01 10:00:00']";
          String fallback = "ARRAY[CAST(1 AS INTEGER)]";
          String sourceType = "ARRAY(INTEGER)";
          String targetType = "ARRAY(TIMESTAMP)";
          if (nested) {
            value = "ARRAY[" + value + "]";
            fallback = "ARRAY[" + fallback + "]";
            sourceType = "ARRAY(" + sourceType + ")";
            targetType = "ARRAY(" + targetType + ")";
          }
          String query = "SELECT " + function + "(" + value + ",1," + fallback
              + ") OVER (ORDER BY id) FROM (VALUES (1)) t(id)";
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(query));
          assertEquals("42000", failure.getSQLState());
          assertTrue(failure.getMessage().contains(sourceType + " is not assignable to " + targetType),
              failure.getMessage());
        }
      }
    }
  }

  /**
   * Rejects cross-kind INTERVAL defaults while accepting conversions within a
   * kind.
   *
   * @throws Exception
   *           if JDBC evaluation fails
   */
  @Test
  void validatesIntervalDefaultKinds() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:.")) {
      for (String function : List.of("LAG", "LEAD")) {
        for (String suffix : List.of("", " WHERE id > 5")) {
          for (String arguments : List.of("INTERVAL '1' DAY,1,INTERVAL '1' YEAR",
              "INTERVAL '1' YEAR,1,INTERVAL '1' DAY")) {
            String query = "SELECT " + function + "(" + arguments + ") OVER (ORDER BY id) FROM (VALUES (1)) t(id)"
                + suffix;
            var failure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(query));
            assertEquals("42000", failure.getSQLState());
            assertTrue(failure.getMessage().contains("INTERVAL DAY"), failure.getMessage());
            assertTrue(failure.getMessage().contains("INTERVAL YEAR"), failure.getMessage());
          }
        }
        String query = "SELECT " + function + "(INTERVAL '1' MONTH,5,INTERVAL '1' YEAR) OVER (ORDER BY id)," + function
            + "(INTERVAL '1' HOUR,5,INTERVAL '1' DAY) OVER (ORDER BY id) FROM (VALUES (1)) t(id)";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(query)) {
          assertTrue(result.next());
          assertEquals(12, ((TemporalInterval) result.getObject(1)).period().toTotalMonths());
          assertEquals(1, ((TemporalInterval) result.getObject(2)).period().getDays());
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Rejects incompatible defaults during preparation even if the query has no
   * rows.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void rejectsIncompatibleDefaultsBeforeExecution() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:.")) {
      for (String function : List.of("LAG", "LEAD")) {
        for (String suffix : List.of("", " WHERE id > 5")) {
          String query = "SELECT id," + function + "(id,5,'x') OVER (ORDER BY id) " + "FROM (VALUES (1),(2)) t(id)"
              + suffix;
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(query), query);
          assertEquals("42000", failure.getSQLState(), query);
        }
      }
      String array = "SELECT LAG(ARRAY[id],1,ARRAY['x']) OVER (ORDER BY id) FROM (VALUES (1)) t(id)";
      assertEquals("42000",
          assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(array)).getSQLState());
    }
  }

  /**
   * Numeric assignment truncates fractions toward zero without discarding leading
   * digits.
   *
   * @throws Exception
   *           if JDBC evaluation fails
   */
  @Test
  void truncatesNumericDefaultsAndRejectsOverflow() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:.")) {
      for (String fraction : List.of("1.5", "-1.5")) {
        String query = "SELECT id,LEAD(id,5," + fraction + ") OVER (ORDER BY id)," + "LAG(id,1," + fraction
            + ") OVER (ORDER BY id) FROM (VALUES (1),(2)) t(id) ORDER BY id";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(query)) {
          assertTrue(result.next(), query);
          int truncated = fraction.startsWith("-") ? -1 : 1;
          assertEquals(truncated, result.getInt(2));
          assertEquals(truncated, result.getInt(3));
          assertEquals(result.getMetaData().getColumnClassName(2), result.getObject(2).getClass().getName());
          assertTrue(result.next(), query);
          assertEquals(truncated, result.getInt(2));
          assertEquals(1, result.getInt(3));
          assertFalse(result.next(), query);
        }
      }
      String overflow = "SELECT LAG(CAST(id AS INTEGER),1,2147483648) OVER (ORDER BY id) FROM (VALUES (1)) t(id)";
      try (var prepared = connection.prepareStatement(overflow)) {
        assertEquals("22003", assertThrows(SQLDataException.class, prepared::executeQuery).getSQLState());
      }
    }
  }

  /**
   * Keeps ARRAY defaults assignable without requiring one concrete Java array
   * class.
   *
   * @throws Exception
   *           if JDBC evaluation fails
   */
  @Test
  void retainsArrayDefaults() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:.")) {
      for (String function : List.of("LAG", "LEAD")) {
        String query = "SELECT " + function + "(ARRAY[id],1,ARRAY[9]) OVER (ORDER BY id) "
            + "FROM (VALUES (1),(2)) t(id) ORDER BY id";
        int[] expected = function.equals("LAG") ? new int[]{
            9, 1
        } : new int[]{
            2, 9
        };
        try (var statement = connection.createStatement(); var result = statement.executeQuery(query)) {
          for (int value : expected) {
            assertTrue(result.next(), query);
            Object[] array = (Object[]) result.getArray(1).getArray();
            assertEquals(1, array.length, query);
            assertEquals(value, ((Number) array[0]).intValue(), query);
          }
          assertFalse(result.next(), query);
        }
      }
    }
  }
}
