package jparq.window.navigation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies navigation defaults whose SQL type has multiple engine
 * representations.
 */
class NavigationDefaultTypeTest {
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
