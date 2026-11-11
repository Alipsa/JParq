package jparq.window.navigation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies navigation defaults whose SQL type has multiple engine
 * representations.
 */
class NavigationDefaultTypeTest {
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
