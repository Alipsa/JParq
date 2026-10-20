package jparq.cte;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Guards recursive CTE alignment against numeric truncation and rounding. */
class RecursiveCteConversionTest {
  @TempDir
  Path directory;

  /**
   * Supplies table-free and FROM-backed anchors for both recursive UNION modes.
   *
   * @return anchors, recursive increments, and expected conversion errors
   */
  static Stream<Arguments> lossyRecursions() {
    List<Arguments> cases = new ArrayList<>();
    for (String union : List.of("UNION", "UNION ALL")) {
      for (String source : List.of("", " FROM (VALUES (1)) e(id) WHERE id = 1")) {
        for (String anchor : List.of("1", "CAST(1 AS INTEGER)")) {
          cases.add(Arguments.of(anchor + source, union, "0.5", "3", "without loss"));
        }
        cases.add(Arguments.of("0.5" + source, union, "1", "30", "exceeds decimal precision"));
        cases.add(Arguments.of("1.5" + source, union, "0.25", "2.5", "without rounding"));
        cases.add(Arguments.of("CAST(2147483647 AS INTEGER)" + source, union, "1", "2147483648", "without loss"));
        cases.add(Arguments.of("9223372036854775807" + source, union, "1", "9223372036854775808", "without loss"));
      }
    }
    return cases.stream();
  }

  /**
   * Fails promptly if a recursive value cannot fit its anchor's numeric type.
   *
   * @param anchor
   *          the anchor SELECT projection and optional FROM clause
   * @param union
   *          the recursive set operator
   * @param increment
   *          the recursive numeric increment
   * @param bound
   *          the exclusive recursion bound
   * @param message
   *          the expected reason for rejecting the conversion
   * @throws SQLException
   *           if the connection or statement cannot be opened
   */
  @ParameterizedTest
  @MethodSource("lossyRecursions")
  void lossyRecursionFailsInsteadOfHangingOrChangingValues(String anchor, String union, String increment, String bound,
      String message) throws SQLException {
    String sql = "WITH RECURSIVE r(n) AS (SELECT " + anchor + " " + union + " SELECT n + " + increment
        + " FROM r WHERE n < " + bound + ") SELECT n FROM r";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      try {
        SQLException failure = assertTimeoutPreemptively(Duration.ofSeconds(5),
            () -> assertThrows(SQLException.class, () -> statement.executeQuery(sql)));
        Throwable cause = failure;
        while (cause != null) {
          if (cause instanceof SQLException sqlException && "22003".equals(sqlException.getSQLState())) {
            assertTrue(cause.getMessage().contains(message), cause.getMessage());
            return;
          }
          cause = cause.getCause();
        }
        fail("Expected numeric conversion failure with SQLState 22003: " + failure);
      } finally {
        statement.cancel();
      }
    }
  }

  /**
   * Keeps representable decimal steps exact even when their trailing scale
   * differs.
   *
   * @param union
   *          the recursive set operator
   * @throws SQLException
   *           if the query fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UNION", "UNION ALL"
  })
  void exactDecimalStepsRemainSupported(String union) throws SQLException {
    String sql = "WITH RECURSIVE r(n) AS (SELECT 1.5 " + union
        + " SELECT n + 0.50 FROM r WHERE n < 2.5) SELECT n FROM r ORDER BY n";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      List<Double> values = new ArrayList<>();
      while (result.next()) {
        values.add(result.getDouble(1));
      }
      assertEquals(List.of(1.5, 2.0, 2.5), values);
    }
  }
}
