package jparq.setops;

import static jparq.support.QueryTestSupport.rows;
import static jparq.support.QueryTestSupport.writeIntegerFixture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Resolves set operation ORDER BY labels, including quoted lowercase references
 * to unquoted output labels, and classifies unknown labels with SQLState 42000
 * (#385).
 */
class SetOperationOrderByLabelTest {

  /**
   * Resolves a quoted lowercase reference to the unquoted output label, matching
   * single-select ORDER BY resolution.
   *
   * @param prepared
   *          whether to use a prepared statement
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void resolvesQuotedLowercaseLabels(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      List<List<Object>> expected = Arrays.asList(List.of(1), List.of(1), List.of(1), List.of(1), List.of(2),
          List.of(2), List.of(3), List.of(3), Arrays.asList((Object) null), Arrays.asList((Object) null));
      assertEquals(expected, rows(connection, "SELECT n FROM o UNION ALL SELECT n FROM o ORDER BY \"n\"", prepared));
      assertEquals(expected,
          rows(connection, "SELECT n AS renamed FROM o UNION ALL SELECT n FROM o ORDER BY \"renamed\"", prepared));
    }
  }

  /**
   * Rejects unknown labels, including quoted references whose case does not
   * match, with the syntax classification.
   *
   * @param prepared
   *          whether to use a prepared statement
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void classifiesUnknownLabels(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String reference : List.of("nosuch", "\"N\"")) {
        String sql = "SELECT n FROM o UNION ALL SELECT n FROM o ORDER BY " + reference;
        SQLSyntaxErrorException failure = assertThrows(SQLSyntaxErrorException.class,
            () -> rows(connection, sql, prepared), sql);
        assertEquals("42000", failure.getSQLState(), sql);
        assertTrue(failure.getMessage().contains("Unknown set operation ORDER BY column: " + reference),
            failure.getMessage());
      }
    }
  }
}