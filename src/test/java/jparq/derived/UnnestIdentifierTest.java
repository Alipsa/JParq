package jparq.derived;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies SQL identifier binding for correlated UNNEST columns. */
class UnnestIdentifierTest {

  /**
   * Resolves quoted lowercase references through ordinary and grouped inputs.
   *
   * @param grouped
   *          whether the preceding inputs form a join group
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void quotedLowercaseNames(boolean grouped) throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      String inputs = "(SELECT ARRAY[1, 2] AS a) m CROSS JOIN (SELECT ARRAY[3] AS a) n";
      if (grouped) {
        inputs = "(" + inputs + ")";
      }
      for (String argument : List.of("\"n\".a", "n.\"a\"", "\"n\".\"a\"")) {
        String sql = "SELECT u.x FROM " + inputs + " CROSS JOIN UNNEST(" + argument + ") AS u(x)";
        try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
          assertTrue(result.next(), sql);
          assertEquals(3, result.getInt(1), sql);
          assertFalse(result.next(), sql);
        }
      }
      String single = "(SELECT ARRAY[3] AS a) n";
      if (grouped) {
        single = "(" + single + " CROSS JOIN (SELECT 1 AS k) m)";
      }
      String sql = "SELECT u.x FROM " + single + " CROSS JOIN UNNEST(\"a\") AS u(x)";
      try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
        assertTrue(result.next(), sql);
        assertEquals(3, result.getInt(1), sql);
        assertFalse(result.next(), sql);
      }
    }
  }

  /**
   * Binds exact quoted SQL labels rather than generated Avro field names.
   *
   * @param grouped
   *          whether the source belongs to a join group
   * @throws Exception
   *           if execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void exactQuotedNames(boolean grouped) throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      for (String name : List.of("a", "Mixed", "has space", "a.b", "a\"\"b")) {
        String quoted = "\"" + name + "\"";
        String input = "(SELECT ARRAY[3] AS " + quoted + ") \"My T\"";
        if (grouped) {
          input = "(" + input + " CROSS JOIN (SELECT 1 AS k) m)";
        }
        for (String argument : List.of(quoted, "\"My T\"." + quoted)) {
          String sql = "SELECT u.x FROM " + input + " CROSS JOIN UNNEST(" + argument + ") AS u(x)";
          try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
            assertTrue(result.next(), sql);
            assertEquals(3, result.getInt(1), sql);
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }

  /**
   * Classifies missing columns and qualifiers even when the input has no rows.
   *
   * @param empty
   *          whether the preceding input is empty
   * @throws Exception
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void missingNamesHaveSyntaxState(boolean empty) throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      String input = "(SELECT ARRAY[3] AS a" + (empty ? " WHERE FALSE" : "") + ") n";
      for (String source : List.of(input, "(" + input + " CROSS JOIN (SELECT 1 AS k) m)")) {
        for (String argument : List.of("n.nosuch", "nosuch", "n.\"nosuch\"", "\"nosuch\"", "\"N\".a", "n.\"A\"",
            "\"A\"")) {
          String sql = "SELECT COUNT(*) FROM " + source + " CROSS JOIN UNNEST(" + argument + ") AS u(x)";
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> {
            try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
              result.next();
            }
          }, sql);
          assertEquals("42000", failure.getSQLState(), sql);
          assertFalse(failure.getMessage().contains("table 'null'"), failure.getMessage());
          assertTrue(failure.getMessage().contains(argument.replace("n.", "")), failure.getMessage());
        }
      }
    }
  }

  /**
   * Rejects unqualified duplicate SQL labels within and across join inputs.
   *
   * @param grouped
   *          whether duplicates belong to a grouped input
   * @throws Exception
   *           if connection setup fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void ambiguousNames(boolean grouped) throws Exception {
    try (var connection = DriverManager.getConnection(url())) {
      String inputs = "(SELECT ARRAY[1] AS a) m CROSS JOIN (SELECT ARRAY[3] AS a) n";
      if (grouped) {
        inputs = "(" + inputs + ")";
      }
      for (String argument : List.of("a", "\"a\"")) {
        String sql = "SELECT COUNT(*) FROM " + inputs + " CROSS JOIN UNNEST(" + argument + ") AS u(x)";
        var failure = assertThrows(SQLException.class, () -> {
          try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
            result.next();
          }
        }, sql);
        assertEquals("42702", failure.getSQLState(), sql);
      }
    }
  }

  /**
   * Builds a JDBC URL for the test resources.
   *
   * @return JDBC URL for the test resources
   */
  private static String url() {
    return "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath();
  }
}
