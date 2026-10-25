package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Regression coverage for dollar-quoted strings through JDBC query preparation.
 */
class DollarQuotedStringTest {
  @TempDir
  Path directory;

  /**
   * Preserves identifiers starting with tagged openers through JDBC preparation.
   *
   * @param identifier
   *          the output alias and derived column name
   * @throws SQLException
   *           if preparing or executing a query fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "$a$b", "$tag$abc$tag$", "$_tag1$x"
  })
  void retainsIdentifiersStartingWithTaggedOpeners(String identifier) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String sql : new String[]{
          "SELECT 1 AS " + identifier, "SELECT " + identifier + " FROM (SELECT 1 AS " + identifier + ") d"
      }) {
        try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
          assertEquals(identifier, result.getMetaData().getColumnLabel(1), sql);
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt(1), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Retains quotes and comment markers through projection and materialization.
   *
   * @param body
   *          the literal contents
   * @throws SQLException
   *           if preparing or executing a query fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "it's", "it''s", "a--b", "a/*b", "a*/b", "a\"b", "a`b", "", "first\n--second\n/*third*/", "quotes ' \" -- /*",
      "prefix $other$nested$other$ suffix"
  })
  void evaluatesDollarQuotedStrings(String body) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      String literal = "$$" + body + "$$";
      for (String sql : new String[]{
          "SELECT " + literal + " AS x -- trailing comment",
          "WITH c AS (SELECT " + literal + " AS x /* real comment */) SELECT x FROM c",
          "SELECT x FROM (SELECT " + literal + " AS x) d WHERE x = " + literal
      }) {
        try (var statement = connection.prepareStatement(sql); var result = statement.executeQuery()) {
          assertTrue(result.next(), sql);
          assertEquals(body, result.getString(1), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }
}
