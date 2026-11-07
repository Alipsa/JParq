package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.JParqPreparedStatement;

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
      "$a$b", "$tag$abc$tag$", "$_tag1$x", "a$$b", "$_tag1$a$$b$other$c$_tag1$"
  })
  void retainsIdentifiersStartingWithTaggedOpeners(String identifier) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      for (String sql : new String[]{
          "SELECT ? AS " + identifier, "SELECT " + identifier + " FROM (SELECT ? AS " + identifier + ") d"
      }) {
        try (var statement = connection.prepareStatement(sql)) {
          assertEquals(1, statement.getParameterMetaData().getParameterCount(), sql);
          statement.setInt(1, 1);
          try (var result = statement.executeQuery()) {
            assertEquals(identifier, result.getMetaData().getColumnLabel(1), sql);
            assertTrue(result.next(), sql);
            assertEquals(1, result.getInt(1), sql);
            assertFalse(result.next(), sql);
          }
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
      "?", ":name", "? :name", "it's", "it''s", "a--b", "a/*b", "a*/b", "a\"b", "a`b", "", "first\n--second\n/*third*/",
      "quotes ' \" -- /*", "prefix $other$nested$other$ suffix"
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
          assertEquals(0, statement.getParameterMetaData().getParameterCount(), sql);
          assertTrue(result.next(), sql);
          assertEquals(body, result.getString(1), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Counts and binds only real parameters, preserving literal contents on each
   * execution.
   *
   * @param body
   *          the literal contents, including text that resembles SQL syntax
   * @throws SQLException
   *           if preparing, binding, or executing a query fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "?", ":name", "? :name", "it's ? :name", "a\"b ? :name", "-- ? :name", "/* ? :name", "*/ ? :name",
      "first\n-- ?\n/* :name */", "", "prefix $other$? :name$other$ suffix"
  })
  void bindsParametersAroundDollarQuotedStrings(String body) throws SQLException {
    String sql = "SELECT ? AS before_value, $$" + body + "$$ AS literal_value, :name AS named_value, "
        + "? AS after_value, :name AS repeated_value, $$? :name$$ AS second_literal";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = (JParqPreparedStatement) connection.prepareStatement(sql)) {
      assertEquals(4, statement.getParameterMetaData().getParameterCount());
      for (String value : new String[]{
          "first", "it's ? :name -- /* $$"
      }) {
        statement.setInt(1, 7);
        statement.setString("name", value);
        statement.setInt(3, 9);
        try (var result = statement.executeQuery()) {
          assertTrue(result.next());
          assertEquals(7, result.getInt(1));
          assertEquals(body, result.getString(2));
          assertEquals(value, result.getString(3));
          assertEquals(9, result.getInt(4));
          assertEquals(value, result.getString(5));
          assertEquals("? :name", result.getString(6));
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Rejects unterminated dollar quotes during preparation.
   *
   * @param literal
   *          the unfinished literal
   * @throws SQLException
   *           if opening or closing the connection fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "$$", "$$unfinished ? :name", "$$it's -- /*"
  })
  void rejectsUnterminatedDollarQuotes(String literal) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      var exception = assertThrows(SQLException.class, () -> connection.prepareStatement("SELECT " + literal));
      assertEquals("42000", exception.getSQLState());
    }
  }

  /**
   * Keeps dollar delimiters inside backtick identifiers out of parameter
   * scanning.
   *
   * @param sql
   *          the query containing backtick identifiers
   * @throws SQLException
   *           if preparing, binding, or executing the query fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT 1 AS `$$`", "SELECT 1 AS `$$a`, ? AS b, 2 AS `c$$`", "SELECT 1 AS `a``$$? :name`, ? AS b, 2 AS `c$$`"
  })
  void preservesDollarQuotesInBacktickIdentifiers(String sql) throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.prepareStatement(sql)) {
      int count = sql.equals("SELECT 1 AS `$$`") ? 0 : 1;
      assertEquals(count, statement.getParameterMetaData().getParameterCount());
      if (count > 0) {
        statement.setInt(1, 7);
      }
      try (var result = statement.executeQuery()) {
        assertTrue(result.next());
        assertEquals(1, result.getInt(1));
        if (count > 0) {
          assertEquals(7, result.getInt(2));
          assertEquals(2, result.getInt(3));
        } else {
          assertEquals("$$", result.getMetaData().getColumnLabel(1));
        }
        assertFalse(result.next());
      }
    }
  }

}
