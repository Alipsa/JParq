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

/** JDBC regression tests for quoted text containing function escape syntax. */
class JdbcFunctionEscapeLiteralTest {

  @TempDir
  Path directory;

  /**
   * Preserves literal values and quoted aliases through statement preprocessing.
   *
   * @param literal
   *          a SQL literal containing escape-like text
   * @throws SQLException
   *           if connection or query execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "'{fn UCASE(''a'')}'", "$${fn UCASE('a')}$$"
  })
  void preservesLiteralsAndAliases(String literal) throws SQLException {
    String sql = "/* {fn NOW()} */ SELECT " + literal + " AS \"{fn UCASE('a')}\", "
        + "{fn LCASE('B')} AS actual -- {fn NOW()}";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory.toAbsolutePath());
        var statement = connection.createStatement();
        var result = statement.executeQuery(sql)) {
      assertTrue(result.next());
      assertEquals("{fn UCASE('a')}", result.getString(1));
      assertEquals("{fn UCASE('a')}", result.getMetaData().getColumnLabel(1));
      assertEquals("b", result.getString("actual"));
      assertFalse(result.next());
    }
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory.toAbsolutePath());
        var statement = connection.prepareStatement("SELECT " + literal + " AS x, {fn UCASE(?)} AS actual")) {
      statement.setString(1, "b");
      try (var result = statement.executeQuery()) {
        assertTrue(result.next());
        assertEquals("{fn UCASE('a')}", result.getString("x"));
        assertEquals("B", result.getString("actual"));
        assertFalse(result.next());
      }
    }
  }
}
