package jparq.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Verifies explicit rejection of unsupported information_schema alias lists.
 */
class InformationSchemaAliasListTest {

  /**
   * Checks both virtual schemas as standalone, nested, and joined inputs.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @throws Exception
   *           if JDBC execution fails unexpectedly
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void rejectsAliasLists(boolean predicate, boolean projection) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown=" + predicate
        + "&projectionPushdown=" + projection;
    try (var connection = DriverManager.getConnection(url)) {
      for (String table : List.of("information_schema.tables", "information_schema.columns")) {
        for (String source : List.of(table + " t(a)", table + " AS t(a)", "(" + table + ") t(a)")) {
          for (String sql : List.of("SELECT a FROM " + source, "SELECT * FROM " + source,
              "SELECT * FROM " + source + " JOIN (VALUES (1)) s(z) ON TRUE",
              "SELECT s.z FROM (VALUES (1)) s(z) JOIN " + source + " ON s.z = 1",
              "SELECT * FROM (SELECT * FROM " + source + ") d")) {
            SQLException failure = assertThrows(SQLException.class, () -> {
              try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
                while (result.next()) {
                  result.getObject(1);
                }
              }
            }, sql);
            assertEquals("42000", failure.getSQLState(), sql);
            assertTrue(
                failure.getMessage().contains("Column alias lists on information_schema inputs are not supported"),
                sql + ": " + failure);
          }
        }
        for (String sql : List.of("SELECT * FROM " + table + " t LIMIT 1",
            "SELECT s.z FROM (VALUES (1)) s(z) JOIN " + table + " t ON s.z = 1 LIMIT 1")) {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertTrue(result.next(), sql);
            if (sql.startsWith("SELECT s.z")) {
              assertEquals(1, result.getInt(1), sql);
            }
            assertFalse(result.next(), sql);
          }
        }
      }
    }
  }
}
