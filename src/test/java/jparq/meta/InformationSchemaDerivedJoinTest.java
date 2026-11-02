package jparq.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Verifies that joins execute derived queries over metadata views. */
class InformationSchemaDerivedJoinTest {

  /**
   * Preserves aggregates, projections, aliases, ordering, and row bounds.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @throws SQLException
   *           if a query fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void executesDerivedQueries(boolean predicate, boolean projection) throws SQLException {
    try (var connection = connect(predicate, projection)) {
      for (String view : List.of("tables", "columns")) {
        String source = "information_schema." + view;
        long expected = count(connection, source, "");
        for (String lateral : List.of("", "LATERAL ")) {
          for (String selection : List.of("d.*", "d.n")) {
            String sql = "SELECT s.id, " + selection + " FROM (VALUES (1), (2)) s(id) CROSS JOIN " + lateral
                + "(SELECT COUNT(*) n FROM " + source + " t) d ORDER BY s.id";
            try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
              assertEquals(2, result.getMetaData().getColumnCount(), sql);
              assertEquals("n", result.getMetaData().getColumnLabel(2), sql);
              assertEquals(Types.BIGINT, result.getMetaData().getColumnType(2), sql);
              for (int id : List.of(1, 2)) {
                assertTrue(result.next(), sql);
                assertEquals(id, result.getInt(1), sql);
                assertEquals(expected, result.getLong(2), sql);
                assertFalse(result.wasNull(), sql);
              }
              assertFalse(result.next(), sql);
            }
          }
          String sql = "SELECT s.id, d.* FROM (VALUES (1)) s(id) CROSS JOIN " + lateral + "(SELECT TABLE_NAME FROM "
              + source + " WHERE TABLE_NAME IN ('employees', 'departments') ORDER BY TABLE_NAME DESC LIMIT 1) d(name)";
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertEquals(2, result.getMetaData().getColumnCount(), sql);
            assertEquals("name", result.getMetaData().getColumnLabel(2), sql);
            assertTrue(result.next(), sql);
            assertEquals("employees", result.getString(2), sql);
            assertFalse(result.next(), sql);
          }
        }
        String sql = "SELECT d.n FROM (SELECT COUNT(*) n FROM " + source + ") d CROSS JOIN (VALUES (1)) s(id)";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(expected, result.getLong(1), sql);
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Evaluates correlated filters for each outer row and preserves empty inputs.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @throws SQLException
   *           if a query fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void correlatesLateralQueries(boolean predicate, boolean projection) throws SQLException {
    try (var connection = connect(predicate, projection)) {
      for (String view : List.of("tables", "columns")) {
        String source = "information_schema." + view;
        String outer = "(VALUES ('departments'), ('employees'), ('missing')) s(name)";
        String sql = "SELECT s.name, d.n FROM " + outer + " CROSS JOIN LATERAL (SELECT COUNT(*) n FROM " + source
            + " t WHERE t.TABLE_NAME = s.name) d ORDER BY s.name";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          for (String name : List.of("departments", "employees", "missing")) {
            assertTrue(result.next(), sql);
            assertEquals(name, result.getString(1), sql);
            assertEquals(count(connection, source, " WHERE TABLE_NAME = '" + name + "'"), result.getLong(2), sql);
            assertFalse(result.wasNull(), sql);
          }
          assertFalse(result.next(), sql);
        }
        sql = "SELECT s.name, d.name FROM " + outer + " LEFT JOIN LATERAL (SELECT TABLE_NAME name FROM " + source
            + " t WHERE t.TABLE_NAME = s.name ORDER BY TABLE_NAME LIMIT 1) d ON TRUE ORDER BY s.name";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          for (String name : List.of("departments", "employees", "missing")) {
            assertTrue(result.next(), sql);
            assertEquals(name, result.getString(1), sql);
            assertEquals(name.equals("missing") ? null : name, result.getString(2), sql);
            assertEquals(name.equals("missing"), result.wasNull(), sql);
          }
          assertFalse(result.next(), sql);
        }
      }
    }
  }

  /**
   * Opens the fixture database with the requested pushdown settings.
   *
   * @param predicate
   *          predicate pushdown setting
   * @param projection
   *          projection pushdown setting
   * @return an open connection
   * @throws SQLException
   *           if the connection fails
   */
  private Connection connect(boolean predicate, boolean projection) throws SQLException {
    return DriverManager.getConnection("jdbc:jparq:" + Path.of("src/test/resources/acme").toAbsolutePath()
        + "?predicatePushdown=" + predicate + "&projectionPushdown=" + projection);
  }

  /**
   * Counts metadata rows independently of join execution.
   *
   * @param connection
   *          fixture connection
   * @param source
   *          metadata view
   * @param filter
   *          optional WHERE clause
   * @return the matching row count
   * @throws SQLException
   *           if the count fails
   */
  private long count(Connection connection, String source, String filter) throws SQLException {
    try (var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT COUNT(*) FROM " + source + filter)) {
      assertTrue(result.next());
      return result.getLong(1);
    }
  }
}
