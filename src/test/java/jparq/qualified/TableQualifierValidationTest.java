package jparq.qualified;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Verifies relation qualifier visibility independently of pushdown and row
 * counts.
 */
class TableQualifierValidationTest {

  /**
   * Rejects unknown and alias-hidden qualifiers throughout single-input and join
   * queries.
   *
   * @param predicatePushdown
   *          whether predicates may be pushed into Parquet
   * @param projectionPushdown
   *          whether projections may be pushed into Parquet
   * @throws Exception
   *           if JDBC setup or execution fails
   */
  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void validatesQualifiers(boolean predicatePushdown, boolean projectionPushdown) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources/hf").toAbsolutePath() + "?predicatePushdown="
        + predicatePushdown + "&projectionPushdown=" + projectionPushdown;
    try (var connection = DriverManager.getConnection(url)) {
      for (String filter : List.of("id = 1", "id < 0")) {
        for (String source : List.of("optimized x", "(SELECT id FROM optimized) x",
            "optimized x JOIN (VALUES (2)) j(k) ON TRUE", "(SELECT id FROM optimized) x JOIN (VALUES (2)) j(k) ON TRUE",
            "c x")) {
          String prefix = source.equals("c x") ? "WITH c AS (SELECT id FROM optimized) " : "";
          String from = " FROM " + source;
          for (String qualifier : List.of("nosuch", source.equals("c x") ? "c" : "optimized")) {
            for (String query : List.of("SELECT " + qualifier + ".id" + from + " WHERE x." + filter,
                "SELECT COUNT(" + qualifier + ".id)" + from + " WHERE x." + filter,
                "SELECT x.id" + from + " WHERE x." + filter + " AND " + qualifier + ".id = 1",
                "SELECT x.id" + from + " WHERE x." + filter + " ORDER BY " + qualifier + ".id",
                "SELECT x.id" + from + " WHERE x." + filter + " ORDER BY " + qualifier + ".id + 0",
                "SELECT COUNT(*)" + from + " WHERE x." + filter + " GROUP BY " + qualifier + ".id",
                "SELECT COUNT(*)" + from + " WHERE x." + filter + " HAVING MAX(" + qualifier + ".id) = 1",
                "SELECT " + qualifier + ".*" + from + " WHERE x." + filter)) {
              String sql = prefix + query;
              SQLSyntaxErrorException error = assertThrows(SQLSyntaxErrorException.class, () -> {
                try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
                  result.next();
                }
              }, sql);
              assertEquals("42000", error.getSQLState(), sql);
              assertTrue(error.getMessage().contains(qualifier), sql + ": " + error);
            }
          }
        }
      }
      for (String sql : List.of("SELECT optimized.id FROM optimized WHERE optimized.id = 1",
          "SELECT X.id FROM optimized x WHERE x.id = 1 ORDER BY x.id",
          "SELECT \"x\".id FROM optimized x WHERE x.id = 1",
          "SELECT \"My X\".id FROM optimized \"My X\" WHERE \"My X\".id = 1",
          "SELECT \"a.b\" FROM (SELECT 1 AS \"a.b\") x", "SELECT x.\"a.b\" FROM (SELECT 1 AS \"a.b\") x",
          "SELECT \"a.b\".id FROM optimized \"a.b\" WHERE \"a.b\".id = 1 ORDER BY \"a.b\".id",
          "SELECT \"a.b\".id + 0 FROM optimized \"a.b\" WHERE \"a.b\".id = 1 ORDER BY \"a.b\".id + 0",
          "SELECT \"a.b\".id FROM optimized \"a.b\" WHERE \"a.b\".id = 1 GROUP BY \"a.b\".id "
              + "HAVING MAX(\"a.b\".id) = 1 ORDER BY \"a.b\".id",
          "SELECT \"a.b\".id FROM (VALUES (1)) \"a.b\"(id) WHERE EXISTS "
              + "(SELECT 1 FROM (VALUES (1)) v(k) WHERE v.k = \"a.b\".id)",
          "WITH c AS (SELECT id FROM optimized) SELECT x.id FROM c x WHERE x.id = 1",
          "SELECT x.id FROM (SELECT id FROM optimized) x JOIN (VALUES (1)) j(k) ON x.id = j.k",
          "SELECT x.id FROM (VALUES (1)) x(id) WHERE x.id = 1 AND EXISTS (SELECT 1 WHERE x.id = 1)",
          "SELECT x.id FROM (VALUES (1)) x(id) WHERE x.id = 1 AND EXISTS "
              + "(SELECT 1 FROM (VALUES (1)) y(id) WHERE y.id = x.id)",
          "SELECT x.id FROM (VALUES (1)) x(id) WHERE x.id = 1 AND EXISTS "
              + "(SELECT 1 FROM (VALUES (2)) x(id) WHERE x.id = 2)")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt(1), sql);
          assertFalse(result.next(), sql);
        }
      }
      for (String table : List.of("tables", "columns")) {
        String sql = "SELECT COUNT(*) FROM information_schema." + table + " WHERE " + table
            + ".table_name = 'optimized'";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertTrue(result.getInt(1) > 0, sql);
        }
      }
      for (String invalid : List.of("SELECT \"X\".id FROM optimized x", "SELECT \"my x\".id FROM optimized \"My X\"",
          "SELECT id FROM optimized \"My X\" WHERE \"my x\".id = 1", "SELECT id FROM optimized x ORDER BY \"X\".id",
          "SELECT \"X\".* FROM optimized x", "SELECT \"my x\".* FROM optimized \"My X\"")) {
        SQLSyntaxErrorException error = assertThrows(SQLSyntaxErrorException.class,
            () -> connection.prepareStatement(invalid), invalid);
        assertEquals("42000", error.getSQLState(), invalid);
      }
      for (String invalid : List.of(
          "SELECT x.id FROM optimized x WHERE EXISTS (SELECT 1 FROM (VALUES (1)) v(k) WHERE v.k = optimized.id)",
          "SELECT id FROM optimized x WHERE EXISTS (SELECT 1 FROM optimized y WHERE optimized.id = y.id)",
          "SELECT x.id FROM optimized x JOIN (VALUES (1)) j(k) ON TRUE WHERE EXISTS "
              + "(SELECT 1 FROM (VALUES (1)) v(k) WHERE v.k = optimized.id)")) {
        SQLSyntaxErrorException error = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(invalid)) {
            result.next();
            result.getObject(1);
          }
        }, invalid);
        assertEquals("42000", error.getSQLState(), invalid);
      }
      String sql = "SELECT nosuch.id FROM optimized x WHERE x.id = ?";
      try (var statement = connection.prepareStatement(sql)) {
        statement.setInt(1, 1);
        SQLSyntaxErrorException error = assertThrows(SQLSyntaxErrorException.class, statement::executeQuery);
        assertEquals("42000", error.getSQLState());
      }
    }
  }
  /**
   * Checks complete schema qualifiers and regressions reported in issues 293 and
   * 295.
   *
   * @param pushdown
   *          whether predicate and projection pushdown are enabled
   * @throws Exception
   *           if JDBC setup or a valid query fails
   */
  @ParameterizedTest
  @CsvSource({
      "true", "false"
  })
  void validatesSchemaQualifiersAndSelfJoins(boolean pushdown) throws Exception {
    String url = "jdbc:jparq:" + Path.of("src/test/resources").toAbsolutePath() + "?predicatePushdown=" + pushdown
        + "&projectionPushdown=" + pushdown;
    try (var connection = DriverManager.getConnection(url)) {
      for (String sql : List.of("SELECT COUNT(acme.employees.id) FROM acme.employees",
          "SELECT COUNT(\"acme\".\"employees\".id) FROM acme.employees",
          "SELECT COUNT(\"acme.employees\".id) FROM acme.employees \"acme.employees\" "
              + "WHERE \"acme.employees\".id > 0")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(5, result.getInt(1), sql);
        }
      }
      for (String qualifier : List.of("acme.employees", "\"acme\".employees")) {
        String sql = "SELECT COUNT(*) FROM acme.employees WHERE EXISTS "
            + "(SELECT 1 FROM (VALUES (1)) v(k) WHERE v.k = " + qualifier + ".id)";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt(1), sql);
        }
      }
      for (String sql : List.of("SELECT acme.employees.* FROM acme.employees ORDER BY acme.employees.id",
          "SELECT acme.employees.id FROM acme.employees JOIN (VALUES (1)) j(k) ON acme.employees.id = j.k",
          "SELECT \"a.b\".id FROM acme.employees \"a.b\" JOIN (VALUES (1)) j(k) ON \"a.b\".id = j.k "
              + "ORDER BY \"a.b\".id")) {
        try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
          assertTrue(result.next(), sql);
          assertEquals(1, result.getInt(1), sql);
        }
      }
      for (String sql : List.of("SELECT COUNT(nosuch.employees.id) FROM acme.employees",
          "SELECT COUNT(nosuch.e.id) FROM acme.employees e", "SELECT id FROM acme.employees e WHERE nosuch.e.id = 1",
          "SELECT id FROM acme.employees e ORDER BY nosuch.e.id", "SELECT nosuch.employees.* FROM acme.employees",
          "SELECT acme.e.* FROM acme.employees e",
          "SELECT COUNT(e.id) FROM acme.employees e WHERE EXISTS "
              + "(SELECT 1 FROM acme.salary s WHERE s.employee = employees.id)",
          "SELECT COUNT(*) FROM mtcars m JOIN (SELECT 1 AS a) x ON TRUE WHERE zz.cyl = 4",
          "SELECT COUNT(*) FROM mtcars m JOIN (SELECT 1 AS a) x ON TRUE WHERE m.cyl = 4 AND zz.cyl = 99",
          "SELECT COUNT(*) FROM mtcars m JOIN (SELECT 1 AS a) x ON TRUE WHERE \"M\".cyl = 4",
          "SELECT a.x FROM (VALUES (1),(3),(2)) a(x) JOIN (VALUES (2)) b(y) ON TRUE ORDER BY z.x DESC",
          "SELECT a.x FROM (VALUES (1),(3),(2)) a(x) JOIN (VALUES (2)) b(y) ON TRUE ORDER BY z.y DESC",
          "SELECT mtcars.cyl FROM mtcars a JOIN mtcars b ON a.model = b.model LIMIT 1",
          "SELECT mtcars.* FROM mtcars a JOIN mtcars b ON a.model = b.model LIMIT 1")) {
        SQLSyntaxErrorException error = assertThrows(SQLSyntaxErrorException.class, () -> {
          try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            result.next();
            result.getObject(1);
          }
        }, sql);
        assertEquals("42000", error.getSQLState(), sql);
      }
    }
  }

}
