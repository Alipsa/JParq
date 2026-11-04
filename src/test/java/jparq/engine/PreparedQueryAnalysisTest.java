package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.SqlParser;

/** Verifies datetime dependency detection throughout parsed query scopes. */
class PreparedQueryAnalysisTest {
  /**
   * Recognizes datetime keywords, function forms, and nested query scopes.
   *
   * @param sql
   *          datetime-dependent query
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT CURRENT_TIMESTAMP", "SELECT CURRENT_TIME", "SELECT CURRENT_DATE", "SELECT LOCALTIME",
      "SELECT LOCALTIMESTAMP", "SELECT CURRENT_DATE()", "SELECT {fn CURTIME()}", "SELECT {fn NOW()}",
      "WITH c AS (SELECT CURRENT_TIMESTAMP AS ts) SELECT ts FROM c", "SELECT * FROM (SELECT LOCALTIMESTAMP AS ts) t",
      "SELECT 1 UNION ALL SELECT EXTRACT(HOUR FROM LOCALTIME)", "SELECT (SELECT CURRENT_TIMESTAMP)",
      "SELECT * FROM (VALUES (CURRENT_DATE)) t(d)", "SELECT * FROM UNNEST(ARRAY[CURRENT_TIME]) t(v)",
      "SELECT 1 FROM mtcars WHERE EXISTS (SELECT 1 WHERE CURRENT_TIME = CURRENT_TIME)",
      "SELECT ROW_NUMBER() OVER (ORDER BY CURRENT_TIMESTAMP) FROM mtcars"
  })
  void detectsDatetimeDependency(String sql) {
    assertTrue(SqlParser.prepareQuery(sql).timeDependent(), sql);
  }

  /**
   * Quoted identifiers, strings, and comments do not create datetime
   * dependencies.
   *
   * @param sql
   *          query without datetime value functions
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT 'CURRENT_TIMESTAMP'", "SELECT 1 AS \"CURRENT_DATE\"", "SELECT \"LOCALTIME\" FROM mtcars",
      "SELECT m.\"CURRENT_TIMESTAMP\" FROM mtcars m", "SELECT 1 /* CURRENT_TIME */",
      "WITH c AS (SELECT RAND() AS v) SELECT v FROM c", "SELECT 1 -- LOCALTIMESTAMP"
  })
  void ignoresDatetimeTextAndIdentifiers(String sql) {
    assertFalse(SqlParser.prepareQuery(sql).timeDependent(), sql);
  }
}
