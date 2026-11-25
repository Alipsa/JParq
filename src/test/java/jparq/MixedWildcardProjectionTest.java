package jparq;

import static jparq.support.QueryTestSupport.rows;
import static jparq.support.QueryTestSupport.writeEmptyIntegerFixture;
import static jparq.support.QueryTestSupport.writeIntegerFixture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Projects an unqualified {@code *} mixed with other select items (#609): the
 * result contains the expanded wildcard columns plus the additional items in
 * select-list order.
 */
class MixedWildcardProjectionTest {

  /**
   * Keeps the additional item after the wildcard columns, with the wildcard
   * columns first.
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
  void appendsExtraItemsAfterTheWildcard(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertEquals(
          Arrays.asList(List.of(1, 2L), List.of(1, 2L), List.of(2, 3L), List.of(3, 4L),
              Arrays.asList((Object) null, (Object) null)),
          rows(connection, "SELECT *, n + 1 AS m FROM o ORDER BY 1 NULLS LAST", prepared));
    }
  }

  /**
   * Keeps the additional item before the wildcard columns when it precedes the
   * wildcard in the select list.
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
  void prependsExtraItemsBeforeTheWildcard(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertEquals(
          Arrays.asList(List.of(2L, 1), List.of(2L, 1), List.of(3L, 2), List.of(4L, 3),
              Arrays.asList((Object) null, (Object) null)),
          rows(connection, "SELECT n + 1 AS m, * FROM o ORDER BY 2 NULLS LAST", prepared));
    }
  }

  /**
   * Deduplicates on the whole mixed projection, wildcard columns and additional
   * items together.
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
  void deduplicatesTheWholeMixedProjection(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertEquals(
          Arrays.asList(List.of(1, 2L), List.of(2, 3L), List.of(3, 4L), Arrays.asList((Object) null, (Object) null)),
          rows(connection, "SELECT DISTINCT *, n + 1 AS m FROM o ORDER BY 1 NULLS LAST", prepared));
    }
  }

  /**
   * Exposes the mixed projection to a consuming derived table, which reads both
   * the wildcard column and the additional item by name.
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
  void exposesTheMixedProjectionToADerivedTable(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertEquals(List.of(List.of(4L, 3), List.of(3L, 2), List.of(2L, 1), List.of(2L, 1)), rows(connection,
          "SELECT m, n FROM (SELECT *, n + 1 AS m FROM o WHERE n IS NOT NULL) d ORDER BY m DESC", prepared));
    }
  }

  /**
   * Reports the full mixed projection in the metadata of an empty result,
   * including the wildcard column labels.
   *
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @Test
  void reportsMixedProjectionMetadataForAnEmptyResult(@TempDir Path directory) throws Exception {
    writeEmptyIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT *, n + 1 AS m FROM e")) {
      var meta = result.getMetaData();
      assertEquals(2, meta.getColumnCount());
      assertEquals("n", meta.getColumnLabel(1));
      assertEquals("m", meta.getColumnLabel(2));
      assertFalse(result.next());
    }
  }
}