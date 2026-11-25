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
import org.junit.jupiter.api.Disabled;
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
   * Orders by a column that the wildcard and an explicit item both project: the
   * two output columns denote the same input column, so the reference is not
   * ambiguous.
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
  void ordersByAColumnRepeatedThroughTheWildcard(boolean prepared, @TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertEquals(
          Arrays.asList(List.of(1, 1), List.of(1, 1), List.of(2, 2), List.of(3, 3),
              Arrays.asList((Object) null, (Object) null)),
          rows(connection, "SELECT *, n FROM o ORDER BY n", prepared));
    }
  }

  /**
   * Runs a star mixed with an extra item over an input with duplicate column
   * names without the 42702 that expanding the star would cause (#605). Only the
   * execution and the row count are asserted: the projected column count is a
   * known deviation until #605/#609 are fixed, since the extra item {@code k} is
   * dropped and {@code ORDER BY k} reports a misleading "Missing columns for
   * table 'x'" error that names the input rather than the dropped item.
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
  void toleratesDuplicateColumnNamesWithoutAmbiguityErrors(boolean prepared, @TempDir Path directory) throws Exception {
    String join = "(VALUES (1, 10)) a(id, v) JOIN (VALUES (1, 20)) b(id, v) ON a.id = b.id";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertEquals(1, rows(connection, "SELECT *, 1 AS k FROM (SELECT * FROM " + join + ") x", prepared).size());
      assertEquals(1,
          rows(connection, "SELECT *, 1 AS k FROM (SELECT a.id, a.v, b.v FROM " + join + ") x", prepared).size());
    }
  }

  /**
   * Projects the extra item alongside a star over an input with duplicate column
   * names: the standard result carries the wildcard columns plus {@code k}. The
   * duplicate-name fallback still drops {@code k} (the pre-#609 behavior), so
   * this pins the correct expectation for when #605/#609 are fixed instead of
   * locking in the narrow result.
   *
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @Disabled("#605/#609: the duplicate-name fallback drops the extra select item")
  @Test
  void keepsExtraItemsOverDuplicateColumnNames(@TempDir Path directory) throws Exception {
    String join = "(VALUES (1, 10)) a(id, v) JOIN (VALUES (1, 20)) b(id, v) ON a.id = b.id";
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertEquals(List.of(List.of(1, 10, 1, 20, 1)),
          rows(connection, "SELECT *, 1 AS k FROM (SELECT * FROM " + join + ") x", false));
      assertEquals(List.of(List.of(1, 10, 20, 1)),
          rows(connection, "SELECT *, 1 AS k FROM (SELECT a.id, a.v, b.v FROM " + join + ") x", false));
    }
  }

  /**
   * Sorts a join wildcard-plus-expression projection by output position. Joins
   * and LATERAL inputs still take the select-all path that drops the extra item
   * (#609 remains open for them), so this pins the desired behavior for when the
   * expansion extends to multiple inputs.
   *
   * @param directory
   *          fixture directory
   * @throws Exception
   *           if fixture creation or query execution fails
   */
  @Disabled("#609: joins and LATERAL inputs still drop select items mixed with *")
  @Test
  void keepsExtraItemsOnJoins(@TempDir Path directory) throws Exception {
    writeIntegerFixture(directory);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      assertEquals(
          Arrays.asList(List.of(1, 1, 1), List.of(1, 1, 1), List.of(2, 1, 1), List.of(3, 1, 1),
              Arrays.asList((Object) null, (Object) 1, (Object) 1)),
          rows(connection, "SELECT *, 1 AS k FROM o JOIN (VALUES (1)) v(x) ON TRUE ORDER BY 1", false));
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