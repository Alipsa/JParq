package jparq.aggregate;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Verifies DISTINCT and row limits on completed aggregate results. */
class AggregateResultClausesTest {

  @TempDir
  Path base;

  /**
   * Writes groups with duplicate counts and a NULL grouping key.
   *
   * @throws Exception
   *           if fixture creation fails
   */
  @BeforeEach
  void fixture() throws Exception {
    Schema schema = SchemaBuilder.record("Row").fields().optionalInt("n").endRecord();
    var configuration = new Configuration(false);
    var file = new org.apache.hadoop.fs.Path(base.resolve("o.parquet").toUri());
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(file, configuration))
        .withSchema(schema).withConf(configuration).build()) {
      for (Integer value : Arrays.asList(1, 1, 2, 3, null)) {
        var row = new GenericData.Record(schema);
        row.put("n", value);
        writer.write(row);
      }
    }
  }

  /**
   * Checks grouped ordering before row slicing, including boundary values.
   *
   * @param prepared
   *          whether to use a prepared statement
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void groupedLimitsFollowOrdering(boolean prepared) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + base)) {
      String sql = "SELECT n, COUNT(*) FROM o GROUP BY n ORDER BY n NULLS LAST";
      assertEquals(List.of(List.of(1, 2L), List.of(2, 1L)), rows(connection, sql + " LIMIT 2", prepared));
      assertEquals(List.of(Arrays.asList(null, 1L)), rows(connection, sql + " OFFSET 3 ROWS", prepared));
      assertEquals(List.of(List.of(2, 1L), List.of(3, 1L)), rows(connection, sql + " LIMIT 2 OFFSET 1", prepared));
      assertEquals(List.of(List.of(2, 1L)), rows(connection, sql + " OFFSET 1 ROW FETCH NEXT 1 ROW ONLY", prepared));
      assertEquals(List.of(), rows(connection, sql + " LIMIT 0", prepared));
      assertEquals(List.of(), rows(connection, sql + " OFFSET 2147483647 ROWS", prepared));
      assertEquals(List.of(List.of(2, 1L), List.of(3, 1L), Arrays.asList(null, 1L)),
          rows(connection, sql + " LIMIT 2147483647 OFFSET 1", prepared));
    }
  }

  /**
   * Checks projected DISTINCT keys and their interaction with HAVING and limits.
   *
   * @param prepared
   *          whether to use a prepared statement
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void distinctUsesProjectedValuesBeforeSlicing(boolean prepared) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + base)) {
      String sql = "SELECT DISTINCT COUNT(*) c FROM o GROUP BY n";
      assertEquals(List.of(List.of(2L), List.of(1L)), rows(connection, sql, prepared));
      assertEquals(List.of(List.of(2L)), rows(connection, sql + " ORDER BY c OFFSET 1 ROWS", prepared));
      assertEquals(List.of(List.of(1L)), rows(connection, sql + " ORDER BY c LIMIT 1", prepared));
      assertEquals(List.of(List.of(2L)), rows(connection, sql + " ORDER BY c LIMIT 1 OFFSET 1", prepared));
      assertEquals(List.of(List.of(1L)), rows(connection, sql + " HAVING COUNT(*) = 1 ORDER BY c", prepared));
      assertEquals(List.of(List.of(1L)),
          rows(connection, "SELECT DISTINCT COUNT(*) + 0 c FROM o GROUP BY n ORDER BY c LIMIT 1", prepared));
      assertEquals(List.of(),
          rows(connection, "SELECT DISTINCT COUNT(*) c, COUNT(*) c FROM o GROUP BY n ORDER BY 1 LIMIT 0", prepared));
    }
  }

  /**
   * Checks NULL equality, duplicate labels, and mixed whole-number values.
   *
   * @param prepared
   *          whether to use a prepared statement
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void distinctPreservesNullsAndTuplePositions(boolean prepared) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + base)) {
      assertEquals(List.of(Arrays.asList((Object) null)),
          rows(connection, "SELECT DISTINCT CASE WHEN COUNT(*) > 0 THEN NULL ELSE 1 END FROM o GROUP BY n", prepared));
      assertEquals(List.of(List.of(1L, 0), List.of(2L, 1)),
          rows(connection,
              "SELECT DISTINCT COUNT(*) c, CASE WHEN n = 1 THEN 1 ELSE 0 END c " + "FROM o GROUP BY n ORDER BY 1",
              prepared));
      assertEquals(List.of(List.of(1L)),
          rows(connection, "SELECT DISTINCT CASE WHEN n = 1 THEN 1 ELSE COUNT(*) END FROM o GROUP BY n", prepared));
    }
  }

  /**
   * Checks global aggregation, empty inputs, and materialized aggregate inputs.
   *
   * @param prepared
   *          whether to use a prepared statement
   * @throws Exception
   *           if query execution fails
   */
  @ParameterizedTest
  @ValueSource(booleans = {
      false, true
  })
  void globalAndNestedAggregatesHonorLimits(boolean prepared) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + base)) {
      assertEquals(List.of(), rows(connection, "SELECT COUNT(*) FROM o LIMIT 0", prepared));
      assertEquals(List.of(), rows(connection, "SELECT COUNT(*) FROM o OFFSET 1 ROW", prepared));
      assertEquals(List.of(List.of(0L)), rows(connection, "SELECT COUNT(*) FROM o WHERE 1 = 0 LIMIT 1", prepared));
      assertEquals(List.of(), rows(connection, "SELECT COUNT(*) FROM o WHERE 1 = 0 LIMIT 0", prepared));
      assertEquals(List.of(List.of(1L)), rows(connection,
          "SELECT COUNT(*) FROM (SELECT DISTINCT COUNT(*) c FROM o GROUP BY n ORDER BY c LIMIT 1) d", prepared));
      assertEquals(List.of(List.of(2L)), rows(connection,
          "WITH c AS (SELECT n FROM o GROUP BY n ORDER BY n LIMIT 2) SELECT COUNT(*) FROM c", prepared));
    }
  }

  /**
   * Checks that bound predicates recompute the limited aggregate result.
   *
   * @throws SQLException
   *           if query execution fails
   */
  @Test
  void boundPredicatesRecomputeLimitedResults() throws SQLException {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + base);
        var statement = connection.prepareStatement(
            "SELECT DISTINCT COUNT(*) c FROM o WHERE n >= ? " + "GROUP BY n ORDER BY c LIMIT 1 OFFSET 1")) {
      for (int minimum : List.of(1, 2, 1)) {
        statement.setInt(1, minimum);
        try (var result = statement.executeQuery()) {
          List<Long> counts = new ArrayList<>();
          while (result.next()) {
            counts.add(result.getLong(1));
          }
          assertEquals(minimum == 1 ? List.of(2L) : List.of(), counts);
        }
      }
    }
  }

  /**
   * Executes through either JDBC statement API and checks empty-result metadata.
   *
   * @param connection
   *          fixture connection
   * @param sql
   *          query to execute
   * @param prepared
   *          whether to use a prepared statement
   * @return all projected row values
   * @throws SQLException
   *           if execution fails
   */
  private static List<List<Object>> rows(Connection connection, String sql, boolean prepared) throws SQLException {
    try (var statement = prepared ? connection.prepareStatement(sql) : connection.createStatement();
        ResultSet result = prepared ? ((PreparedStatement) statement).executeQuery() : statement.executeQuery(sql)) {
      if (sql.equals("SELECT COUNT(*) FROM o LIMIT 0")) {
        assertEquals(Types.BIGINT, result.getMetaData().getColumnType(1));
      }
      List<List<Object>> rows = new ArrayList<>();
      while (result.next()) {
        List<Object> row = new ArrayList<>();
        for (int column = 1; column <= result.getMetaData().getColumnCount(); column++) {
          row.add(result.getObject(column));
        }
        rows.add(row);
      }
      return rows;
    }
  }
}
