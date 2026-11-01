package jparq.support;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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

/** Shared Parquet fixture creation and JDBC row collection for query tests. */
public final class QueryTestSupport {

  /** Prevents utility class instantiation. */
  private QueryTestSupport() {
  }

  /**
   * Writes table {@code o(n)} with nullable INTEGER values 1, 1, 2, 3, and NULL.
   *
   * @param directory
   *          existing directory in which to create {@code o.parquet}
   * @throws IOException
   *           if fixture creation fails
   */
  public static void writeIntegerFixture(Path directory) throws IOException {
    Schema schema = SchemaBuilder.record("Row").fields().optionalInt("n").endRecord();
    var configuration = new Configuration(false);
    var file = new org.apache.hadoop.fs.Path(directory.resolve("o.parquet").toUri());
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
   * Collects projected values through either JDBC statement API without value
   * conversion, preserving NULLs and duplicate column positions.
   *
   * @param connection
   *          fixture connection, which remains open
   * @param sql
   *          query to execute
   * @param prepared
   *          whether to use a prepared statement
   * @return all projected row values
   * @throws SQLException
   *           if preparation, execution, or value access fails
   */
  public static List<List<Object>> rows(Connection connection, String sql, boolean prepared) throws SQLException {
    try (var statement = prepared ? connection.prepareStatement(sql) : connection.createStatement();
        ResultSet result = prepared ? ((PreparedStatement) statement).executeQuery() : statement.executeQuery(sql)) {
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
