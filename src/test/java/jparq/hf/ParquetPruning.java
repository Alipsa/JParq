package jparq.hf;

import static org.junit.jupiter.api.Assertions.assertTrue;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import se.alipsa.jparq.JParqPreparedStatement;
import se.alipsa.jparq.engine.ParquetFilterBuilder;
import se.alipsa.jparq.engine.ParquetSchemas;

/**
 * Measures the physical row ranges retained by the production filter
 * configuration.
 */
final class ParquetPruning {
  private ParquetPruning() {
  }

  /** Reads the filtered physical count for a file and SQL condition. */
  static long filteredCount(Path path, String condition) throws Exception {
    Configuration configuration = new Configuration(false);
    JParqPreparedStatement.configureParquetFiltering(configuration);
    var schema = ParquetSchemas.readAvroSchema(path, configuration);
    var predicate = ParquetFilterBuilder.split(schema, CCJSqlParserUtil.parseCondExpression(condition)).predicate();
    assertTrue(predicate.isPresent());
    var options = HadoopReadOptions.builder(configuration, path).withRecordFilter(FilterCompat.get(predicate.get()))
        .build();
    try (var reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, configuration), options)) {
      return reader.getFilteredRecordCount();
    }
  }
}
