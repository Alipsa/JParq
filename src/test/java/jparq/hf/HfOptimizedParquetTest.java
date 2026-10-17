package jparq.hf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.TimeZone;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqPreparedStatement;
import se.alipsa.jparq.engine.ParquetFileInfo;
import se.alipsa.jparq.engine.ParquetFilterBuilder;
import se.alipsa.jparq.engine.ParquetSchemas;

/** Verifies that the committed PyArrow fixtures exercise indexed CDC pages. */
class HfOptimizedParquetTest {

  /** Checks all logical and nested types in a non-UTC default time zone. */
  @Test
  void preservesLogicalTypesAndTimestampPrecision() throws Exception {
    TimeZone previous = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Stockholm"));
    try {
      String directory = java.nio.file.Path.of(getClass().getResource("/hf").toURI()).toString();
      for (boolean projection : new boolean[]{
          true, false
      }) {
        try (
            var connection = DriverManager
                .getConnection("jdbc:jparq:" + directory + "?projectionPushdown=" + projection);
            var statement = connection.createStatement()) {
          for (String name : List.of("plain", "optimized")) {
            try (var result = statement.executeQuery("SELECT * FROM " + name + " LIMIT 1")) {
              assertTrue(result.next());
              assertEquals(java.sql.Date.valueOf("2022-01-09"), result.getDate("date32"));
              assertEquals(Timestamp.from(Instant.ofEpochMilli(1700000000001L)), result.getTimestamp("ts_ms"));
              assertEquals(Timestamp.from(Instant.ofEpochSecond(1700000000L, 1000)), result.getTimestamp("ts_us"));
              assertEquals(Timestamp.from(Instant.ofEpochSecond(1700000000L, 1)), result.getTimestamp("ts_ns"));
              assertEquals(Timestamp.valueOf("2023-11-14 22:13:20.000001"), result.getTimestamp("ts_local"));
              assertEquals(new java.math.BigDecimal("0.01"), result.getBigDecimal("amount"));
              assertEquals(1.5f, result.getFloat("half"));
              org.junit.jupiter.api.Assertions.assertArrayEquals(new Object[]{
                  1, 2
              }, (Object[]) result.getObject("items"));
              assertTrue(result.getString("image").contains("image.png"));
            }
            try (var result = statement.executeQuery("SELECT ts_ns, ts_us, half FROM " + name + " LIMIT 1")) {
              assertTrue(result.next());
              assertEquals(1, result.getTimestamp("ts_ns").getNanos());
              assertEquals(1000, result.getTimestamp("ts_us").getNanos());
              assertEquals(1.5f, result.getFloat("half"));
            }
            for (String condition : List.of("date32 = DATE '2022-01-09'", "ts_ms = '2023-11-14 23:13:20.001'",
                "ts_us = '2023-11-14 23:13:20.000001'", "ts_ns = '2023-11-14 23:13:20.000000001'",
                "ts_local = '2023-11-14 22:13:20.000001'", "ts_ns IN ('2023-11-14 23:13:20.000000001')",
                "ts_us BETWEEN '2023-11-14 23:13:20.000001' AND '2023-11-14 23:13:20.000001'", "amount IN (0.01)",
                "amount BETWEEN 0.01 AND 0.01")) {
              try (var result = statement.executeQuery("SELECT id FROM " + name + " WHERE " + condition)) {
                assertTrue(result.next(), condition);
                assertEquals(1, result.getLong(1), condition);
                org.junit.jupiter.api.Assertions.assertFalse(result.next(), condition);
              }
            }
            try (var columns = connection.getMetaData().getColumns(null, null, name, "%")) {
              int count = 0;
              while (columns.next()) {
                count++;
                switch (columns.getString("COLUMN_NAME")) {
                  case "image" -> assertEquals(java.sql.Types.STRUCT, columns.getInt("DATA_TYPE"));
                  case "items" -> assertEquals(java.sql.Types.ARRAY, columns.getInt("DATA_TYPE"));
                  case "half" -> assertEquals(java.sql.Types.REAL, columns.getInt("DATA_TYPE"));
                  case "ts_ns", "ts_local" -> assertEquals(java.sql.Types.TIMESTAMP, columns.getInt("DATA_TYPE"));
                  default -> {
                  }
                }
              }
              assertEquals(13, count);
            }
          }
        }
      }
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  /** Proves actual page-index pruning independently of query-result equality. */
  @Test
  void pageIndexSkipsRowsWithinAStatisticsMatchingRowGroup() throws Exception {
    long plainCount = filteredCount("plain", "id BETWEEN 100 AND 110");
    assertEquals(8192, plainCount);
    long optimizedCount = filteredCount("optimized", "id BETWEEN 100 AND 110");
    assertTrue(optimizedCount >= 11);
    assertTrue(optimizedCount < plainCount);
    for (String predicate : List.of("id = 100", "id IN (100, 101)", "id >= 100 AND id <= 110", "id < 110", "id > 16370",
        "id != 100 AND id BETWEEN 100 AND 110", "amount BETWEEN 1.00 AND 1.10", "amount IN (1.00, 1.01)",
        "text LIKE 'prefix-001%'", "n IS NULL")) {
      assertTrue(filteredCount("optimized", predicate) < filteredCount("plain", predicate), predicate);
    }
  }

  /**
   * Compares temporal pushdown with fixed results and Java filtering in a non-UTC
   * zone.
   */
  @Test
  void temporalBoundsRoundInThePhysicalColumnUnit() throws Exception {
    TimeZone previous = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Stockholm"));
    try {
      String directory = java.nio.file.Path.of(getClass().getResource("/hf").toURI()).toString();
      for (boolean pushdown : new boolean[]{
          true, false
      }) {
        try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown);
            var statement = connection.createStatement()) {
          for (String name : List.of("plain", "optimized")) {
            for (String condition : List.of("ts_ms < '2023-11-14 23:13:20.0025'", "ts_ms <= '2023-11-14 23:13:20.0025'",
                "ts_us < '2023-11-14 23:13:20.0000025'", "ts_local < '2023-11-14 22:13:20.0000025'")) {
              try (var result = statement
                  .executeQuery("SELECT id FROM " + name + " WHERE " + condition + " ORDER BY id")) {
                assertTrue(result.next(), condition);
                assertEquals(1, result.getLong(1));
                assertTrue(result.next(), condition);
                assertEquals(2, result.getLong(1));
                org.junit.jupiter.api.Assertions.assertFalse(result.next(), condition);
              }
            }
            try (var result = statement
                .executeQuery("SELECT COUNT(*) FROM " + name + " WHERE ts_us = '2023-11-14 23:13:20'")) {
              assertTrue(result.next());
              assertEquals(0, result.getLong(1));
            }
          }
        }
      }
    } finally {
      TimeZone.setDefault(previous);
    }
  }

  /**
   * Opens the production filter configuration and measures the physical row
   * ranges retained.
   */
  private long filteredCount(String name, String condition) throws Exception {
    Path path = new Path(getClass().getResource("/hf/" + name + ".parquet").toURI());
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

  /** Verifies footer diagnostics and CLI output on nested PyArrow files. */
  @Test
  void describesIndexedFilesAndConnectionOptions() throws Exception {
    Path path = new Path(getClass().getResource("/hf/optimized.parquet").toURI());
    var info = ParquetFileInfo.read(path, new Configuration(false));
    assertEquals(16384, info.rowCount());
    assertEquals(2, info.rowGroups().size());
    assertTrue(info.pageIndexPresent());
    var output = new java.io.StringWriter();
    var errors = new java.io.StringWriter();
    try (var session = new se.alipsa.jparq.cli.JParqCliSession(new java.io.PrintWriter(output, true),
        new java.io.PrintWriter(errors, true))) {
      session.connectDirectory(java.nio.file.Path.of(getClass().getResource("/hf").toURI()).toString());
      session.handleLine("/describe optimized");
      session.handleLine("/info");
      assertTrue(output.toString().contains("Page index: present"));
      assertTrue(output.toString().contains("image"));
      assertTrue(output.toString().contains("Predicate pushdown: true"));
      assertTrue(output.toString().contains("Projection pushdown: true"));
      assertEquals("", errors.toString());
    }
  }

  /** Checks that both writer layouts can be queried through JDBC. */
  @Test
  void readsBothWriterLayouts() throws Exception {
    String directory = java.nio.file.Path.of(getClass().getResource("/hf").toURI()).toString();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = connection.createStatement()) {
      for (String name : new String[]{
          "plain", "optimized"
      }) {
        try (var result = statement.executeQuery("SELECT COUNT(*) FROM " + name)) {
          assertTrue(result.next());
          assertEquals(16384, result.getLong(1));
        }
        try (var result = statement.executeQuery("SELECT * FROM " + name + " LIMIT 1")) {
          assertTrue(result.next());
          assertEquals(1, result.getLong("id"));
          assertEquals(1, result.getInt("n"));
          assertEquals("prefix-00001", result.getString("text"));
          assertEquals(1, result.getInt("label"));
        }
      }
    }
  }

  /**
   * Checks file sizes, row groups, index presence, and enough pages for pruning
   * tests.
   */
  @Test
  void fixturesContainMultipleRowGroupsAndIndexedPages() throws Exception {
    for (String name : new String[]{
        "plain", "optimized"
    }) {
      var resource = getClass().getResource("/hf/" + name + ".parquet");
      assertNotNull(resource);
      assertTrue(Files.size(java.nio.file.Path.of(resource.toURI())) <= 1024 * 1024);
      Path path = new Path(resource.toURI());
      try (
          ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration(false)))) {
        assertEquals(16384, reader.getRecordCount());
        assertEquals(2, reader.getFooter().getBlocks().size());
        assertTrue(reader.getFooter().getFileMetaData().getKeyValueMetaData().containsKey("huggingface"));
        for (var group : reader.getFooter().getBlocks()) {
          assertEquals(8192, group.getRowCount());
          for (var column : group.getColumns()) {
            boolean optimized = "optimized".equals(name);
            assertEquals(optimized, column.getColumnIndexReference() != null);
            assertEquals(optimized, column.getOffsetIndexReference() != null);
            if (optimized && "id".equals(column.getPath().toDotString())) {
              assertTrue(reader.readOffsetIndex(column).getPageCount() >= 8);
            }
          }
        }
      }
    }
  }
}
