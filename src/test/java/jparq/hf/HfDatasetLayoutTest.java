package jparq.hf;

import static org.junit.jupiter.api.Assertions.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.cli.JParqCliSession;
import se.alipsa.jparq.engine.AvroProjections;
import se.alipsa.jparq.engine.LogicalSchemas;
import se.alipsa.jparq.engine.ParquetFileInfo;
import se.alipsa.jparq.engine.ParquetSchemas;
import se.alipsa.jparq.engine.ParquetTable;

/**
 * Verifies logical shard execution, schema boundaries, metadata, and CLI
 * behavior.
 */
class HfDatasetLayoutTest {
  @TempDir
  Path base;

  private static Schema schema(String name) {
    return new Schema.Parser().parse("""
        {"type":"record","name":"%s","namespace":"test.%s","fields":[
          {"name":"id","type":"int"}, {"name":"n","type":["null","int"],"default":null},
          {"name":"nested","type":{"type":"record","name":"Nested%s","fields":[
            {"name":"label","type":"string"}]}}, {"name":"unused","type":"string"}]}
        """.formatted(name, name, name));
  }

  private static void write(Path path, Schema schema, int... ids) throws Exception {
    Files.createDirectories(path.getParent());
    Configuration conf = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(path.toUri()), conf);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(conf)
        .withExtraMetaData(java.util.Map.of("comment", schema.getName(), "column.comment.id", schema.getName()))
        .build()) {
      for (int id : ids) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("n", id == 2 ? null : id);
        var nested = new GenericData.Record(schema.getField("nested").schema());
        nested.put("label", "row-" + id);
        row.put("nested", nested);
        row.put("unused", "ignored");
        writer.write(row);
      }
    }
  }

  @BeforeEach
  void fixtures() throws Exception {
    write(base.resolve("data/train-0-of-3-a.parquet"), schema("First"), 1, 2);
    write(base.resolve("data/train-1-of-3-b.parquet"), schema("Empty"));
    write(base.resolve("data/train-2-of-3-c.parquet"), schema("Last"), 3, 4);
    write(base.resolve("single.parquet"), schema("Single"), 1, 2, 3, 4);
  }

  private Connection connect(boolean predicate, boolean projection) throws SQLException {
    return DriverManager.getConnection("jdbc:jparq:" + base + "?datasetLayout=hf&predicatePushdown=" + predicate
        + "&projectionPushdown=" + projection);
  }

  private static List<List<Object>> rows(Connection conn, String sql) throws SQLException {
    try (var stmt = conn.createStatement(); var rs = stmt.executeQuery(sql)) {
      List<List<Object>> rows = new ArrayList<>();
      int count = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        List<Object> row = new ArrayList<>();
        for (int index = 1; index <= count; index++) {
          row.add(rs.getObject(index));
        }
        rows.add(row);
      }
      return rows;
    }
  }

  @Test
  void allPushdownCombinationsAgreeAcrossRelationalBoundaries() throws Exception {
    for (boolean predicate : List.of(true, false)) {
      for (boolean projection : List.of(true, false)) {
        try (var conn = connect(predicate, projection)) {
          for (String template : List.of("SELECT id, n FROM %s ORDER BY id",
              "SELECT id FROM %s WHERE id >= 3 ORDER BY id", "SELECT id FROM %s ORDER BY id LIMIT 2 OFFSET 1",
              "SELECT COUNT(*), SUM(id), MIN(n), MAX(n) FROM %s", "SELECT DISTINCT n FROM %s ORDER BY n",
              "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM %s ORDER BY id",
              "SELECT a.id FROM %s a JOIN single b ON a.id = b.id WHERE a.id >= 3 ORDER BY a.id",
              "WITH x AS (SELECT id FROM %s ORDER BY id OFFSET 1 ROWS) SELECT id FROM x ORDER BY id",
              "SELECT id FROM (SELECT id FROM %s ORDER BY id LIMIT 3) x ORDER BY id",
              "SELECT id FROM single WHERE id IN (SELECT id FROM %s WHERE id > 2) ORDER BY id",
              "SELECT id FROM %s WHERE id > 2 UNION ALL SELECT id FROM single WHERE id = 1 ORDER BY id",
              "SELECT a.id, (SELECT COUNT(*) FROM single b WHERE b.id < a.id) AS c FROM %s a ORDER BY a.id",
              "SELECT id FROM %s TABLESAMPLE BERNOULLI (50) REPEATABLE (42) ORDER BY id")) {
            assertEquals(rows(conn, template.formatted("single")), rows(conn, template.formatted("data.train")),
                template + " predicate=" + predicate + " projection=" + projection);
          }
        }
      }
    }
  }

  @Test
  void projectedRecordsAndAdaptersKeepEachShardsOwnNames() throws Exception {
    try (var conn = (JParqConnection) connect(true, true)) {
      var table = ParquetTable.validate(conn.resolveTable("data", "train"), new Configuration(false));
      AtomicInteger reads = new AtomicInteger();
      var observed = ParquetTable.validate(conn.resolveTable("data", "train"), new Configuration(false),
          (path, conf) -> {
            reads.incrementAndGet();
            return ParquetSchemas.readAvroSchema(path, conf);
          });
      assertEquals(3, reads.get());
      assertEquals(3, observed.shards().size());
      for (boolean projection : List.of(true, false)) {
        Configuration configuration = new Configuration(false);
        if (projection) {
          var projected = AvroProjections.project(table.schema(), Set.of("id", "nested"));
          AvroReadSupport.setRequestedProjection(configuration, projected);
          AvroReadSupport.setAvroReadSchema(configuration, projected);
        }
        String original = configuration.get(AvroReadSupport.AVRO_REQUESTED_PROJECTION);
        var last = table.shards().getLast();
        var ownConfig = ParquetTable.configurationFor(last, configuration);
        assertEquals(original, configuration.get(AvroReadSupport.AVRO_REQUESTED_PROJECTION));
        if (projection) {
          var own = new Schema.Parser().parse(ownConfig.get(AvroReadSupport.AVRO_REQUESTED_PROJECTION));
          assertEquals("Last", own.getName());
          assertEquals("NestedLast", own.getField("nested").schema().getName());
          assertNull(own.getField("unused"));
          assertEquals(own.toString(), ownConfig.get("parquet.avro.read.schema"));
        }
        try (var reader = table.open(configuration, null)) {
          for (int id = 1; id <= 4; id++) {
            GenericRecord row = reader.read();
            String name = id <= 2 ? "First" : "Last";
            assertEquals(name, row.getSchema().getName());
            assertEquals("Nested" + name, ((GenericRecord) row.get("nested")).getSchema().getName());
            assertEquals("row-" + id, ((GenericRecord) row.get("nested")).get("label").toString());
            assertEquals(projection ? 2 : 4, row.getSchema().getFields().size());
          }
          assertNull(reader.read());
          assertNull(reader.read());
        }
      }
    }
    for (boolean projection : List.of(true, false)) {
      try (var conn = connect(true, projection)) {
        assertEquals(rows(conn, "SELECT id, nested FROM single ORDER BY id"),
            rows(conn, "SELECT id, nested FROM data.train ORDER BY id"));
        assertEquals(4, rows(conn, "SELECT a.nested FROM data.train a JOIN single b ON a.id = b.id").size());
      }
    }
  }

  private void incompatibleGroup() throws Exception {
    Files.createDirectories(base.resolve("other"));
    Files.copy(base.resolve("single.parquet"), base.resolve("other/bad-0-of-2.parquet"));
    var output = HadoopOutputFile.fromPath(
        new org.apache.hadoop.fs.Path(base.resolve("other/bad-1-of-2.parquet").toUri()), new Configuration(false));
    // A zero-column record cannot be written as Parquet: use a differently typed
    // id.
    Schema wrong = new Schema.Parser().parse("""
        {"type":"record","name":"Wrong","fields":[{"name":"id","type":"string"}]}
        """);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(wrong).build()) {
      var row = new GenericData.Record(wrong);
      row.put("id", "wrong");
      writer.write(row);
    }
  }

  @Test
  void tableListingsStayCheapWhileBroadColumnMetadataValidatesTheWholeCatalog() throws Exception {
    incompatibleGroup();
    try (var conn = connect(true, true)) {
      try (var tables = conn.getMetaData().getTables(null, null, "%", null)) {
        int count = 0;
        while (tables.next()) {
          count++;
        }
        assertEquals(3, count);
      }
      assertEquals(3, rows(conn, "SELECT table_name FROM information_schema.tables").size());
      assertEquals(4, rows(conn, "SELECT id FROM data.train").size());
      try (var columns = conn.getMetaData().getColumns(null, "data", "train", "%")) {
        assertTrue(columns.next());
      }
      assertThrows(SQLException.class, () -> conn.getMetaData().getColumns(null, null, "%", "%"));
      assertThrows(SQLException.class,
          () -> rows(conn, "SELECT * FROM information_schema.columns WHERE table_name = 'train'"));
      SQLException failure = assertThrows(SQLException.class, () -> rows(conn, "SELECT * FROM other.bad"));
      assertTrue(messages(failure).contains("Incompatible shard schema"));
      assertTrue(messages(failure).contains("bad-1-of-2"));
      assertThrows(SQLException.class, () -> rows(conn, "SELECT * FROM data.train a JOIN other.bad b ON a.id = b.id"));
    }
    StringWriter output = new StringWriter();
    StringWriter errors = new StringWriter();
    try (var cli = new JParqCliSession(new PrintWriter(output, true), new PrintWriter(errors, true))) {
      cli.connectDirectory(base + "?datasetLayout=hf");
      cli.handleLine("/list");
      assertEquals("", errors.toString());
      assertTrue(output.toString().contains("other.bad"));
      cli.handleLine("/describe other.bad");
      assertTrue(errors.toString().contains("Incompatible shard schema"));
    }
  }

  private static String messages(Throwable failure) {
    StringBuilder result = new StringBuilder();
    for (Throwable current = failure; current != null; current = current.getCause()) {
      result.append(current.getMessage());
    }
    return result.toString();
  }

  @Test
  void tableListingsNeverOpenLaterShardFooters() throws Exception {
    Files.writeString(base.resolve("data/train-2-of-3-c.parquet"), "not a footer");
    try (var conn = connect(true, true)) {
      assertEquals(2, ((JParqConnection) conn).listTables().size());
      try (var tables = conn.getMetaData().getTables(null, null, "%", null)) {
        assertTrue(tables.next());
      }
      assertEquals(2, rows(conn, "SELECT * FROM information_schema.tables").size());
      assertThrows(SQLException.class, () -> conn.getMetaData().getColumns(null, "data", "train", "%"));
      assertThrows(SQLException.class, () -> rows(conn, "SELECT * FROM data.train"));
    }
  }

  @Test
  void emptyAndExhaustedResultsKeepRepresentativeMetadata() throws Exception {
    for (boolean predicate : List.of(true, false)) {
      for (boolean projection : List.of(true, false)) {
        try (var conn = connect(predicate, projection); var stmt = conn.createStatement()) {
          try (var result = stmt.executeQuery("SELECT * FROM data.train WHERE id > 99")) {
            assertEquals(4, result.getMetaData().getColumnCount());
            assertEquals(java.sql.Types.INTEGER, result.getMetaData().getColumnType(1));
            assertFalse(result.next());
            assertEquals(4, result.getMetaData().getColumnCount());
          }
          try (var result = stmt.executeQuery("SELECT id FROM data.train")) {
            while (result.next()) {
              assertEquals(java.sql.Types.INTEGER, result.getMetaData().getColumnType(1));
            }
            assertEquals(java.sql.Types.INTEGER, result.getMetaData().getColumnType(1));
          }
        }
      }
    }
  }

  @Test
  void describeSelectsLiteralNamesBeforeValidationInBothLayouts() throws Exception {
    Files.copy(base.resolve("single.parquet"), base.resolve("data/train_a.parquet"));
    Files.copy(base.resolve("single.parquet"), base.resolve("data/train.a.parquet"));
    Files.copy(base.resolve("single.parquet"), base.resolve("data/train%a.parquet"));
    Files.copy(base.resolve("single.parquet"), base.resolve("data/train\\a.parquet"));
    incompatibleGroup();
    Files.move(base.resolve("other/bad-0-of-2.parquet"), base.resolve("data/train-a-0-of-2.parquet"));
    Files.move(base.resolve("other/bad-1-of-2.parquet"), base.resolve("data/train-a-1-of-2.parquet"));
    for (String layout : List.of("hf", "none")) {
      StringWriter output = new StringWriter();
      StringWriter errors = new StringWriter();
      try (var cli = new JParqCliSession(new PrintWriter(output, true), new PrintWriter(errors, true))) {
        cli.connectDirectory(base + "?datasetLayout=" + layout);
        for (String name : List.of("train_a", "train.a", "train%a", "train\\a")) {
          output.getBuffer().setLength(0);
          cli.handleLine("/describe data." + name);
          assertEquals("", errors.toString(), name);
          assertTrue(output.toString().contains("Files: 1, Row groups: 1, rows: 4"), output.toString());
          assertEquals(1, output.toString().lines().filter(line -> line.startsWith("id ")).count());
        }
        if (layout.equals("hf")) {
          cli.handleLine("/describe data.train-a");
          assertTrue(errors.toString().contains("Incompatible shard schema"));
        }
      }
    }
  }

  @Test
  void describeAndSqlHandleQualifiedDotsAndUnqualifiedHyphens() throws Exception {
    for (String name : List.of("train.v2", "train-extra", "Train-X")) {
      Files.copy(base.resolve("single.parquet"), base.resolve(name + "-0-of-1.parquet"));
    }
    Files.copy(base.resolve("single.parquet"), base.resolve("data/single.parquet"));
    for (boolean sensitive : List.of(false, true)) {
      try (var conn = DriverManager
          .getConnection("jdbc:jparq:" + base + "?datasetLayout=hf&caseSensitive=" + sensitive)) {
        for (String from : List.of("PUBLIC.\"train.v2\"", "\"train-extra\"", "\"train-extra\" t",
            sensitive ? "\"PUBLIC\".\"train.v2\"" : "\"public\".\"train.v2\"",
            sensitive ? "\"Train-X\"" : "\"train-x\"")) {
          assertEquals(4, rows(conn, "SELECT id FROM " + from).size());
        }
        String wrong = sensitive ? "\"train-x\"" : "\"Train-X\"";
        assertThrows(SQLException.class, () -> rows(conn, "SELECT id FROM " + wrong));
        assertEquals(4,
            rows(conn, "SELECT a.id FROM PUBLIC.\"train.v2\" a JOIN \"train-extra\" b ON a.id = b.id").size());
        assertEquals(4, rows(conn, "SELECT id FROM single WHERE id IN (SELECT id FROM PUBLIC.\"train.v2\")").size());
      }
      StringWriter output = new StringWriter();
      StringWriter errors = new StringWriter();
      try (var cli = new JParqCliSession(new PrintWriter(output, true), new PrintWriter(errors, true))) {
        cli.connectDirectory(base + "?datasetLayout=hf&caseSensitive=" + sensitive);
        for (String name : List.of("PUBLIC.train.v2", "train-extra", "single")) {
          output.getBuffer().setLength(0);
          cli.handleLine("/describe " + name);
          assertEquals("", errors.toString());
          assertTrue(output.toString().contains("Files: 1, Row groups: 1, rows: 4"));
        }
      }
    }
  }

  @Test
  void describeAggregatesFooterCountsAndPartialIndexes() throws Exception {
    Path root = Path.of(getClass().getResource("/hf-layout").toURI());
    Files.createDirectories(base.resolve("indexed"));
    Files.copy(root.resolve("data/train-0-of-2-a0.parquet"), base.resolve("indexed/0.parquet"));
    Files.copy(root.resolve("plain.parquet"), base.resolve("indexed/1.parquet"));
    StringWriter output = new StringWriter();
    StringWriter errors = new StringWriter();
    try (var cli = new JParqCliSession(new PrintWriter(output, true), new PrintWriter(errors, true))) {
      cli.connectDirectory(base + "?datasetLayout=hf");
      cli.handleLine("/describe indexed");
      assertEquals("", errors.toString());
      assertTrue(output.toString().contains("Files: 2, Row groups: 3, rows: 8194"));
      assertTrue(output.toString().contains("Page index: partial"));
    }
    assertEquals("absent", new ParquetFileInfo(List.of()).pageIndexStatus());
    assertEquals("partial",
        new ParquetFileInfo(List.of(new ParquetFileInfo.RowGroup(1, false, false, true))).pageIndexStatus());
  }

  @Test
  void logicalStructureIgnoresNamesButDetectsEverySemanticDifference() {
    Schema first = schema("One");
    Schema second = schema("Two");
    assertTrue(LogicalSchemas.compatible(first, second));
    String json = first.toString();
    for (String changed : List.of(json.replace("\"int\"", "\"long\""), json.replace("\"label\"", "\"other\""),
        json.replace("[\"null\",\"int\"]", "\"int\""))) {
      // Remove a now-incompatible default when changing nullability.
      assertFalse(
          LogicalSchemas.compatible(first, new Schema.Parser().parse(changed.replace(",\"default\":null", ""))));
    }
    Schema forward = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL));
    Schema backward = Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT));
    assertTrue(LogicalSchemas.compatible(forward, backward));
    assertFalse(LogicalSchemas.compatible(Schema.createArray(forward), Schema.createMap(forward)));
    Schema decimal = org.apache.avro.LogicalTypes.decimal(6, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema otherDecimal = org.apache.avro.LogicalTypes.decimal(6, 3).addToSchema(Schema.create(Schema.Type.BYTES));
    assertFalse(LogicalSchemas.compatible(decimal, otherDecimal));
  }

  @Test
  void indexedShardsPruneIndependentlyAndRetainEnrichedLogicalTypes() throws Exception {
    Path root = Path.of(getClass().getResource("/hf-layout").toURI());
    for (int index = 0; index < 2; index++) {
      Path file = root.resolve("data/train-" + index + "-of-2-a" + index + ".parquet");
      String condition = index == 0 ? "id BETWEEN 100 AND 110" : "id BETWEEN 9000 AND 9010";
      long retained = ParquetPruning.filteredCount(new org.apache.hadoop.fs.Path(file.toUri()), condition);
      assertTrue(retained >= 11 && retained < 4096, "shard " + index + ": " + retained);
      String outside = index == 0 ? "id > 8192" : "id <= 8192";
      assertEquals(0, ParquetPruning.filteredCount(new org.apache.hadoop.fs.Path(file.toUri()), outside));
    }
    for (boolean projection : List.of(true, false)) {
      try (var conn = new JParqConnection("jdbc:jparq:" + root + "?datasetLayout=hf&projectionPushdown=" + projection,
          null)) {
        assertEquals(11, rows(conn, "SELECT id FROM data.train WHERE id BETWEEN 9000 AND 9010").size());
        var table = ParquetTable.validate(conn.resolveTable("data", "train"), new Configuration(false));
        Configuration conf = new Configuration(false);
        if (projection) {
          Schema projected = AvroProjections.project(table.schema(), Set.of("id", "half", "ts_ns", "image"));
          AvroReadSupport.setRequestedProjection(conf, projected);
          AvroReadSupport.setAvroReadSchema(conf, projected);
        }
        try (var reader = table.open(conf, null)) {
          GenericRecord row;
          int count = 0;
          while ((row = reader.read()) != null) {
            count++;
            assertEquals("float16", se.alipsa.jparq.helper.JdbcTypeMapper
                .nonNullSchema(row.getSchema().getField("half").schema()).getProp("jparq.logicalType"));
            assertEquals("timestamp-nanos", se.alipsa.jparq.helper.JdbcTypeMapper
                .nonNullSchema(row.getSchema().getField("ts_ns").schema()).getProp("logicalType"));
          }
          assertEquals(16384, count);
        }
      }
    }
  }

  @Test
  void metadataValidationScopeIsObservableAndRepeatedAcrossStatements() throws Exception {
    // Static mocking observes the real footer reader without replacing its
    // behavior.
    List<String> reads = new ArrayList<>();
    try (var schemas = org.mockito.Mockito.mockStatic(ParquetSchemas.class, org.mockito.Mockito.CALLS_REAL_METHODS);
        var conn = (JParqConnection) connect(true, true)) {
      schemas.when(
          () -> ParquetSchemas.readAvroSchema(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
          .thenAnswer(invocation -> {
            reads.add(invocation.getArgument(0).toString());
            return invocation.callRealMethod();
          });
      conn.listTables();
      conn.listSchemas();
      conn.resolveTable("data", "train");
      assertTrue(reads.isEmpty());
      try (var tables = conn.getMetaData().getTables(null, null, "%", null)) {
        assertTrue(tables.next());
      }
      assertTrue(reads.isEmpty(), "Table remarks must not invoke full schema validation");
      try (var columns = conn.getMetaData().getColumns(null, "data", "train", "%")) {
        assertTrue(columns.next());
      }
      assertEquals(3, reads.size());
      assertTrue(reads.getLast().contains("train-2-of-3"));
      reads.clear();
      for (int attempt = 0; attempt < 2; attempt++) {
        rows(conn, "SELECT * FROM information_schema.columns WHERE table_name = 'single'");
        assertEquals(4 * (attempt + 1), reads.size());
      }
    }
  }

  @Test
  void symlinkDataReadsFollowTargetsOutsideTheVisibleBase() throws Exception {
    Path outside = Files.createTempDirectory("jparq-layout-target");
    try {
      Path blob = outside.resolve("blob");
      Files.copy(base.resolve("single.parquet"), blob);
      Files.createDirectories(base.resolve("linked"));
      try {
        Files.createSymbolicLink(base.resolve("linked/0.parquet"), blob);
      } catch (UnsupportedOperationException | java.io.IOException exception) {
        org.junit.jupiter.api.Assumptions.abort("Symlinks unavailable: " + exception);
      }
      try (var conn = connect(true, true)) {
        assertEquals(rows(conn, "SELECT id FROM single ORDER BY id"), rows(conn, "SELECT id FROM linked ORDER BY id"));
      }
    } finally {
      try (var paths = Files.list(outside)) {
        for (Path path : paths.toList()) {
          Files.delete(path);
        }
      }
      Files.delete(outside);
    }
  }

  @Test
  void directoryTablesAndCommittedRecordNameFixturesUseTheSameAccessPath() throws Exception {
    Files.createDirectories(base.resolve("default/train"));
    Files.copy(base.resolve("data/train-0-of-3-a.parquet"), base.resolve("default/train/9.parquet"));
    Files.copy(base.resolve("data/train-2-of-3-c.parquet"), base.resolve("default/train/10.parquet"));
    try (var conn = connect(true, true)) {
      assertEquals(rows(conn, "SELECT id, nested FROM single ORDER BY id"),
          rows(conn, "SELECT id, nested FROM default.train ORDER BY id"));
      try (var columns = conn.getMetaData().getColumns(null, "default", "train", "%")) {
        assertTrue(columns.next());
        assertEquals("id", columns.getString("COLUMN_NAME"));
        assertEquals("First", columns.getString("REMARKS"));
      }
      try (var tables = conn.getMetaData().getTables(null, "default", "train", null)) {
        assertTrue(tables.next());
        assertEquals("First", tables.getString("REMARKS"));
        assertFalse(tables.next());
      }
    }
    Path root = Path.of(getClass().getResource("/hf-layout").toURI());
    for (boolean projection : List.of(true, false)) {
      try (var conn = new JParqConnection("jdbc:jparq:" + root + "?datasetLayout=hf&projectionPushdown=" + projection,
          null)) {
        var location = conn.resolveTable("recordnames", "named");
        var table = ParquetTable.validate(location, new Configuration(false));
        assertEquals("First", table.schema().getName());
        assertEquals("Last", table.shards().getLast().schema().getName());
        assertEquals(2, rows(conn, "SELECT id, nested FROM recordnames.named").size());
        assertEquals(2,
            rows(conn, "SELECT a.nested FROM recordnames.named a JOIN recordnames.named b ON a.id = b.id").size());
        assertEquals(16384, rows(conn, "SELECT id FROM default.train").size());
      }
    }
  }

  @Test
  void incompatibilityIsDetectedBeforeOpeningRowsAndNewValidationsObserveTargetChanges() throws Exception {
    try (var conn = (JParqConnection) connect(true, true)) {
      var location = conn.resolveTable("data", "train");
      var reference = location.file();
      Path last = location.files().getLast().toPath();
      Files.delete(last);
      Schema wrong = new Schema.Parser().parse(schema("Changed").toString().replace("\"label\"", "\"other\""));
      // An empty incompatible shard must fail too; row presence cannot determine
      // metadata.
      write(last, wrong);
      SQLException failure = assertThrows(SQLException.class,
          () -> ParquetTable.validate(location, new Configuration(false)));
      assertTrue(failure.getMessage().contains(reference.toString()));
      assertTrue(failure.getMessage().contains(last.toString()));
      Files.delete(last);
      write(last, schema("Repaired"), 3, 4);
      assertEquals(4, rows(conn, "SELECT id FROM data.train").size());
    }
  }

  @Test
  void cancelClosesActiveLogicalResultsAndReaderCloseDoesNotAdvance() throws Exception {
    try (var conn = connect(true, true); var stmt = conn.createStatement()) {
      var rs = stmt.executeQuery("SELECT id FROM data.train");
      assertTrue(rs.next());
      stmt.cancel();
      assertTrue(rs.isClosed());
      try (var prepared = conn.prepareStatement("SELECT id FROM data.train")) {
        var result = prepared.executeQuery();
        assertTrue(result.next());
        prepared.cancel();
        assertTrue(result.isClosed());
      }
      var table = ParquetTable.validate(((JParqConnection) conn).resolveTable("data", "train"),
          new Configuration(false));
      var reader = table.open(new Configuration(false), null);
      reader.close();
      assertNull(reader.read());
      try (var active = table.open(new Configuration(false), null)) {
        assertNotNull(active.read());
        Files.delete(table.shards().getLast().file().toPath());
        assertNotNull(active.read());
        assertThrows(java.io.IOException.class, active::read);
        assertNull(active.read());
      }
    }
  }
}
