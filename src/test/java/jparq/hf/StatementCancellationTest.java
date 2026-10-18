package jparq.hf;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.ParquetTable;
import se.alipsa.jparq.engine.QueryCancellation;
import se.alipsa.jparq.engine.RecordReader;
import se.alipsa.jparq.engine.function.SystemFunctions;

/**
 * Exercises cancellation before executeQuery returns and across thread-owned
 * SQL contexts.
 */
class StatementCancellationTest {
  @TempDir
  Path base;

  /**
   * Writes a small valid input; blocking reader seams replace slow large-file
   * probes.
   */
  @BeforeEach
  void fixture() throws Exception {
    Schema schema = new Schema.Parser().parse("""
        {"type":"record","name":"Input","fields":[{"name":"id","type":"int"}]}
        """);
    var conf = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(base.resolve("train.parquet").toUri()), conf);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(conf).build()) {
      for (int id = 0; id < 2; id++) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        writer.write(row);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT COUNT(*) FROM train WHERE id * 2 <> -1", "SELECT id FROM train ORDER BY id DESC",
      "SELECT a.id FROM train a JOIN train b ON a.id = b.id",
      "WITH x AS (SELECT COUNT(*) AS n FROM train) SELECT n FROM x",
      "SELECT id FROM (SELECT id FROM train ORDER BY id DESC) x", "SELECT id FROM train UNION ALL SELECT id FROM train",
      "SELECT id FROM train TABLESAMPLE SYSTEM (2 ROWS) REPEATABLE (42)"
  })
  void cancelInterruptsStatementExecutionBeforeAResultIsPublished(String sql) throws Exception {
    verifyCancellation(sql, false);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "SELECT COUNT(*) FROM train WHERE id * 2 <> -1", "SELECT id FROM train ORDER BY id DESC",
      "SELECT a.id FROM train a JOIN train b ON a.id = b.id", "SELECT COUNT(*) FROM train WHERE id <> ?",
      "SELECT id FROM train TABLESAMPLE SYSTEM (2 ROWS) REPEATABLE (42)"
  })
  void cancelInterruptsPreparedExecutionAndBoundReplanning(String sql) throws Exception {
    verifyCancellation(sql, true);
  }

  /**
   * Pauses a real execution inside its physical input, cancels, and then verifies
   * reuse.
   *
   * @param sql
   *          materializing query
   * @param prepared
   *          whether to execute a prepared statement
   * @throws Exception
   *           if execution or synchronization fails
   */
  private void verifyCancellation(String sql, boolean prepared) throws Exception {
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var closes = new AtomicInteger();
    try (var plannedTables = blockingTables(started, release, closes);
        var conn = DriverManager.getConnection("jdbc:jparq:" + base);
        Statement statement = prepared ? conn.prepareStatement(sql) : conn.createStatement()) {
      if (statement instanceof PreparedStatement parameterized && sql.contains("?")) {
        parameterized.setInt(1, -1);
      }
      // Keep a previous result open: cancellation must target the active execution.
      try (ResultSet previous = prepared ? null : statement.executeQuery("SELECT id FROM train")) {
        var running = new FutureTask<Boolean>(() -> {
          try (var tables = blockingTables(started, release, closes)) {
            try (var result = execute(statement, sql)) {
              return result.next();
            }
          }
        });
        Thread.ofPlatform().start(running);
        try {
          assertTrue(started.await(10, TimeUnit.SECONDS), "Execution must reach its registered input");
          var canceling = new FutureTask<Void>(() -> {
            statement.cancel();
            return null;
          });
          Thread.ofPlatform().start(canceling);
          canceling.get(5, TimeUnit.SECONDS);
          assertFalse(running.isDone());
          assertEquals(0, closes.get(), "Cancellation must not close a decoder in use");
          if (previous != null) {
            assertFalse(previous.isClosed(), "Do not cancel the previous result instead of current execution");
          }
        } finally {
          release.countDown();
        }
        ExecutionException failure = assertThrows(ExecutionException.class, () -> running.get(10, TimeUnit.SECONDS));
        assertEquals("Statement was canceled", assertInstanceOf(SQLException.class, failure.getCause()).getMessage());
        assertEquals(QueryCancellation.SQL_STATE, ((SQLException) failure.getCause()).getSQLState());
        assertEquals(1, closes.get());
        // A fresh execution must not inherit the canceled state.
        try (var result = execute(statement, prepared ? sql : "SELECT id FROM train ORDER BY id")) {
          assertTrue(result.next());
        }
      }
    }
  }

  /**
   * Installs physical-read seams in whichever thread plans the input schemas.
   *
   * @param started
   *          physical-read notification
   * @param release
   *          decoder release latch
   * @param closes
   *          physical close counter
   * @return thread-local validation observer
   */
  private static org.mockito.MockedStatic<ParquetTable> blockingTables(CountDownLatch started, CountDownLatch release,
      AtomicInteger closes) {
    var tables = mockStatic(ParquetTable.class, CALLS_REAL_METHODS);
    tables.when(() -> ParquetTable.validate(any(), any())).thenAnswer(invocation -> {
      ParquetTable table = (ParquetTable) invocation.callRealMethod();
      ParquetTable observed = spy(table);
      doAnswer(open -> table.open(open.getArgument(0), open.getArgument(1),
          (shard, conf, predicate) -> blockingInput(shard.schema(), started, release, closes))).when(observed)
          .open(any(Configuration.class), nullable(FilterPredicate.class));
      return observed;
    });
    return tables;
  }

  /**
   * Executes either JDBC statement form.
   *
   * @param statement
   *          owning statement
   * @param sql
   *          SQL for ordinary statements
   * @return result
   * @throws SQLException
   *           if execution fails
   */
  private static ResultSet execute(Statement statement, String sql) throws SQLException {
    return statement instanceof PreparedStatement prepared ? prepared.executeQuery() : statement.executeQuery(sql);
  }

  /**
   * Simulates a decoder that returns one row and then pauses until released.
   *
   * @param schema
   *          full physical schema
   * @param started
   *          signals the blocked physical read
   * @param release
   *          releases that read
   * @param closes
   *          observes physical ownership cleanup
   * @return deterministic physical input
   */
  private static RecordReader blockingInput(Schema schema, CountDownLatch started, CountDownLatch release,
      AtomicInteger closes) {
    return new RecordReader() {
      private int reads;

      @Override
      public GenericRecord read() throws IOException {
        if (reads >= 2) {
          return null;
        }
        if (reads++ > 0) {
          started.countDown();
          try {
            if (!release.await(15, TimeUnit.SECONDS)) {
              throw new IOException("Timed out awaiting cancellation");
            }
          } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException(exception);
          }
        }
        var row = new GenericData.Record(schema);
        row.put("id", reads - 1);
        return row;
      }

      @Override
      public void close() {
        closes.incrementAndGet();
      }
    };
  }

  @Test
  void cancellationInsideACorrelatedSubqueryRetainsItsJdbcIdentity() throws Exception {
    Files.copy(base.resolve("train.parquet"), base.resolve("small.parquet"));
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    try (var conn = DriverManager.getConnection("jdbc:jparq:" + base);
        var statement = conn.createStatement();
        var result = statement
            .executeQuery("SELECT b.id FROM train b WHERE EXISTS " + "(SELECT 1 FROM small s2 WHERE s2.id = b.id)")) {
      assertTrue(result.next());
      assertEquals(0, result.getInt(1));
      var running = new FutureTask<Boolean>(() -> {
        try (var tables = mockStatic(ParquetTable.class, CALLS_REAL_METHODS)) {
          tables.when(() -> ParquetTable.validate(any(), any())).thenAnswer(invocation -> {
            ParquetTable table = (ParquetTable) invocation.callRealMethod();
            ParquetTable observed = spy(table);
            doAnswer(open -> table.open(open.getArgument(0), open.getArgument(1),
                (shard, conf, predicate) -> new RecordReader() {
                  @Override
                  public GenericRecord read() throws IOException {
                    started.countDown();
                    try {
                      if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new IOException("Timed out waiting for correlated cancellation");
                      }
                    } catch (InterruptedException exception) {
                      Thread.currentThread().interrupt();
                      throw new IOException(exception);
                    }
                    return null;
                  }

                  @Override
                  public void close() {
                  }
                })).when(observed).open(any(Configuration.class), nullable(FilterPredicate.class));
            return observed;
          });
          return result.next();
        }
      });
      Thread.ofPlatform().start(running);
      try {
        assertTrue(started.await(10, TimeUnit.SECONDS));
        statement.cancel();
      } finally {
        release.countDown();
      }
      ExecutionException failure = assertThrows(ExecutionException.class, () -> running.get(10, TimeUnit.SECONDS));
      SQLException cancellation = assertInstanceOf(SQLException.class, failure.getCause());
      assertEquals("Statement was canceled", cancellation.getMessage());
      assertEquals(QueryCancellation.SQL_STATE, cancellation.getSQLState());
      assertTrue(result.isClosed());
      assertEquals(QueryCancellation.SQL_STATE, assertThrows(SQLException.class, result::next).getSQLState());
    }
  }

  @Test
  void crossThreadCancellationPreservesTheCallersContextAndInvalidatesOnlyTheOwnersHandle() throws Exception {
    var ready = new CountDownLatch(1);
    var resume = new CountDownLatch(1);
    var target = new AtomicReference<Statement>();
    var worker = new FutureTask<Void>(() -> {
      try (var outer = SystemFunctions.openContext("owner-outer", "owner-user");
          var conn = DriverManager.getConnection("jdbc:jparq:" + base);
          var statement = conn.createStatement();
          var result = statement.executeQuery("SELECT DATABASE() AS db, USER() AS u FROM train")) {
        assertTrue(result.next());
        target.set(statement);
        ready.countDown();
        assertTrue(resume.await(10, TimeUnit.SECONDS));
        // No owner-side ResultSet.close() is needed to stop exposing the canceled
        // context.
        assertEquals("owner-outer", SystemFunctions.database());
        assertEquals("owner-user", SystemFunctions.user());
        result.close();
        result.close();
        assertEquals("owner-outer", SystemFunctions.database());
        assertEquals("owner-user", SystemFunctions.user());
      }
      assertEquals("JParq", SystemFunctions.database());
      return null;
    });
    Thread.ofPlatform().start(worker);
    try {
      assertTrue(ready.await(10, TimeUnit.SECONDS));
      Path other = Files.createDirectory(base.resolve("other"));
      Files.copy(base.resolve("train.parquet"), other.resolve("train.parquet"));
      try (var outer = SystemFunctions.openContext("caller-outer", "caller-user");
          Connection conn = DriverManager.getConnection("jdbc:jparq:" + other);
          var statement = conn.createStatement();
          var result = statement.executeQuery("SELECT DATABASE() AS db, USER() AS u FROM train")) {
        assertTrue(result.next());
        target.get().cancel();
        assertEquals(conn.getCatalog(), SystemFunctions.database());
        assertEquals(conn.getCatalog(), result.getString("db"));
        assertEquals(System.getProperty("user.name"), result.getString("u"));
        result.close();
        result.close();
        assertEquals("caller-outer", SystemFunctions.database());
        assertEquals("caller-user", SystemFunctions.user());
      }
    } finally {
      resume.countDown();
    }
    worker.get(10, TimeUnit.SECONDS);
  }
}
