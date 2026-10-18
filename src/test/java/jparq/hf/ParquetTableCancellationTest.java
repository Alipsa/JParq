package jparq.hf;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqConnection.TableLocation;
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.JParqStatement;
import se.alipsa.jparq.engine.ParquetTable;
import se.alipsa.jparq.engine.RecordReader;
import se.alipsa.jparq.engine.SqlParser;

/** Verifies nonblocking cancellation and errors from in-flight JDBC reads. */
class ParquetTableCancellationTest {
  @TempDir
  Path base;

  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void cancellationReturnsBeforeTheActivePhysicalReadFinishes(boolean returnsRow) throws Exception {
    try (var scan = new BlockingScan(false, returnsRow, true)) {
      var reading = new FutureTask<>(scan.reader::read);
      var closing = new FutureTask<Void>(() -> {
        scan.reader.close();
        return null;
      });
      Thread.ofPlatform().start(reading);
      try {
        assertTrue(scan.started.await(5, TimeUnit.SECONDS));
        Thread.ofPlatform().start(closing);
        closing.get(5, TimeUnit.SECONDS);
        assertFalse(reading.isDone(), "Closing must return while the physical read is still blocked");
        assertEquals(0, scan.closes.get(), "The reading thread still owns the decoder");
      } finally {
        scan.release.countDown();
      }
      assertNull(reading.get(5, TimeUnit.SECONDS));
      assertEquals(1, scan.opens.get());
      assertEquals(1, scan.closes.get());
      assertNull(scan.reader.read());
    }
  }

  @ParameterizedTest
  @CsvSource({
      "true,true", "true,false", "false,true", "false,false"
  })
  void cancelMakesAnInFlightNextThrowInsteadOfReportingEndOfData(boolean returnsRow, boolean pushed) throws Exception {
    try (var scan = new BlockingScan(true, returnsRow, pushed);
        var conn = (JParqConnection) DriverManager.getConnection("jdbc:jparq:" + base);
        var statement = new JParqStatement(conn)) {
      String sql = pushed
          ? "SELECT id FROM train WHERE id = 0 OR id = 999"
          : "SELECT id FROM train WHERE id * 2 = -1 OR id = 0";
      var select = SqlParser.parseSelect(sql);
      try (var result = new JParqResultSet(scan.reader, select, "train", "PUBLIC", null, pushed ? null : select.where(),
          List.of("id"), List.of("id"), null)) {
        statement.setCurrentRs(result);
        assertTrue(result.next());
        assertEquals(0, result.getInt(1));
        var reading = new FutureTask<>(result::next);
        var canceling = new FutureTask<Void>(() -> {
          statement.cancel();
          return null;
        });
        Thread.ofPlatform().start(reading);
        try {
          assertTrue(scan.started.await(5, TimeUnit.SECONDS));
          Thread.ofPlatform().start(canceling);
          canceling.get(5, TimeUnit.SECONDS);
          assertTrue(result.isClosed());
          assertFalse(reading.isDone(), "The physical read must still be in progress when cancel returns");
          assertEquals(0, scan.closes.get());
        } finally {
          scan.release.countDown();
        }
        ExecutionException failure = assertThrows(ExecutionException.class, () -> reading.get(5, TimeUnit.SECONDS));
        SQLException canceled = assertInstanceOf(SQLException.class, failure.getCause());
        assertEquals("Statement was canceled", canceled.getMessage());
        assertEquals("Statement was canceled", assertThrows(SQLException.class, result::next).getMessage());
        assertEquals(1, scan.opens.get());
        assertEquals(1, scan.closes.get());
      }
    }
  }

  /**
   * Simulates one physical read paused inside a decoder, optionally after a
   * prefetched row.
   */
  private static final class BlockingScan implements AutoCloseable {
    private final CountDownLatch started = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicBoolean decoding = new AtomicBoolean();
    private final AtomicInteger opens = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();
    private final AtomicInteger reads = new AtomicInteger();
    private final RecordReader reader;

    /**
     * Builds a deterministic physical-read boundary without creating large files.
     *
     * @param prefetch
     *          whether to emit the constructor's first JDBC row immediately
     * @param returnsRow
     *          whether the blocked read returns a matching row or shard EOF
     * @param pushed
     *          whether a Parquet predicate is attached
     * @throws SQLException
     *           if validation fails
     */
    private BlockingScan(boolean prefetch, boolean returnsRow, boolean pushed) throws SQLException {
      Schema schema = new Schema.Parser().parse("""
          {"type":"record","name":"Row","fields":[{"name":"id","type":"int"}]}
          """);
      var location = new TableLocation("PUBLIC", "train", List.of(new File("first.parquet"), new File("next.parquet")));
      var configuration = new Configuration(false);
      var table = ParquetTable.validate(location, configuration, (path, conf) -> schema);
      FilterPredicate predicate = pushed
          ? FilterApi.or(FilterApi.eq(FilterApi.intColumn("id"), 0), FilterApi.eq(FilterApi.intColumn("id"), 999))
          : null;
      reader = table.open(configuration, predicate, (shard, conf, filter) -> {
        assertSame(predicate, filter);
        assertEquals(1, opens.incrementAndGet(), "Cancellation must prevent opening later shards");
        return new RecordReader() {
          @Override
          public GenericRecord read() throws IOException {
            if (prefetch && reads.getAndIncrement() == 0) {
              return row(schema, 0);
            }
            decoding.set(true);
            started.countDown();
            try {
              if (!release.await(10, TimeUnit.SECONDS)) {
                throw new IOException("Timed out waiting for the simulated physical read");
              }
              return returnsRow ? row(schema, pushed ? 999 : 0) : null;
            } catch (InterruptedException exception) {
              Thread.currentThread().interrupt();
              throw new IOException("Physical read interrupted", exception);
            } finally {
              decoding.set(false);
            }
          }

          @Override
          public void close() {
            assertFalse(decoding.get(), "A physical reader must not be closed during decoding");
            closes.incrementAndGet();
          }
        };
      });
    }

    /**
     * Creates a matching row for either the pushed or residual predicate.
     *
     * @param schema
     *          full physical schema
     * @param id
     *          identifier value
     * @return populated physical record
     */
    private static GenericRecord row(Schema schema, int id) {
      var row = new GenericData.Record(schema);
      row.put("id", id);
      return row;
    }

    @Override
    public void close() throws IOException {
      release.countDown();
      reader.close();
    }
  }
}
