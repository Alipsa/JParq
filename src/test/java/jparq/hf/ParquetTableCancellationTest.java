package jparq.hf;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.JParqConnection.TableLocation;
import se.alipsa.jparq.engine.ParquetTable;
import se.alipsa.jparq.engine.RecordReader;

/**
 * Verifies cancellation while a physical read holds the sequential reader
 * monitor.
 */
class ParquetTableCancellationTest {
  @ParameterizedTest
  @ValueSource(booleans = {
      true, false
  })
  void cancellationStopsAnActiveScanAtTheNextPhysicalBoundary(boolean returnsRow) throws Exception {
    Schema schema = new Schema.Parser().parse("""
        {"type":"record","name":"Row","fields":[{"name":"id","type":"int"}]}
        """);
    var location = new TableLocation("PUBLIC", "train", List.of(new File("first.parquet"), new File("next.parquet")));
    var configuration = new Configuration(false);
    var table = ParquetTable.validate(location, configuration, (path, conf) -> schema);
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var decoding = new AtomicBoolean();
    var opens = new AtomicInteger();
    var closes = new AtomicInteger();
    var predicate = FilterApi.eq(FilterApi.intColumn("id"), 999);
    try (var reader = table.open(configuration, predicate, (shard, conf, filter) -> {
      assertSame(predicate, filter);
      assertEquals(1, opens.incrementAndGet(), "Cancellation must prevent opening later shards");
      return new RecordReader() {
        @Override
        public GenericRecord read() throws IOException {
          decoding.set(true);
          started.countDown();
          try {
            if (!release.await(10, TimeUnit.SECONDS)) {
              throw new IOException("Timed out waiting for the simulated physical read");
            }
            return returnsRow ? new GenericData.Record(schema) : null;
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
    })) {
      var reading = new FutureTask<>(reader::read);
      var closing = new FutureTask<Void>(() -> {
        reader.close();
        return null;
      });
      Thread.ofPlatform().start(reading);
      try {
        assertTrue(started.await(5, TimeUnit.SECONDS));
        Thread cancellation = Thread.ofPlatform().start(closing);
        // BLOCKED proves close() reached the monitor while read() still owns it.
        // Its cancellation flag must already be visible before decoding resumes.
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
          while (cancellation.isAlive() && cancellation.getState() != Thread.State.BLOCKED) {
            Thread.sleep(1);
          }
          assertEquals(Thread.State.BLOCKED, cancellation.getState());
        });
        assertEquals(0, closes.get());
      } finally {
        release.countDown();
      }
      assertNull(reading.get(5, TimeUnit.SECONDS), "Do not expose a row read after cancellation was requested");
      closing.get(5, TimeUnit.SECONDS);
      assertEquals(1, opens.get());
      assertEquals(1, closes.get());
      assertNull(reader.read());
    }
  }
}
