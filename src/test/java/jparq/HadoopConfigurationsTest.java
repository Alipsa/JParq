package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqConnection.TableLocation;
import se.alipsa.jparq.engine.HadoopConfigurations;
import se.alipsa.jparq.engine.ParquetTable;

/**
 * Checks driver-owned configuration copies preserve caller options and
 * ownership.
 */
class HadoopConfigurationsTest {
  @Test
  void createsDriverAwareConfigurationWithoutChangingContextLoader() {
    ClassLoader context = Thread.currentThread().getContextClassLoader();
    Configuration configuration = HadoopConfigurations.create();
    assertSame(HadoopConfigurations.class.getClassLoader(), configuration.getClassLoader());
    assertEquals("org.apache.hadoop.fs.LocalFileSystem", configuration.get("fs.file.impl"));
    assertSame(context, Thread.currentThread().getContextClassLoader());
  }

  @Test
  void tableReadersNormalizeCallerConfigurations() throws Exception {
    Configuration source = new Configuration(false);
    source.setClassLoader(ClassLoader.getPlatformClassLoader());
    source.set("caller.option", "preserved");
    var location = new TableLocation("PUBLIC", "salary", Path.of("src/test/resources/acme/salary.parquet").toFile());
    var table = ParquetTable.validate(location, source);
    var readerConfiguration = ParquetTable.configurationFor(table.shards().getFirst(), source);
    assertEquals("preserved", readerConfiguration.get("caller.option"));
    assertSame(HadoopConfigurations.class.getClassLoader(), readerConfiguration.getClassLoader());
    assertEquals("org.apache.hadoop.fs.LocalFileSystem", readerConfiguration.get("fs.file.impl"));
    try (var reader = table.open(source, null)) {
      org.junit.jupiter.api.Assertions.assertNotNull(reader.read());
    }
    assertSame(ClassLoader.getPlatformClassLoader(), source.getClassLoader());
    assertNull(source.get("fs.file.impl"));
  }

  @Test
  void normalizesPrivateCopyAndPreservesCallerConfiguration() {
    Configuration source = new Configuration(false);
    source.setClassLoader(ClassLoader.getPlatformClassLoader());
    source.set("parquet.avro.projection", "retained");
    Configuration result = HadoopConfigurations.copy(source);
    assertNotSame(source, result);
    assertSame(HadoopConfigurations.class.getClassLoader(), result.getClassLoader());
    assertEquals("org.apache.hadoop.fs.LocalFileSystem", result.get("fs.file.impl"));
    assertEquals("retained", result.get("parquet.avro.projection"));
    assertSame(ClassLoader.getPlatformClassLoader(), source.getClassLoader());
    assertNull(source.get("fs.file.impl"));
  }
}
