package jparq.hf;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqConnection.TableLocation;

/**
 * Exercises footer-free discovery, strict group validation, and compatibility.
 */
class HfDiscoveryTest {
  @TempDir
  Path base;

  private Path touch(String relative) throws Exception {
    Path file = base.resolve(relative);
    Files.createDirectories(file.getParent());
    return Files.createFile(file);
  }

  private JParqConnection connect(String options) throws SQLException {
    return new JParqConnection("jdbc:jparq:" + base + "?" + options, new Properties());
  }

  @Test
  void validatesPropertyAndPreservesDefaultDiscovery() throws Exception {
    touch("data/train-0-of-1.parquet");
    touch("default/train/0000.parquet");
    for (String option : List.of("", "datasetLayout=none", "datasetLayout=None")) {
      try (var conn = connect(option)) {
        assertEquals(List.of("train-0-of-1"), conn.listTables().stream().map(TableLocation::tableName).toList());
      }
    }
    for (String option : List.of("datasetLayout=hf", "datasetLayout=HF")) {
      try (var conn = connect(option)) {
        assertEquals(2, conn.listTables().size());
        assertEquals("train", conn.resolveTable("data", "train").tableName());
        assertEquals("train", conn.resolveTable("default", "train").tableName());
      }
    }
    for (String value : List.of("", "other", " hf ")) {
      SQLException failure = assertThrows(SQLException.class, () -> connect("datasetLayout=" + value));
      assertTrue(failure.getMessage().contains("none, hf"));
    }
  }

  @Test
  void classifiesDirectoriesAtEachSupportedBase() throws Exception {
    touch("default/train/0000.parquet");
    touch("default/train/0010.parquet");
    touch("default/train/9.parquet");
    touch("default/train/README.md");
    touch("default/train/.gitattributes");
    touch("default/train/.ipynb_checkpoints/hidden");
    touch(".cache/huggingface/ignored.parquet");
    touch("data/config/train-0-of-1.parquet");
    try (var conn = connect("datasetLayout=hf")) {
      assertEquals(List.of("PUBLIC", "data", "default"), conn.listSchemas());
      assertEquals(1, conn.listTables().size());
      assertEquals(List.of("0000.parquet", "9.parquet", "0010.parquet"),
          conn.resolveTable("default", "train").files().stream().map(File::getName).toList());
    }
    try (var conn = new JParqConnection("jdbc:jparq:" + base.resolve("default") + "?datasetLayout=hf", null)) {
      assertEquals(List.of("PUBLIC"), conn.listSchemas());
      assertEquals("train", conn.resolveTable(null, "train").tableName());
    }
    try (var conn = new JParqConnection("jdbc:jparq:" + base.resolve("data") + "?datasetLayout=hf", null)) {
      assertEquals("train", conn.resolveTable("config", "train").tableName());
    }
    try (var conn = new JParqConnection("jdbc:jparq:" + base.resolve("default/train") + "?datasetLayout=hf", null)) {
      assertEquals("0000", conn.resolveTable(null, "0000").tableName());
    }
  }

  @Test
  void visibleChildrenAndNonnumericFilesDisqualifyDirectoryTables() throws Exception {
    touch("train/0.parquet");
    touch("train/partial-train/README");
    touch("test/0.parquet");
    touch("test/extra.parquet");
    Files.createDirectory(base.resolve("empty"));
    try (var conn = connect("datasetLayout=hf")) {
      assertEquals(List.of("PUBLIC", "empty", "test", "train"), conn.listSchemas());
      assertEquals(3, conn.listTables().size());
      assertEquals("0", conn.resolveTable("train", "0").tableName());
    }
  }

  @Test
  void numericOrderAndHashesDoNotDependOnPadding() throws Exception {
    for (int index = 10; index >= 0; index--) {
      touch("data/train.v2-extra-" + index + "-of-11-a" + index + ".PARQUET");
    }
    touch("data/non-matching-0-of-1-xyz.parquet");
    touch(".hidden-0-of-2.parquet");
    try (var conn = connect("datasetLayout=hf")) {
      var files = conn.resolveTable("data", "train.v2-extra").files();
      assertEquals(11, files.size());
      assertTrue(files.get(9).getName().contains("-9-of-"));
      assertTrue(files.get(10).getName().contains("-10-of-"));
      assertEquals(2, conn.listTables().size());
    }
  }

  @Test
  void invalidGroupsFailWholeCatalogAndRecoverWithoutReconnection() throws Exception {
    touch("healthy.parquet");
    Path first = touch("data/train-0-of-2.parquet");
    try (var conn = connect("datasetLayout=hf")) {
      assertThrows(SQLException.class, conn::listTables);
      assertThrows(SQLException.class, conn::listSchemas);
      assertThrows(SQLException.class, () -> conn.resolveTable(null, "healthy"));
      assertThrows(SQLException.class, () -> conn.getMetaData().getTables(null, null, "healthy", null));
      Path second = touch("data/train-1-of-2.parquet");
      assertEquals(2, conn.listTables().size());
      Files.delete(second);
      Files.delete(first);
      assertEquals(1, conn.listTables().size());
    }
  }

  @Test
  void rejectsInvalidIndicesTotalsAndDuplicatePadding() throws Exception {
    for (List<String> names : List.of(List.of("t-0-of-0.parquet"), List.of("t-2-of-2.parquet"),
        List.of("t-0-of-2.parquet", "t-1-of-3.parquet"), List.of("t-0-of-1-a.parquet", "t-00-of-01-b.parquet"),
        List.of("t/9.parquet", "t/09.parquet"))) {
      for (String name : names) {
        touch(name);
      }
      try (var conn = connect("datasetLayout=hf")) {
        SQLException failure = assertThrows(SQLException.class, conn::listTables);
        assertTrue(failure.getMessage().contains(base.toString()));
      }
      for (String name : names) {
        Files.delete(base.resolve(name));
      }
    }
  }

  @Test
  void collisionsHaveNoPrecedence() throws Exception {
    for (List<String> sources : List.of(List.of("train.parquet", "train-0-of-1.parquet"),
        List.of("train-0-of-1.parquet", "train/0.parquet"), List.of("data/train.parquet", "data/train/0.parquet"),
        List.of("train.parquet", "public/train.parquet"))) {
      for (String name : sources) {
        touch(name);
      }
      try (var conn = connect("datasetLayout=hf")) {
        SQLException failure = assertThrows(SQLException.class, conn::listTables);
        assertTrue(failure.getMessage().contains("collision"));
      }
      for (String name : sources) {
        Files.delete(base.resolve(name));
      }
    }
  }

  @Test
  void rejectsCaseCollisionsEvenForEmptySchemaDirectories() throws Exception {
    Files.createDirectory(base.resolve("Data"));
    if (Files.exists(base.resolve("data"))) {
      org.junit.jupiter.api.Assumptions.abort("Filesystem is case-insensitive");
    }
    Files.createDirectory(base.resolve("data"));
    touch("Data/README");
    try (var conn = connect("datasetLayout=hf")) {
      assertThrows(SQLException.class, conn::listSchemas);
    }
    try (var conn = connect("datasetLayout=hf&caseSensitive=true")) {
      assertEquals(List.of("PUBLIC", "Data", "data"), conn.listSchemas());
    }
    touch("train.parquet");
    touch("Train.parquet");
    Files.delete(base.resolve("data"));
    try (var conn = connect("datasetLayout=hf")) {
      assertThrows(SQLException.class, conn::listTables);
    }
    try (var conn = connect("datasetLayout=hf&caseSensitive=true")) {
      assertEquals(2, conn.listTables().size());
    }
  }

  @Test
  void followsVisibleLinksAndIgnoresHiddenAndNonParquetDanglingLinks() throws Exception {
    Path blob = touch(".blobs/content");
    Files.createDirectories(base.resolve("default/train"));
    try {
      Files.createSymbolicLink(base.resolve("default/train/0000.parquet"), blob);
    } catch (UnsupportedOperationException | java.io.IOException exception) {
      org.junit.jupiter.api.Assumptions.abort("Symlinks unavailable: " + exception);
    }
    Files.createSymbolicLink(base.resolve("README.md"), base.resolve("missing"));
    Files.createSymbolicLink(base.resolve(".hidden.parquet"), base.resolve("missing"));
    try (var conn = connect("datasetLayout=hf")) {
      assertEquals(1, conn.resolveTable("default", "train").files().size());
      Path broken = Files.createSymbolicLink(base.resolve("broken.parquet"), base.resolve("missing"));
      assertThrows(SQLException.class, () -> conn.resolveTable("default", "train"));
      Files.delete(broken);
      assertEquals(1, conn.listTables().size());
    }
    Files.createSymbolicLink(base.resolve("linked"), base.resolve("default"));
    try (var conn = connect("datasetLayout=hf")) {
      assertEquals("train", conn.resolveTable("linked", "train").tableName());
    }
  }

  @Test
  void tableLocationsRetainSingleFileCompatibilityAndImmutableLists() {
    File file = base.resolve("first.parquet").toFile();
    var location = new TableLocation("PUBLIC", "t", file);
    assertEquals(file, location.file());
    assertEquals(List.of(file), location.files());
    assertThrows(UnsupportedOperationException.class, () -> location.files().clear());
    assertThrows(IllegalArgumentException.class, () -> new TableLocation("PUBLIC", "t", List.of()));
    assertThrows(NullPointerException.class, () -> new TableLocation("PUBLIC", "t", (File) null));
  }
}
