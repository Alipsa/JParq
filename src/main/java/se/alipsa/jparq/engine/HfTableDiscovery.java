package se.alipsa.jparq.engine;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqConnection.TableLocation;

/** Bounded, footer-free discovery of Hugging Face logical tables. */
public final class HfTableDiscovery {
  private static final Pattern SHARD = Pattern
      .compile("^(?<split>[A-Za-z0-9_][A-Za-z0-9_.-]*)-(?<index>[0-9]+)-of-(?<total>[0-9]+)"
          + "(?:-(?<hash>[0-9a-fA-F]+))?\\.(?i:parquet)$");
  private static final Pattern NUMERIC = Pattern.compile("^[0-9]+\\.(?i:parquet)$");
  private final boolean caseSensitive;
  private final Map<List<String>, TableLocation> tables = new LinkedHashMap<>();
  private final Map<String, String> schemas = new LinkedHashMap<>();
  private final Map<String, File> schemaSources = new LinkedHashMap<>();

  private HfTableDiscovery(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
    schemas.put(key(JParqConnection.DEFAULT_SCHEMA), JParqConnection.DEFAULT_SCHEMA);
  }

  /**
   * A complete catalog snapshot without any footer reads.
   * 
   * @param tables
   *          logical tables
   * @param schemas
   *          schema names including PUBLIC
   */
  public record Catalog(List<TableLocation> tables, List<String> schemas) {
    /**
     * Copies the catalog collections.
     * 
     * @param tables
     *          logical tables
     * @param schemas
     *          schema names
     */
    public Catalog {
      tables = List.copyOf(tables);
      schemas = List.copyOf(schemas);
    }
  }

  /**
   * Discovers the whole supported directory boundary before returning any names.
   * 
   * @param base
   *          URL base directory
   * @param caseSensitive
   *          identifier matching mode
   * @return immutable catalog
   * @throws SQLException
   *           for invalid groups, links, collisions, or directory access
   */
  public static Catalog discover(File base, boolean caseSensitive) throws SQLException {
    HfTableDiscovery discovery = new HfTableDiscovery(caseSensitive);
    List<File> entries = entries(base);
    discovery.addFiles(entries, JParqConnection.DEFAULT_SCHEMA);
    for (File child : entries) {
      if (child.isDirectory()) {
        discovery.addRootDirectory(child);
      }
    }
    return new Catalog(new ArrayList<>(discovery.tables.values()), new ArrayList<>(discovery.schemas.values()));
  }

  private void addRootDirectory(File directory) throws SQLException {
    List<File> children = entries(directory);
    List<File> numeric = numericFiles(children);
    if (!numeric.isEmpty()) {
      add(new TableLocation(JParqConnection.DEFAULT_SCHEMA, directory.getName(), numeric));
      return;
    }
    String schema = JParqConnection.DEFAULT_SCHEMA.equalsIgnoreCase(directory.getName())
        ? JParqConnection.DEFAULT_SCHEMA
        : directory.getName();
    File previous = schemaSources.putIfAbsent(key(schema), directory);
    if (previous != null) {
      throw new SQLException("Schema collision for " + schema + ": " + previous + " and " + directory);
    }
    schemas.putIfAbsent(key(schema), schema);
    addFiles(children, schema);
    for (File child : children) {
      if (child.isDirectory()) {
        List<File> nested = numericFiles(entries(child));
        if (!nested.isEmpty()) {
          add(new TableLocation(schema, child.getName(), nested));
        }
      }
    }
  }

  private static List<File> entries(File directory) throws SQLException {
    File[] entries = directory.listFiles(file -> !file.getName().startsWith("."));
    if (entries == null) {
      throw new SQLException("Failed to list directory: " + directory);
    }
    Arrays.sort(entries, Comparator.comparing(File::getName));
    for (File file : entries) {
      if (!file.isDirectory() && isParquet(file) && !file.isFile()) {
        String target = "unavailable";
        try {
          if (Files.isSymbolicLink(file.toPath())) {
            target = Files.readSymbolicLink(file.toPath()).toString();
          }
        } catch (java.io.IOException exception) {
          throw new SQLException("Cannot inspect Parquet link " + file, exception);
        }
        throw new SQLException("Invalid Parquet file/link " + file + " (target: " + target + ")");
      }
    }
    return List.of(entries);
  }

  private static boolean isParquet(File file) {
    return file.getName().toLowerCase(Locale.ROOT).endsWith(".parquet");
  }

  private static List<File> numericFiles(List<File> entries) throws SQLException {
    if (entries.stream().anyMatch(File::isDirectory)) {
      return List.of();
    }
    List<File> parquet = entries.stream().filter(HfTableDiscovery::isParquet).filter(File::isFile).toList();
    if (parquet.isEmpty() || parquet.stream().anyMatch(file -> !NUMERIC.matcher(file.getName()).matches())) {
      return List.of();
    }
    Map<BigInteger, File> ordered = new TreeMap<>();
    for (File file : parquet) {
      BigInteger index = new BigInteger(file.getName().substring(0, file.getName().lastIndexOf('.')));
      File previous = ordered.putIfAbsent(index, file);
      if (previous != null) {
        throw new SQLException("Duplicate numeric shard index " + index + ": " + previous + " and " + file);
      }
    }
    return List.copyOf(ordered.values());
  }

  private void addFiles(List<File> entries, String schema) throws SQLException {
    Map<String, ShardGroup> groups = new LinkedHashMap<>();
    for (File file : entries) {
      if (!file.isFile() || !isParquet(file)) {
        continue;
      }
      var matcher = SHARD.matcher(file.getName());
      if (matcher.matches()) {
        String split = matcher.group("split");
        ShardGroup group = groups.computeIfAbsent(split, ignored -> new ShardGroup(split, file.getParentFile()));
        group.add(file, new BigInteger(matcher.group("index")), new BigInteger(matcher.group("total")));
      } else {
        String name = file.getName();
        add(new TableLocation(schema, name.substring(0, name.lastIndexOf('.')), file));
      }
    }
    for (ShardGroup group : groups.values()) {
      add(new TableLocation(schema, group.split, group.complete()));
    }
  }

  private void add(TableLocation location) throws SQLException {
    List<String> name = List.of(key(location.schemaName()), key(location.tableName()));
    TableLocation previous = tables.putIfAbsent(name, location);
    if (previous != null) {
      throw new SQLException("Table collision for " + name + ": " + previous.files() + " and " + location.files());
    }
  }

  private String key(String name) {
    return Identifier.normalize(name, caseSensitive);
  }

  /**
   * Accumulates numeric indices without allocating space for an untrusted total.
   */
  private static final class ShardGroup {
    private final String split;
    private final File directory;
    private final Map<BigInteger, File> files = new TreeMap<>();
    private BigInteger total;

    private ShardGroup(String split, File directory) {
      this.split = split;
      this.directory = directory;
    }

    private void add(File file, BigInteger index, BigInteger declaredTotal) throws SQLException {
      if (declaredTotal.signum() <= 0 || index.compareTo(declaredTotal) >= 0) {
        throw error("invalid index " + index + " / total " + declaredTotal + " in " + file);
      }
      if (total != null && !total.equals(declaredTotal)) {
        throw error("disagreeing totals " + total + " and " + declaredTotal + " in " + file);
      }
      total = declaredTotal;
      File previous = files.putIfAbsent(index, file);
      if (previous != null) {
        throw error("duplicate index " + index + ": " + previous + " and " + file);
      }
    }

    private List<File> complete() throws SQLException {
      if (!BigInteger.valueOf(files.size()).equals(total)) {
        throw error("expected total " + total + ", present indices " + files.keySet() + "; missing shards");
      }
      return List.copyOf(files.values());
    }

    private SQLException error(String detail) {
      return new SQLException("Invalid shard group " + split + " in " + directory + ": " + detail);
    }
  }
}
