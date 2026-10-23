package se.alipsa.jparq.engine;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetReader;
import se.alipsa.jparq.JParqConnection.TableLocation;

/** Shared logical-table validation and sequential per-shard reader factory. */
public final class ParquetTable {
  private final List<Shard> shards;

  /**
   * Footer schema reader, injectable for observing validation without reflection.
   */
  @FunctionalInterface
  public interface SchemaReader {
    /**
     * Reads one enriched full schema.
     * 
     * @param path
     *          file path
     * @param configuration
     *          per-file configuration
     * @return enriched schema
     * @throws IOException
     *           if the footer cannot be read
     */
    Schema read(Path path, Configuration configuration) throws IOException;
  }

  /**
   * A physical file and its own enriched full schema.
   * 
   * @param file
   *          physical path
   * @param schema
   *          enriched full schema
   */
  public record Shard(File file, Schema schema) {
  }

  private ParquetTable(List<Shard> shards) {
    this.shards = List.copyOf(shards);
  }

  /**
   * Validates every shard before exposing metadata or opening any data reader.
   * 
   * @param location
   *          logical table
   * @param configuration
   *          input configuration
   * @return validated table
   * @throws SQLException
   *           for unreadable or incompatible shards
   */
  public static ParquetTable validate(TableLocation location, Configuration configuration) throws SQLException {
    return validate(location, configuration, ParquetSchemas::readAvroSchema);
  }

  /**
   * Validates using an injectable footer reader. No cache is retained between
   * calls.
   * 
   * @param location
   *          logical table
   * @param configuration
   *          input configuration
   * @param schemaReader
   *          footer reader
   * @return validated table
   * @throws SQLException
   *           for unreadable or incompatible shards
   */
  public static ParquetTable validate(TableLocation location, Configuration configuration, SchemaReader schemaReader)
      throws SQLException {
    List<Shard> shards = new ArrayList<>();
    for (File file : location.files()) {
      try {
        Schema schema = schemaReader.read(new Path(file.toURI()), HadoopConfigurations.copy(configuration));
        if (!shards.isEmpty() && !LogicalSchemas.compatible(shards.getFirst().schema(), schema)) {
          String detail = LogicalSchemas.firstDifference(shards.getFirst().schema(), schema)
              .orElse("logical structure differs");
          throw new SQLException("Incompatible shard schema for " + location.schemaName() + "." + location.tableName()
              + ": reference " + shards.getFirst().file() + "; offending " + file + "; " + detail);
        }
        shards.add(new Shard(file, schema));
      } catch (IOException exception) {
        throw new SQLException("Cannot read shard " + file + " for " + location.tableName(), exception);
      }
    }
    return new ParquetTable(shards);
  }

  /**
   * Returns immutable shard descriptions in numeric scan order.
   * 
   * @return files with their individual enriched schemas
   */
  public List<Shard> shards() {
    return shards;
  }

  /**
   * Returns the representative first-file schema for planning and metadata.
   * 
   * @return full first-file schema
   */
  public Schema schema() {
    return shards.getFirst().schema();
  }

  /**
   * Builds a private reader configuration using this shard's record names.
   * 
   * @param shard
   *          physical input
   * @param input
   *          shared input configuration, which remains unchanged
   * @return per-file configuration with per-file projection and read schemas
   */
  public static Configuration configurationFor(Shard shard, Configuration input) {
    Configuration result = HadoopConfigurations.copy(input);
    result.setBoolean(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, false);
    String requested = input.get(AvroReadSupport.AVRO_REQUESTED_PROJECTION);
    result.unset(AvroReadSupport.AVRO_REQUESTED_PROJECTION);
    // parquet-avro keeps this configuration key private.
    result.unset("parquet.avro.read.schema");
    if (requested != null) {
      Schema projection = new Schema.Parser().parse(requested);
      Set<String> fields = projection.getFields().stream().map(Schema.Field::name).collect(Collectors.toSet());
      Schema ownProjection = AvroProjections.project(shard.schema(), fields);
      AvroReadSupport.setRequestedProjection(result, ownProjection);
      AvroReadSupport.setAvroReadSchema(result, ownProjection);
    }
    return result;
  }

  /**
   * Opens a lazy sequential reader. It owns at most one physical reader at a
   * time.
   * 
   * @param configuration
   *          per-input configuration
   * @param predicate
   *          optional per-input Parquet predicate, or null
   * @return reader owning all subsequently opened physical readers
   */
  public RecordReader open(Configuration configuration, FilterPredicate predicate) {
    return open(configuration, predicate, ParquetTable::openShard);
  }

  /**
   * Opens a sequential reader using an injectable physical reader factory.
   *
   * @param configuration
   *          per-input configuration
   * @param predicate
   *          optional Parquet predicate, or null
   * @param factory
   *          physical reader factory
   * @return reader owning all subsequently opened physical readers
   */
  public RecordReader open(Configuration configuration, FilterPredicate predicate, ShardReaderFactory factory) {
    return new SequentialReader(shards, HadoopConfigurations.copy(configuration), predicate, factory);
  }

  /**
   * Opens one physical shard reader with its own enriched schema.
   *
   * @param shard
   *          physical input and its own schema
   * @param configuration
   *          input reader configuration
   * @param predicate
   *          optional Parquet predicate, or null
   * @return adapter owning the physical reader
   * @throws IOException
   *           if the shard cannot be opened
   */
  private static RecordReader openShard(Shard shard, Configuration configuration, FilterPredicate predicate)
      throws IOException {
    var builder = ParquetReader.<GenericRecord>builder(new PrunedAvroReadSupport<>(), new Path(shard.file().toURI()))
        .withConf(configurationFor(shard, configuration));
    if (predicate != null) {
      builder.withFilter(FilterCompat.get(predicate));
    }
    return new ParquetRecordReaderAdapter(builder.build(), shard.schema());
  }

  /** Opens physical shard readers, injectable for observing reader ownership. */
  @FunctionalInterface
  public interface ShardReaderFactory {
    /**
     * Opens a reader for one validated shard.
     *
     * @param shard
     *          validated physical input and its own full schema
     * @param configuration
     *          input reader configuration
     * @param predicate
     *          optional Parquet predicate, or null
     * @return physical reader owned by the sequential reader
     * @throws IOException
     *           if the shard cannot be opened
     */
    RecordReader open(Shard shard, Configuration configuration, FilterPredicate predicate) throws IOException;
  }

  /**
   * Opens each adapter with that shard's own full enriched schema. Closing
   * publishes a request without waiting for an active decoder; that read closes
   * its input when it returns, before opening another shard. A pushed-down record
   * filter can make one physical read span the rest of a shard.
   */
  private static final class SequentialReader implements RecordReader {
    private final List<Shard> shards;
    private final Configuration configuration;
    private final FilterPredicate predicate;
    private final ShardReaderFactory factory;
    private final ReentrantLock readLock = new ReentrantLock();
    private volatile boolean cancelRequested;
    private int nextShard;
    private RecordReader active;
    private boolean closed;

    private SequentialReader(List<Shard> shards, Configuration configuration, FilterPredicate predicate,
        ShardReaderFactory factory) {
      this.shards = shards;
      this.configuration = configuration;
      this.predicate = predicate;
      this.factory = factory;
    }

    @Override
    public GenericRecord read() throws IOException {
      readLock.lock();
      try {
        while (!closed && !cancelRequested) {
          if (active == null) {
            if (nextShard == shards.size()) {
              close();
              return null;
            }
            Shard shard = shards.get(nextShard++);
            active = factory.open(shard, configuration, predicate);
          }
          if (cancelRequested) {
            close();
            return null;
          }
          GenericRecord row = active.read();
          if (cancelRequested) {
            close();
            return null;
          }
          if (row != null) {
            return row;
          }
          active.close();
          active = null;
        }
        close();
        return null;
      } catch (IOException | RuntimeException exception) {
        try {
          close();
        } catch (IOException closeException) {
          exception.addSuppressed(closeException);
        }
        throw exception;
      } finally {
        readLock.unlock();
        // A request can arrive after the final check but before unlock. Retrying
        // after releasing the lock prevents that race from leaving input open.
        if (cancelRequested) {
          close();
        }
      }
    }

    @Override
    public void close() throws IOException {
      cancelRequested = true;
      if (!readLock.tryLock()) {
        return;
      }
      try {
        closed = true;
        if (active != null) {
          try {
            active.close();
          } finally {
            active = null;
          }
        }
      } finally {
        readLock.unlock();
      }
    }
  }
}
