package se.alipsa.jparq.engine;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
        Schema schema = schemaReader.read(new Path(file.toURI()), new Configuration(configuration));
        if (!shards.isEmpty()) {
          var difference = LogicalSchemas.firstDifference(shards.getFirst().schema(), schema);
          if (difference.isPresent()) {
            throw new SQLException("Incompatible shard schema for " + location.schemaName() + "." + location.tableName()
                + ": reference " + shards.getFirst().file() + "; offending " + file + "; " + difference.get());
          }
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
    Configuration result = new Configuration(input);
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
    return new SequentialReader(shards, new Configuration(configuration), predicate);
  }

  /**
   * Opens each adapter with that shard's own full enriched schema. Read and close
   * share a monitor so cancellation waits for the current physical read and
   * cannot close a decoder in use or race a shard transition.
   */
  private static final class SequentialReader implements RecordReader {
    private final List<Shard> shards;
    private final Configuration configuration;
    private final FilterPredicate predicate;
    private int nextShard;
    private RecordReader active;
    private boolean closed;

    private SequentialReader(List<Shard> shards, Configuration configuration, FilterPredicate predicate) {
      this.shards = shards;
      this.configuration = configuration;
      this.predicate = predicate;
    }

    @Override
    public synchronized GenericRecord read() throws IOException {
      try {
        while (!closed) {
          if (active == null) {
            if (nextShard == shards.size()) {
              close();
              return null;
            }
            Shard shard = shards.get(nextShard++);
            var builder = ParquetReader.<GenericRecord>builder(new AvroReadSupport<>(), new Path(shard.file().toURI()))
                .withConf(configurationFor(shard, configuration));
            if (predicate != null) {
              builder.withFilter(FilterCompat.get(predicate));
            }
            active = new ParquetRecordReaderAdapter(builder.build(), shard.schema());
          }
          GenericRecord row = active.read();
          if (row != null) {
            return row;
          }
          active.close();
          active = null;
        }
        return null;
      } catch (IOException | RuntimeException exception) {
        try {
          close();
        } catch (IOException closeException) {
          exception.addSuppressed(closeException);
        }
        throw exception;
      }
    }

    @Override
    public synchronized void close() throws IOException {
      closed = true;
      if (active != null) {
        try {
          active.close();
        } finally {
          active = null;
        }
      }
    }
  }
}
