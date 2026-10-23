package se.alipsa.jparq.engine;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * Avro read support that derives the requested Parquet projection by pruning
 * the file schema instead of converting the Avro projection back to Parquet.
 *
 * <p>
 * The standard {@link AvroReadSupport} converts the requested Avro projection
 * to a Parquet schema using a fixed list layout (the legacy 2-level layout by
 * default). When the file uses another layout, such as the standard 3-level
 * LIST structure written by PyArrow, Spark, and parquet-avro with
 * {@code parquet.avro.write-old-list-structure=false}, the converted projection
 * no longer matches the file schema and the affected columns silently read as
 * null. Pruning the file schema keeps whatever layout each file uses, so both
 * legacy and modern list files project correctly.
 *
 * @param <T>
 *          the type of the materialized record
 */
public class PrunedAvroReadSupport<T> extends AvroReadSupport<T> {

  /**
   * Configuration key of the Avro read schema. {@code AvroReadSupport} keeps this
   * key private; the value must stay in sync with parquet-avro.
   */
  static final String AVRO_READ_SCHEMA_CONFIG = "parquet.avro.read.schema";

  // AvroReadSupport keeps this metadata key private; it must stay in sync with
  // parquet-avro
  private static final String AVRO_READ_SCHEMA_METADATA = "avro.read.schema";

  /**
   * Builds the read context, replacing the requested projection with a pruned
   * file schema when a projection is configured.
   *
   * @param configuration
   *          the read configuration
   * @param keyValueMetaData
   *          the file metadata
   * @param fileSchema
   *          the schema of the file
   * @return the read context with a file-layout-preserving projection
   */
  @Override
  public ReadContext init(ParquetConfiguration configuration, Map<String, String> keyValueMetaData,
      MessageType fileSchema) {
    String requestedProjectionString = configuration.get(AVRO_REQUESTED_PROJECTION);
    if (requestedProjectionString == null) {
      return super.init(configuration, keyValueMetaData, fileSchema);
    }
    Set<String> fields = new LinkedHashSet<>();
    for (Schema.Field field : new Schema.Parser().parse(requestedProjectionString).getFields()) {
      fields.add(field.name());
    }
    Map<String, String> metadata = new LinkedHashMap<>();
    String avroReadSchema = configuration.get(AVRO_READ_SCHEMA_CONFIG);
    if (avroReadSchema != null) {
      metadata.put(AVRO_READ_SCHEMA_METADATA, avroReadSchema);
    }
    if (configuration.getBoolean(AVRO_COMPATIBILITY, AVRO_DEFAULT_COMPATIBILITY)) {
      metadata.put(AVRO_COMPATIBILITY, "true");
    }
    return new ReadContext(prune(fileSchema, fields), metadata);
  }

  /**
   * Retains the top-level file fields selected by the projection, preserving
   * their physical layout.
   *
   * @param fileSchema
   *          the full file schema
   * @param names
   *          the projected field names
   * @return the pruned schema, or the full schema when nothing or everything
   *         matches
   */
  private static MessageType prune(MessageType fileSchema, Set<String> names) {
    List<Type> kept = fileSchema.getFields().stream().filter(field -> names.contains(field.getName()))
        .collect(Collectors.toList());
    // An empty match cannot occur under the current caller: the projection is
    // derived from the shard's own Avro schema and AvroProjections.project
    // already falls back to the full schema. The fallback mirrors that behavior,
    // reading all columns instead of none.
    if (kept.isEmpty() || kept.size() == fileSchema.getFieldCount()) {
      return fileSchema;
    }
    return new MessageType(fileSchema.getName(), kept);
  }
}
