package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

/** Utility methods for reading Parquet schemas. */
public final class ParquetSchemas {

  private ParquetSchemas() {
  }

  /**
   * Reads the Avro schema from a Parquet file.
   *
   * @param path
   *          the Parquet file path
   * @param conf
   *          the Hadoop configuration
   * @return the Avro schema
   * @throws IOException
   *           if an I/O error occurs
   */
  public static Schema readAvroSchema(Path path, Configuration conf) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {

      var meta = reader.getFooter().getFileMetaData();
      var kv = meta.getKeyValueMetaData();

      // Try common keys used by parquet-avro writers
      String avroJson = kv.get("parquet.avro.schema");
      if (avroJson == null) {
        avroJson = kv.get("avro.schema");
      }
      if (avroJson != null && !avroJson.isEmpty()) {
        Schema schema = new Schema.Parser().parse(avroJson);
        Set<String> binaryStrings = detectBinaryStringFields(meta.getSchema(), meta.getKeyValueMetaData());
        return normalizeStringTypes(schema, binaryStrings);
      }

      // Fallback: derive Avro schema from the Parquet schema
      MessageType parquetSchema = meta.getSchema();
      Schema schema = new AvroSchemaConverter().convert(parquetSchema);
      Set<String> binaryStrings = detectBinaryStringFields(parquetSchema, meta.getKeyValueMetaData());
      return normalizeStringTypes(schema, binaryStrings);
    }
  }

  /**
   * Normalize the supplied schema so that textual data encoded as binary with
   * UTF-8 semantics surface as {@link Schema.Type#STRING}.
   *
   * @param schema
   *          the schema to normalise (may be {@code null})
   * @return a schema equivalent to {@code schema} where textual columns are
   *         represented using {@link Schema.Type#STRING}
   */
  public static Schema normalizeStringTypes(Schema schema) {
    return normalizeStringTypes(schema, Set.of());
  }

  /**
   * Normalize the supplied schema using the provided hint set.
   *
   * @param schema
   *          the schema to normalise (may be {@code null})
   * @param binaryStringFields
   *          field paths that should be treated as textual values even when the
   *          underlying representation is binary
   * @return a schema equivalent to {@code schema} where textual columns are
   *         represented using {@link Schema.Type#STRING}
   */
  public static Schema normalizeStringTypes(Schema schema, Set<String> binaryStringFields) {
    if (schema == null) {
      return null;
    }
    return normalizeSchema(schema, null, binaryStringFields == null ? Set.of() : binaryStringFields,
        new IdentityHashMap<>());
  }

  private static Schema normalizeSchema(Schema schema, String path, Set<String> binaryStringFields,
      Map<Schema, Schema> cache) {
    return switch (schema.getType()) {
      case RECORD -> normalizeRecord(schema, path, binaryStringFields, cache);
      case ARRAY -> normalizeArray(schema, path, binaryStringFields, cache);
      case MAP -> normalizeMap(schema, path, binaryStringFields, cache);
      case UNION -> normalizeUnion(schema, path, binaryStringFields, cache);
      case FIXED,
          BYTES ->
        shouldPromoteToString(schema, path, binaryStringFields)
            ? copyProps(schema, Schema.create(Schema.Type.STRING))
            : schema;
      default -> schema;
    };
  }

  private static Schema normalizeArray(Schema schema, String path, Set<String> binaryStringFields,
      Map<Schema, Schema> cache) {
    Schema cached = cache.get(schema);
    if (cached != null) {
      return cached;
    }
    Schema normalizedElement = normalizeSchema(schema.getElementType(), elementPath(path), binaryStringFields, cache);
    Schema array = copyProps(schema, Schema.createArray(normalizedElement));
    cache.put(schema, array);
    return array;
  }

  private static Schema normalizeMap(Schema schema, String path, Set<String> binaryStringFields,
      Map<Schema, Schema> cache) {
    Schema cached = cache.get(schema);
    if (cached != null) {
      return cached;
    }
    Schema normalizedValue = normalizeSchema(schema.getValueType(), mapValuePath(path), binaryStringFields, cache);
    Schema map = copyProps(schema, Schema.createMap(normalizedValue));
    cache.put(schema, map);
    return map;
  }

  private static Schema normalizeRecord(Schema schema, String path, Set<String> binaryStringFields,
      Map<Schema, Schema> cache) {
    Schema cached = cache.get(schema);
    if (cached != null) {
      return cached;
    }
    Schema record = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
    cache.put(schema, record);
    List<Schema.Field> fields = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      String fieldPath = appendPath(path, field.name());
      Schema normalized = normalizeSchema(field.schema(), fieldPath, binaryStringFields, cache);
      Schema.Field newField = new Schema.Field(field.name(), normalized, field.doc(), field.defaultVal(),
          field.order());
      copyFieldProps(field, newField);
      fields.add(newField);
    }
    record.setFields(fields);
    copyProps(schema, record);
    copyAliases(schema, record);
    return record;
  }

  private static Schema normalizeUnion(Schema schema, String path, Set<String> binaryStringFields,
      Map<Schema, Schema> cache) {
    Schema cached = cache.get(schema);
    if (cached != null) {
      return cached;
    }
    List<Schema> types = new ArrayList<>();
    boolean changed = false;
    for (Schema member : schema.getTypes()) {
      Schema normalized = normalizeSchema(member, path, binaryStringFields, cache);
      types.add(normalized);
      changed |= normalized != member;
    }
    if (!changed) {
      cache.put(schema, schema);
      return schema;
    }
    Schema union = Schema.createUnion(types);
    copyProps(schema, union);
    cache.put(schema, union);
    return union;
  }

  private static boolean hasUtf8Semantic(Schema schema) {
    if (schema == null) {
      return false;
    }
    if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
      return false;
    }
    String logicalType = schema.getProp("logicalType");
    if (logicalType != null && "decimal".equalsIgnoreCase(logicalType)) {
      return false;
    }
    if (logicalType != null && "string".equalsIgnoreCase(logicalType)) {
      return true;
    }
    String originalType = schema.getProp("originalType");
    if (originalType != null && "UTF8".equalsIgnoreCase(originalType)) {
      return true;
    }
    String convertedType = schema.getProp("convertedType");
    if (convertedType != null && "UTF8".equalsIgnoreCase(convertedType)) {
      return true;
    }
    Object javaString = schema.getObjectProp("avro.java.string");
    return javaString != null;
  }

  private static boolean shouldPromoteToString(Schema schema, String path, Set<String> binaryStringFields) {
    if (hasUtf8Semantic(schema)) {
      return true;
    }
    if (path == null || binaryStringFields.isEmpty()) {
      return false;
    }
    return binaryStringFields.contains(path);
  }

  private static String appendPath(String parent, String child) {
    if (child == null || child.isEmpty()) {
      return parent;
    }
    if (parent == null || parent.isEmpty()) {
      return child;
    }
    return parent + "." + child;
  }

  private static String elementPath(String parent) {
    if (parent == null || parent.isEmpty()) {
      return "[]";
    }
    return parent + "[]";
  }

  private static String mapValuePath(String parent) {
    if (parent == null || parent.isEmpty()) {
      return "<value>";
    }
    return parent + ".<value>";
  }

  private static Set<String> detectBinaryStringFields(MessageType schema, Map<String, String> metadata) {
    if (schema == null) {
      return Set.of();
    }
    Set<String> result = new LinkedHashSet<>();
    collectAnnotatedStringFields(schema, "", result);
    result.addAll(parseColumnTypeHints(schema, metadata));
    return result;
  }

  private static void collectAnnotatedStringFields(Type type, String parentPath, Set<String> target) {
    if (type == null) {
      return;
    }
    String currentPath = appendPath(parentPath, type.getName());
    if (type.isPrimitive()) {
      if (isStringAnnotatedBinary(type)) {
        target.add(currentPath);
      }
      return;
    }
    for (Type child : type.asGroupType().getFields()) {
      collectAnnotatedStringFields(child, currentPath, target);
    }
  }

  /**
   * Determine whether the supplied Parquet type encodes textual data using the
   * UTF-8 string logical type.
   *
   * @param type
   *          the Parquet type to inspect (may be {@code null})
   * @return {@code true} if the type is a binary primitive annotated with the
   *         {@link LogicalTypeAnnotation.StringLogicalTypeAnnotation}
   */
  static boolean isStringAnnotatedBinary(Type type) {
    if (type == null || !type.isPrimitive()) {
      return false;
    }
    if (type.asPrimitiveType().getPrimitiveTypeName() != PrimitiveTypeName.BINARY) {
      return false;
    }
    LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();
    return logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation;
  }

  private static Set<String> parseColumnTypeHints(MessageType schema, Map<String, String> metadata) {
    if (schema == null || metadata == null || metadata.isEmpty()) {
      return Set.of();
    }
    List<Type> fields = schema.getFields();
    List<String> hints = metadata.entrySet().stream()
        .filter(entry -> entry.getKey() != null && entry.getKey().endsWith(".columnTypes")).map(Map.Entry::getValue)
        .filter(value -> value != null && !value.isBlank()).findFirst().map(ParquetSchemas::splitTypeHints)
        .orElse(List.of());
    if (hints.isEmpty() || hints.size() != fields.size()) {
      return Set.of();
    }
    Set<String> result = new LinkedHashSet<>();
    for (int i = 0; i < hints.size(); i++) {
      if (isStringHint(hints.get(i))) {
        result.add(fields.get(i).getName());
      }
    }
    return result;
  }

  private static List<String> splitTypeHints(String value) {
    return List.of(value.split(",")).stream().map(String::trim).filter(token -> !token.isEmpty())
        .collect(Collectors.toList());
  }

  private static boolean isStringHint(String typeName) {
    if (typeName == null || typeName.isEmpty()) {
      return false;
    }
    String normalized = typeName.trim();
    if (normalized.isEmpty()) {
      return false;
    }
    if (normalized.startsWith("class ")) {
      normalized = normalized.substring("class ".length());
    }
    if (normalized.startsWith("java.lang.")) {
      normalized = normalized.substring("java.lang.".length());
    }
    return "String".equals(normalized) || "CharSequence".equals(normalized) || normalized.endsWith("Utf8");
  }

  private static Schema copyProps(Schema source, Schema target) {
    if (source == null || target == null) {
      return target;
    }
    for (Map.Entry<String, Object> entry : source.getObjectProps().entrySet()) {
      target.addProp(entry.getKey(), entry.getValue());
    }
    return target;
  }

  private static void copyFieldProps(Schema.Field source, Schema.Field target) {
    for (Map.Entry<String, Object> entry : source.getObjectProps().entrySet()) {
      target.addProp(entry.getKey(), entry.getValue());
    }
    for (String alias : source.aliases()) {
      target.addAlias(alias);
    }
  }

  private static void copyAliases(Schema source, Schema target) {
    for (String alias : source.getAliases()) {
      target.addAlias(alias);
    }
  }
}
