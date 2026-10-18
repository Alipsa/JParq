package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;

/**
 * Compares logical field structure independently of writer names and metadata.
 */
public final class LogicalSchemas {
  private LogicalSchemas() {
  }

  /**
   * Compares fields, ordering, nullability, and logical types recursively.
   * 
   * @param first
   *          reference schema
   * @param second
   *          candidate schema
   * @return whether the schemas have the same logical structure
   */
  public static boolean compatible(Schema first, Schema second) {
    return structure(ParquetSchemas.normalizeStringTypes(first), new IdentityHashMap<>())
        .equals(structure(ParquetSchemas.normalizeStringTypes(second), new IdentityHashMap<>()));
  }

  private static String structure(Schema schema, Map<Schema, Integer> ancestors) {
    Integer previous = ancestors.get(schema);
    if (previous != null) {
      return "recursive:" + previous;
    }
    ancestors.put(schema, ancestors.size());
    List<String> children = new ArrayList<>();
    switch (schema.getType()) {
      case RECORD -> {
        for (Schema.Field field : schema.getFields()) {
          children.add(field.name() + ":" + structure(field.schema(), ancestors));
        }
      }
      case ARRAY -> children.add(structure(schema.getElementType(), ancestors));
      case MAP -> children.add(structure(schema.getValueType(), ancestors));
      case UNION -> {
        for (Schema member : schema.getTypes()) {
          children.add(structure(member, ancestors));
        }
        children.sort(String::compareTo);
      }
      case FIXED -> children.add(Integer.toString(schema.getFixedSize()));
      case ENUM -> children.addAll(schema.getEnumSymbols());
      default -> {
        // Primitive structure is represented by the type and logical properties.
      }
    }
    ancestors.remove(schema);
    for (String property : List.of("logicalType", "jparq.logicalType", "precision", "scale")) {
      children.add(property + "=" + schema.getObjectProp(property));
    }
    return schema.getType() + children.toString();
  }
}
