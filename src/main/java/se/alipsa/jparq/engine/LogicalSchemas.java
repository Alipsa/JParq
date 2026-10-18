package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.avro.Schema;
import se.alipsa.jparq.helper.JdbcTypeMapper;

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

  /**
   * Describes the first logical difference without dumping complete schemas.
   *
   * @param first
   *          reference schema
   * @param second
   *          candidate schema
   * @return field path and difference, or empty for compatible schemas
   */
  public static Optional<String> firstDifference(Schema first, Schema second) {
    return Optional.ofNullable(difference(ParquetSchemas.normalizeStringTypes(first),
        ParquetSchemas.normalizeStringTypes(second), "", new IdentityHashMap<>()));
  }

  private static String difference(Schema first, Schema second, String path, Map<Schema, Schema> ancestors) {
    if (compatible(first, second) || ancestors.get(first) == second) {
      return null;
    }
    String label = path.isEmpty() ? "<root>" : path;
    boolean firstNullable = JdbcTypeMapper.isNullable(first);
    boolean secondNullable = JdbcTypeMapper.isNullable(second);
    if (firstNullable != secondNullable) {
      return label + ": " + (firstNullable ? "nullable vs required" : "required vs nullable");
    }
    if (first.getType() != second.getType()) {
      return label + ": " + first.getType() + " vs " + second.getType();
    }
    ancestors.put(first, second);
    try {
      for (String property : List.of("logicalType", "jparq.logicalType", "precision", "scale")) {
        if (!Objects.equals(first.getObjectProp(property), second.getObjectProp(property))) {
          return label + ": " + property + " " + first.getObjectProp(property) + " vs "
              + second.getObjectProp(property);
        }
      }
      switch (first.getType()) {
        case RECORD -> {
          List<Schema.Field> left = first.getFields();
          List<Schema.Field> right = second.getFields();
          for (int index = 0; index < Math.max(left.size(), right.size()); index++) {
            if (index >= left.size()) {
              return childPath(path, right.get(index).name()) + ": unexpected field";
            }
            if (index >= right.size()) {
              return childPath(path, left.get(index).name()) + ": missing field";
            }
            Schema.Field field = left.get(index);
            Schema.Field other = right.get(index);
            String fieldPath = childPath(path, field.name());
            if (!field.name().equals(other.name())) {
              return fieldPath + ": expected field " + field.name() + " vs " + other.name() + " at position " + index;
            }
            String mismatch = difference(field.schema(), other.schema(), fieldPath, ancestors);
            if (mismatch != null) {
              return mismatch;
            }
          }
        }
        case ARRAY -> {
          return difference(first.getElementType(), second.getElementType(), label + "[]", ancestors);
        }
        case MAP -> {
          return difference(first.getValueType(), second.getValueType(), label + "{}", ancestors);
        }
        case UNION -> {
          List<Schema> remaining = new ArrayList<>(second.getTypes());
          List<Schema> unmatched = new ArrayList<>();
          for (Schema member : first.getTypes()) {
            Schema match = remaining.stream().filter(other -> compatible(member, other)).findFirst().orElse(null);
            if (match == null) {
              unmatched.add(member);
            } else {
              remaining.remove(match);
            }
          }
          for (Schema member : unmatched) {
            Schema candidate = remaining.stream().filter(other -> other.getType() == member.getType()).findFirst()
                .orElse(null);
            if (candidate == null) {
              return unionDifference(label, first, second);
            }
            String mismatch = difference(member, candidate, path, ancestors);
            if (mismatch != null) {
              return mismatch;
            }
          }
          return unionDifference(label, first, second);
        }
        case FIXED -> {
          return label + ": fixed size " + first.getFixedSize() + " vs " + second.getFixedSize();
        }
        case ENUM -> {
          return label + ": enum symbols " + first.getEnumSymbols() + " vs " + second.getEnumSymbols();
        }
        default -> {
          // Primitive differences are reported above.
        }
      }
      return null;
    } finally {
      ancestors.remove(first);
    }
  }

  /**
   * Describes both branch sets when a union alternative has no matching type.
   *
   * @param path
   *          differing field path
   * @param first
   *          reference union
   * @param second
   *          candidate union
   * @return branch types from both schemas
   */
  private static String unionDifference(String path, Schema first, Schema second) {
    return path + ": union members " + first.getTypes().stream().map(Schema::getType).toList() + " vs "
        + second.getTypes().stream().map(Schema::getType).toList();
  }

  private static String childPath(String parent, String field) {
    return parent.isEmpty() ? field : parent + "." + field;
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
