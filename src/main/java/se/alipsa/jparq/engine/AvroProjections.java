package se.alipsa.jparq.engine;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;

/**
 * Builds a projection (subset) Avro schema keeping field order from the file
 * schema.
 */
public final class AvroProjections {

  private AvroProjections() {
  }

  /**
   * @param fileSchema
   *          full file schema (record)
   * @param needed
   *          field names to keep; order will follow fileSchema
   * @return record schema with only the needed fields (or fileSchema if needed is
   *         null/empty)
   */
  public static Schema project(Schema fileSchema, Set<String> needed) {
    if (fileSchema.getType() != Schema.Type.RECORD || needed == null || needed.isEmpty()) {
      return fileSchema;
    }
    // Preserve order: iterate file schema, pick matching fields
    List<Schema.Field> kept = fileSchema.getFields().stream().filter(f -> needed.contains(f.name()))
        .map(f -> copyField(f)) // keep original field type/props/defaults
        .collect(Collectors.toList());

    if (kept.isEmpty()) { // fall back to full schema (reader requires at least 1)
      return fileSchema;
    }

    Schema proj = Schema.createRecord(fileSchema.getName(), fileSchema.getDoc(), fileSchema.getNamespace(),
        fileSchema.isError());
    proj.setFields(kept);
    // copy object props (aliases etc.) if you want
    copyObjectProps(fileSchema, proj);
    return proj;
  }

  private static Schema.Field copyField(Schema.Field f) {
    Schema.Field nf = new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order());
    copyObjectProps(f, nf);
    return nf;
  }

  private static void copyObjectProps(Schema from, Schema to) {
    for (String k : from.getObjectProps().keySet()) {
      to.addProp(k, from.getObjectProp(k));
    }
  }

  private static void copyObjectProps(Schema.Field from, Schema.Field to) {
    // Copy all non-reserved props
    from.getObjectProps().forEach((k, v) -> to.addProp(k, v));
    // Copy aliases
    if (from.aliases() != null) {
      for (String a : from.aliases()) {
        to.addAlias(a);
      }
    }
  }
}
