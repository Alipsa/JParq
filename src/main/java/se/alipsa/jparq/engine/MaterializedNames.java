package se.alipsa.jparq.engine;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.avro.Schema;

/** Separates SQL output identifiers from Avro's restricted internal names. */
public final class MaterializedNames {

  /**
   * Schema property retaining the original SQL output identifier or expression.
   */
  public static final String SQL_LABEL = "jparq.sqlLabel";

  /** Schema property retaining a column's SQL lookup key. */
  public static final String LOOKUP_KEY = "jparq.lookupKey";

  private static final Pattern AVRO_NAME = Pattern.compile("[A-Za-z_][A-Za-z_0-9]*");

  private MaterializedNames() {
  }

  /**
   * Allocates a unique Avro name, preserving valid names when possible.
   *
   * @param label
   *          SQL label or record name
   * @param used
   *          names already allocated in this schema, updated by this call
   * @return a unique valid Avro name
   */
  public static String allocate(String label, Set<String> used) {
    String text = displayLabel(label);
    String base = text != null && AVRO_NAME.matcher(text).matches() ? text : "_column";
    String candidate = base;
    int suffix = 1;
    while (!used.add(candidate)) {
      candidate = base + "_" + suffix++;
    }
    return candidate;
  }

  /**
   * Sanitizes an internal record name without allocating column names.
   *
   * @param label
   *          SQL table or record name
   * @return a valid record name, using a record-specific fallback
   */
  public static String recordName(String label) {
    String text = displayLabel(label);
    return text != null && AVRO_NAME.matcher(text).matches() ? text : "_record";
  }

  /**
   * Resolves the SQL lookup key of a field.
   *
   * @param field
   *          source field
   * @return explicit SQL lookup key or the ordinary field-name key
   */
  public static String lookupKey(Schema.Field field) {
    String key = field.getProp(LOOKUP_KEY);
    return key == null ? Identifier.lookupKey(field.name()) : key;
  }

  /**
   * Builds an index of visible SQL names; empty values mark ambiguous names.
   *
   * @param schema
   *          source schema, or null for an unavailable schema
   * @return SQL lookup keys mapped to internal fields or ambiguity markers
   */
  public static Map<String, String> lookupIndex(Schema schema) {
    Map<String, String> index = new LinkedHashMap<>();
    if (schema != null) {
      for (Schema.Field field : schema.getFields()) {
        index.merge(lookupKey(field), field.name(), (left, right) -> left.equals(right) ? left : "");
      }
    }
    return Map.copyOf(index);
  }

  /**
   * Prevents an unresolved SQL identifier from addressing a generated internal
   * name.
   *
   * @param schema
   *          source schema
   * @param reference
   *          SQL column identifier
   * @param canonical
   *          resolved name or an unresolved reference
   * @return resolved name, or null for an internal-name fallback
   */
  public static String sqlReference(Schema schema, String reference, String canonical) {
    Schema.Field field = canonical == null ? null : schema.getField(canonical);
    Identifier identifier = Identifier.of(reference);
    if (field != null && identifier != null && canonical.equals(identifier.text())
        && field.getProp(SQL_LABEL) != null) {
      String key = lookupKey(field);
      boolean quotedLowercase = identifier.quoted() && key.equals("U:" + identifier.text());
      if (!key.equals(identifier.lookupKey()) && !quotedLowercase) {
        return null;
      }
    }
    return canonical;
  }

  /**
   * Resolves a field's SQL label, including its original quoting semantics.
   *
   * @param field
   *          source field
   * @return original SQL label, or its physical name for ordinary source fields
   */
  public static String sqlLabel(Schema.Field field) {
    String label = field.getProp(SQL_LABEL);
    return label == null ? field.name() : label;
  }

  /**
   * Removes identifier delimiters from a JDBC output label.
   *
   * @param label
   *          raw SQL label
   * @return display label retaining identifier case and interior whitespace
   */
  public static String displayLabel(String label) {
    Identifier identifier = Identifier.of(label);
    return identifier != null && identifier.quoted() ? identifier.text() : label;
  }
}
