package se.alipsa.jparq.engine;

import java.util.Set;
import org.apache.avro.Schema;

/** Separates SQL output identifiers from Avro's restricted internal names. */
public final class MaterializedNames {

  /**
   * Schema property retaining the original SQL output identifier or expression.
   */
  public static final String SQL_LABEL = "jparq.sqlLabel";

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
    String base = text != null && text.matches("[A-Za-z_][A-Za-z_0-9]*") ? text : "_column";
    String candidate = base;
    int suffix = 1;
    while (!used.add(candidate)) {
      candidate = base + "_" + suffix++;
    }
    return candidate;
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
