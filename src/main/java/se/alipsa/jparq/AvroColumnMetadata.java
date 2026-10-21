package se.alipsa.jparq;

import org.apache.avro.Schema;

/** Retains source schemas that JDBC type codes cannot fully describe. */
interface AvroColumnMetadata {

  /**
   * Resolves a projected column's non-null source schema.
   *
   * @param column
   *          one-based output column index
   * @return source schema, or null when the column is computed or unknown
   */
  Schema columnValueSchema(int column);
}
