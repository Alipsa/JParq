package se.alipsa.jparq.meta;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.engine.IdentifierUtil;

/**
 * Utility for materializing the SQL standard INFORMATION_SCHEMA.TABLES view.
 */
public final class InformationSchemaTables {

  /** Fully qualified identifier used to reference the view. */
  public static final String TABLE_IDENTIFIER = "information_schema.tables";
  private static final String SCHEMA_NAME = "information_schema";
  private static final String TABLE_NAME = "tables";
  private static final Schema TABLE_SCHEMA = buildSchema();

  private InformationSchemaTables() {
  }

  private static Schema buildSchema() {
    return SchemaBuilder.record("information_schema_tables").namespace("se.alipsa.jparq.meta").fields()
        .name("TABLE_CATALOG").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
        .name("TABLE_SCHEMA").type().stringType().noDefault().name("TABLE_NAME").type().unionOf().nullType().and()
        .stringType().endUnion().nullDefault().name("TABLE_TYPE").type().unionOf().nullType().and().stringType()
        .endUnion().nullDefault().name("REMARKS").type().unionOf().nullType().and().stringType().endUnion()
        .nullDefault().endRecord();
  }

  /**
   * Resolve whether the provided table reference targets the
   * INFORMATION_SCHEMA.TABLES view.
   *
   * @param schemaName
   *          schema portion of the identifier (may be {@code null})
   * @param tableName
   *          table portion of the identifier (may be {@code null})
   * @param fullyQualified
   *          fully qualified identifier (may be {@code null})
   * @return {@code true} when the identifier references INFORMATION_SCHEMA.TABLES
   */
  public static boolean matchesQualifiedName(String schemaName, String tableName, String fullyQualified) {
    String schema = normalize(schemaName);
    String table = normalize(tableName);
    if (schema != null && table != null) {
      return SCHEMA_NAME.equals(schema) && TABLE_NAME.equals(table);
    }
    String qualified = normalize(fullyQualified);
    return TABLE_IDENTIFIER.equals(qualified);
  }

  private static String normalize(String identifier) {
    String sanitized = IdentifierUtil.sanitizeIdentifier(identifier);
    if (sanitized == null) {
      return null;
    }
    String trimmed = sanitized.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    return trimmed.toLowerCase(Locale.ROOT);
  }

  /**
   * Determine whether the supplied table reference corresponds to the information
   * schema view.
   *
   * @param tableName
   *          raw table identifier emitted by the SQL parser
   * @return {@code true} when {@code tableName} represents
   *         INFORMATION_SCHEMA.TABLES
   */
  public static boolean matchesTableReference(String tableName) {
    String normalized = normalize(tableName);
    return TABLE_IDENTIFIER.equals(normalized);
  }

  /**
   * Materialize the INFORMATION_SCHEMA.TABLES view by adapting the driver
   * metadata rows to a virtual table.
   *
   * @param connection
   *          active connection providing access to database metadata
   * @return immutable schema and rows describing the information schema view
   * @throws SQLException
   *           if the metadata cannot be queried
   */
  public static TableData load(JParqConnection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    List<GenericRecord> rows = new ArrayList<>();
    try (ResultSet rs = metaData.getTables(connection.getCatalog(), null, "%", null)) {
      while (rs.next()) {
        GenericRecord record = new GenericData.Record(TABLE_SCHEMA);
        record.put("TABLE_CATALOG", rs.getString("TABLE_CAT"));
        record.put("TABLE_SCHEMA", rs.getString("TABLE_SCHEM"));
        record.put("TABLE_NAME", rs.getString("TABLE_NAME"));
        String tableType = rs.getString("TABLE_TYPE");
        if (tableType != null && "TABLE".equalsIgnoreCase(tableType)) {
          tableType = "BASE TABLE";
        }
        record.put("TABLE_TYPE", tableType);
        record.put("REMARKS", rs.getString("REMARKS"));
        rows.add(record);
      }
    }
    return new TableData(TABLE_SCHEMA, rows);
  }

  /**
   * Container describing the schema and rows backing the
   * INFORMATION_SCHEMA.TABLES view.
   *
   * @param schema
   *          Avro schema representing the view
   * @param rows
   *          immutable rows that populate the view
   */
  public record TableData(Schema schema, List<GenericRecord> rows) {

    /**
     * Create an immutable view of the supplied table data.
     */
    public TableData {
      schema = Objects.requireNonNull(schema, "schema");
      rows = List.copyOf(Objects.requireNonNull(rows, "rows"));
    }
  }
}
