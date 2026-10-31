package se.alipsa.jparq;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.avro.Schema;
import se.alipsa.jparq.engine.MaterializedNames;
import se.alipsa.jparq.helper.IntervalType;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.TemporalInterval;
import se.alipsa.jparq.model.ResultSetMetaDataAdapter;

/**
 * Simple metadata for aggregate result sets where the values do not originate
 * from the underlying Avro schema.
 */
public class AggregateResultSetMetaData extends ResultSetMetaDataAdapter implements AvroColumnMetadata {

  private final List<String> labels;
  private final List<Integer> sqlTypes;
  private final String tableName;
  private final String tableSchema;
  private final String tableCatalog;
  private final int[] precisions;
  private final int[] scales;
  private List<Schema> valueSchemas = List.of();

  /**
   * Create metadata for an aggregate result set.
   *
   * @param labels
   *          column labels exposed to JDBC callers
   * @param sqlTypes
   *          SQL types corresponding to each column label
   * @param tableName
   *          virtual table name reported to JDBC clients
   * @param tableSchema
   *          schema reported to JDBC clients (may be {@code null})
   * @param tableCatalog
   *          catalog reported to JDBC clients (may be {@code null})
   */
  public AggregateResultSetMetaData(List<String> labels, List<Integer> sqlTypes, String tableName, String tableSchema,
      String tableCatalog) {
    this(labels, sqlTypes, tableName, tableSchema, tableCatalog, List.of());
  }

  /**
   * Creates metadata with decimal precision and scale derived from materialized
   * values.
   *
   * @param labels
   *          the output column labels
   * @param sqlTypes
   *          the JDBC types in projection order
   * @param tableName
   *          the virtual table name
   * @param tableSchema
   *          the owning schema, or null
   * @param tableCatalog
   *          the owning catalog, or null
   * @param rows
   *          the materialized rows used to determine decimal bounds
   */
  public AggregateResultSetMetaData(List<String> labels, List<Integer> sqlTypes, String tableName, String tableSchema,
      String tableCatalog, List<List<Object>> rows) {
    this.labels = new ArrayList<>(labels);
    this.sqlTypes = new ArrayList<>(sqlTypes);
    this.tableName = tableName;
    this.tableSchema = tableSchema;
    this.tableCatalog = tableCatalog;
    this.precisions = new int[labels.size()];
    this.scales = new int[labels.size()];
    this.valueSchemas = new ArrayList<>(java.util.Collections.nCopies(labels.size(), null));
    for (int column = 0; column < labels.size(); column++) {
      int index = column;
      if (rows.stream().anyMatch(row -> row.get(index) instanceof TemporalInterval)) {
        valueSchemas.set(column, IntervalType.infer(rows.stream().map(row -> row.get(index)).toList()));
      }
    }
    for (int i = 0; i < labels.size(); i++) {
      if (getColumnType(i + 1) != Types.DECIMAL && getColumnType(i + 1) != Types.NUMERIC) {
        continue;
      }
      int column = i;
      List<Object> values = rows.stream().map(row -> row.get(column)).filter(Number.class::isInstance).toList();
      if (!values.isEmpty()) {
        var decimal = DecimalTypeInference.infer(values);
        precisions[i] = decimal.getPrecision();
        scales[i] = decimal.getScale();
      }
    }
  }

  /**
   * Retains source schemas and declared decimal bounds for materialized columns.
   *
   * @param schemas
   *          schemas in output order, allowing null for unknown schemas
   */
  void retainValueSchemas(List<Schema> schemas) {
    List<Schema> retained = new ArrayList<>(schemas);
    for (int i = 0; i < retained.size(); i++) {
      if (retained.get(i) == null && i < valueSchemas.size()) {
        retained.set(i, valueSchemas.get(i));
      }
    }
    valueSchemas = retained;
    for (int i = 0; i < schemas.size(); i++) {
      Schema schema = schemas.get(i);
      if (schema != null && schema.getLogicalType() instanceof Decimal decimal) {
        precisions[i] = decimal.getPrecision();
        scales[i] = decimal.getScale();
      }
    }
  }

  /**
   * Applies declared output types and numeric bounds, including for empty groups.
   *
   * @param projected
   *          metadata resolved from the source schema and expressions
   */
  void retainExpressionTypes(JParqResultSetMetaData projected) {
    List<Schema> schemas = new ArrayList<>();
    for (int i = 0; i < labels.size(); i++) {
      int column = i + 1;
      NumericType numeric = projected.numericType(column);
      if (numeric != null) {
        sqlTypes.set(i, numeric.jdbcType());
        if (numeric.jdbcType() != Types.DECIMAL || numeric.precision() > 0) {
          precisions[i] = numeric.jdbcPrecision();
          scales[i] = numeric.scale();
        }
      }
      Schema declaration = projected.columnValueSchema(column);
      if (numeric == null && declaration != null) {
        sqlTypes.set(i, JdbcTypeMapper.mapSchemaToJdbcType(declaration));
      }
      schemas.add(numeric != null ? numeric.schema() : declaration != null ? declaration : columnValueSchema(column));
    }
    valueSchemas = schemas;
  }

  /** {@inheritDoc} */
  @Override
  public Schema columnValueSchema(int column) {
    return valueSchemas.isEmpty() ? null : valueSchemas.get(column - 1);
  }

  @Override
  public int getColumnCount() {
    return labels.size();
  }

  /** {@inheritDoc} */
  @Override
  public String columnSqlLabel(int column) {
    return labels.get(column - 1);
  }

  @Override
  public String getColumnLabel(int column) {
    return MaterializedNames.displayLabel(columnSqlLabel(column));
  }

  @Override
  public String getColumnName(int column) {
    return getColumnLabel(column);
  }

  @Override
  public String getTableName(int column) {
    return tableName;
  }

  @Override
  public String getSchemaName(int column) {
    return tableSchema == null ? "" : tableSchema;
  }

  @Override
  public String getCatalogName(int column) {
    return tableCatalog == null ? "" : tableCatalog;
  }

  @Override
  public int getColumnType(int column) {
    if (sqlTypes.isEmpty()) {
      return Types.OTHER;
    }
    int idx = column - 1;
    if (idx < 0 || idx >= sqlTypes.size()) {
      return Types.OTHER;
    }
    Integer type = sqlTypes.get(idx);
    return type == null ? Types.OTHER : type;
  }

  @Override
  public String getColumnTypeName(int column) {
    return IntervalType.matches(columnValueSchema(column))
        ? "INTERVAL"
        : JDBCType.valueOf(getColumnType(column)).getName();
  }

  /** {@inheritDoc} */
  @Override
  public int getPrecision(int column) {
    return NumericType.jdbcPrecision(getColumnType(column), precisions[column - 1]);
  }

  /** {@inheritDoc} */
  @Override
  public int getScale(int column) {
    return scales[column - 1];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getColumnClassName(int column) {
    Schema schema = columnValueSchema(column);
    int type = getColumnType(column);
    return schema == null || type == Types.SMALLINT || type == Types.TINYINT
        ? JdbcTypeMapper.mapJdbcTypeToClassName(getColumnType(column))
        : JdbcTypeMapper.mapSchemaToJavaClassName(schema);
  }

  @Override
  public int isNullable(int column) {
    return columnNullableUnknown;
  }
}
