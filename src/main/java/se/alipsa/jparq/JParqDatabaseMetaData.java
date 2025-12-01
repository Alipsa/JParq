package se.alipsa.jparq;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import se.alipsa.jparq.helper.JParqUtil;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/**
 * An implementation of the java.sql.DatabaseMetaData interface for parquet
 * files.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqDatabaseMetaData implements DatabaseMetaData {

  /**
   * Canonical JDBC numeric functions and their SQL equivalents.
   */
  public enum JdbcNumericFunction {
    /** Absolute value function */
    ABS("ABS"),
    /** Arc cosine function */
    ACOS("ACOS"),
    /** Arc sine function */
    ASIN("ASIN"),
    /** Arc tangent function */
    ATAN("ATAN"),
    /** Arc tangent function with two arguments */
    ATAN2("ATAN2"),
    /** Ceiling function */
    CEILING("CEILING", "CEILING", "CEIL"),
    /** Cosine function */
    COS("COS"),
    /** Cotangent function */
    COT("COT"),
    /** Degrees conversion function */
    DEGREES("DEGREES"),
    /** Exponential function */
    EXP("EXP"),
    /** Floor function */
    FLOOR("FLOOR"),
    /** Natural logarithm function */
    LOG("LOG"),
    /** Base 10 logarithm function */
    LOG10("LOG10"),
    /** Modulo function */
    MOD("MOD"),
    /** PI constant function */
    PI("PI"),
    /** Power function */
    POWER("POWER", "POWER", "POW"),
    /** Radians conversion function */
    RADIANS("RADIANS"),
    /** Random number generator */
    RAND("RAND", "RAND", "RANDOM"),
    /** Rounding function */
    ROUND("ROUND"),
    /** Sign function */
    SIGN("SIGN"),
    /** Sine function */
    SIN("SIN"),
    /** Square root function */
    SQRT("SQRT"),
    /** Tangent function */
    TAN("TAN"),
    /** Truncate function */
    TRUNCATE("TRUNCATE", "TRUNCATE", "TRUNC");

    private final String jdbcName;
    private final String sqlName;
    private final List<String> jdbcNames;

    JdbcNumericFunction(String jdbcName) {
      this(jdbcName, jdbcName);
    }

    JdbcNumericFunction(String jdbcName, String sqlName, String... aliases) {
      this.jdbcName = jdbcName;
      this.sqlName = sqlName;
      List<String> names = new ArrayList<>();
      names.add(jdbcName);
      if (aliases != null && aliases.length > 0) {
        names.addAll(List.of(aliases));
      }
      this.jdbcNames = List.copyOf(names);
    }

    /**
     * The JDBC escape function name.
     *
     * @return canonical JDBC function identifier
     */
    public String jdbcName() {
      return jdbcName;
    }

    /**
     * All JDBC escape names mapping to this SQL function, including aliases.
     *
     * @return immutable list of JDBC function identifiers
     */
    public List<String> jdbcNames() {
      return jdbcNames;
    }

    /**
     * The SQL function name equivalent to the JDBC escape name.
     *
     * @return SQL function identifier
     */
    public String sqlName() {
      return sqlName;
    }

    /**
     * Render the JDBC function name in escaped form ({@code {fn NAME}}) without
     * arguments.
     *
     * @return formatted JDBC escape name
     */
    public String formatted() {
      return "{fn " + jdbcName + "}";
    }
  }

  /**
   * Canonical JDBC string functions and their SQL equivalents.
   */
  public enum JdbcStringFunction {
    /** ASCII function */
    ASCII("ASCII", "ASCII"),
    /** CHAR function */
    CHAR("CHAR", "CHAR"),
    /** CONCAT function */
    CONCAT("CONCAT", "CONCAT"),
    /** DIFFERENCE function */
    DIFFERENCE("DIFFERENCE", "DIFFERENCE"),
    /** INSERT function */
    INSERT("INSERT", "INSERT"),
    /** LCASE function */
    LCASE("LCASE", "LOWER"),
    /** LEFT function */
    LEFT("LEFT", "LEFT"),
    /** LENGTH function */
    LENGTH("LENGTH", "LENGTH"),
    /** LOCATE function */
    LOCATE("LOCATE", "LOCATE"),
    /** LTRIM function */
    LTRIM("LTRIM", "LTRIM"),
    /** REPEAT function */
    REPEAT("REPEAT", "REPEAT"),
    /** REPLACE function */
    REPLACE("REPLACE", "REPLACE"),
    /** RIGHT function */
    RIGHT("RIGHT", "RIGHT"),
    /** RTRIM function */
    RTRIM("RTRIM", "RTRIM"),
    /** SOUNDEX function */
    SOUNDEX("SOUNDEX", "SOUNDEX"),
    /** SPACE function */
    SPACE("SPACE", "SPACE"),
    /** SUBSTRING function */
    SUBSTRING("SUBSTRING", "SUBSTRING"),
    /** UCASE function */
    UCASE("UCASE", "UPPER");

    private final String jdbcName;
    private final String sqlName;

    JdbcStringFunction(String jdbcName, String sqlName) {
      this.jdbcName = jdbcName;
      this.sqlName = sqlName;
    }

    /**
     * The JDBC escape function name.
     *
     * @return canonical JDBC function identifier
     */
    public String jdbcName() {
      return jdbcName;
    }

    /**
     * The SQL function name equivalent to the JDBC escape name.
     *
     * @return SQL function identifier
     */
    public String sqlName() {
      return sqlName;
    }

    /**
     * Render the JDBC function name in escaped form ({@code {fn NAME}}) without
     * arguments.
     *
     * @return formatted JDBC escape name
     */
    public String formatted() {
      return "{fn " + jdbcName + "}";
    }
  }

  /**
   * Canonical JDBC datetime functions and their SQL equivalents. Used to map JDBC
   * escape syntax (e.g. {@code {fn CURDATE()}}) to native functions understood by
   * the engine.
   */
  public enum JdbcDateTimeFunction {
    /** Current date function */
    CURDATE("CURDATE", "CURRENT_DATE", true),
    /** Current time function */
    CURTIME("CURTIME", "CURRENT_TIME", true),
    /** Current timestamp function */
    NOW("NOW", "CURRENT_TIMESTAMP", true),
    /** Day of week function */
    DAYOFWEEK("DAYOFWEEK", "DAYOFWEEK"),
    /** Day of month function */
    DAYOFMONTH("DAYOFMONTH", "DAYOFMONTH"),
    /** Day of year function */
    DAYOFYEAR("DAYOFYEAR", "DAYOFYEAR"),
    /** Hour function */
    HOUR("HOUR", "HOUR"),
    /** Minute function */
    MINUTE("MINUTE", "MINUTE"),
    /** Month function */
    MONTH("MONTH", "MONTH"),
    /** Quarter function */
    QUARTER("QUARTER", "QUARTER"),
    /** Second function */
    SECOND("SECOND", "SECOND"),
    /** Week function */
    WEEK("WEEK", "WEEK"),
    /** Year function */
    YEAR("YEAR", "YEAR"),
    /** Timestamp add function */
    TIMESTAMPADD("TIMESTAMPADD", "TIMESTAMPADD"),
    /** Timestamp diff function */
    TIMESTAMPDIFF("TIMESTAMPDIFF", "TIMESTAMPDIFF");

    private final String jdbcName;
    private final String sqlName;
    private final boolean noArg;

    JdbcDateTimeFunction(String jdbcName, String sqlName) {
      this(jdbcName, sqlName, false);
    }

    JdbcDateTimeFunction(String jdbcName, String sqlName, boolean noArg) {
      this.jdbcName = jdbcName;
      this.sqlName = sqlName;
      this.noArg = noArg;
    }

    /**
     * JDBC escape function name.
     *
     * @return canonical JDBC function identifier
     */
    public String jdbcName() {
      return jdbcName;
    }

    /**
     * Native SQL function name understood by the engine.
     *
     * @return SQL function identifier
     */
    public String sqlName() {
      return sqlName;
    }

    /**
     * Whether the function is invoked without parentheses in SQL form.
     *
     * @return {@code true} if no parentheses/arguments should be emitted
     */
    public boolean noArg() {
      return noArg;
    }

    /**
     * Render the JDBC function name in escaped form ({@code {fn NAME}}) without
     * arguments.
     *
     * @return formatted JDBC escape name
     */
    public String formatted() {
      return "{fn " + jdbcName + "}";
    }
  }

  /**
   * Canonical JDBC system functions and their SQL equivalents.
   */
  public enum JdbcSystemFunction {
    /** Database name function */
    DATABASE("DATABASE", "DATABASE", true),
    /** If-null function */
    IFNULL("IFNULL", "COALESCE"),
    /** User function */
    USER("USER", "USER", true);

    private final String jdbcName;
    private final String sqlName;
    private final boolean appendEmptyArgs;

    JdbcSystemFunction(String jdbcName, String sqlName, boolean appendEmptyArgs) {
      this.jdbcName = jdbcName;
      this.sqlName = sqlName;
      this.appendEmptyArgs = appendEmptyArgs;
    }

    JdbcSystemFunction(String jdbcName, String sqlName) {
      this(jdbcName, sqlName, false);
    }

    /**
     * The JDBC escape function name.
     *
     * @return canonical JDBC function identifier
     */
    public String jdbcName() {
      return jdbcName;
    }

    /**
     * The SQL function name equivalent to the JDBC escape name.
     *
     * @return SQL function identifier
     */
    public String sqlName() {
      return sqlName;
    }

    /**
     * Whether an empty argument list should be appended when none is supplied in
     * the JDBC escape.
     *
     * @return {@code true} if {@code ()} should be appended when the escape has no
     *         arguments
     */
    public boolean appendEmptyArgs() {
      return appendEmptyArgs;
    }

    /**
     * Render the JDBC function name in escaped form ({@code {fn NAME}}) without
     * arguments.
     *
     * @return formatted JDBC escape name
     */
    public String formatted() {
      return "{fn " + jdbcName + "}";
    }
  }

  /**
   * The SQL keywords that are not part of the SQL standard that are supported.
   */
  public static final List<String> SUPPORTED_KEYWORDS = List.of("ILIKE", "LIMIT", "REGEXP_LIKE");

  private final JParqConnection conn;
  private final String url;

  /**
   * Constructor for JParqDatabaseMetaData.
   *
   * @param conn
   *          the JParqConnection
   * @param url
   *          the jdbc url used to connect
   */
  public JParqDatabaseMetaData(JParqConnection conn, String url) {
    this.conn = conn;
    this.url = url;
  }

  @Override
  public String getDatabaseProductName() {
    return "JParq";
  }

  @Override
  public String getDatabaseProductVersion() {
    return "1.1.0";
  }

  @Override
  public String getDriverName() {
    return "se.alipsa.jparq.JParqDriver";
  }

  @Override
  public String getDriverVersion() {
    return "1.1.0";
  }

  @Override
  public int getDriverMajorVersion() {
    return 1;
  }

  @Override
  public int getDriverMinorVersion() {
    return 1;
  }

  @Override
  public boolean usesLocalFiles() {
    return true;
  }

  @Override
  public boolean usesLocalFilePerTable() {
    return true;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() {
    return conn.isCaseSensitive();
  }

  private static final List<String> TABLE_REMARK_KEYS = List.of("comment", "description", "parquet.schema.comment",
      "doc");
  private static final List<String> COLUMN_REMARK_PREFIXES = List.of("parquet.column.comment.", "column.comment.",
      "parquet.column.description.", "column.description.", "parquet.column.doc.", "column.doc.");

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    List<Object[]> rows = new ArrayList<>();
    Set<String> typeFilter = null;
    if (types != null && types.length > 0) {
      typeFilter = new LinkedHashSet<>();
      for (String type : types) {
        if (type != null) {
          typeFilter.add(type.toUpperCase(Locale.ROOT));
        }
      }
      if (typeFilter.isEmpty()) {
        typeFilter = null;
      }
    }
    boolean caseSensitive = conn.isCaseSensitive();
    String tableRegex = buildRegex(tableNamePattern, caseSensitive);
    String schemaRegex = buildRegex(schemaPattern, caseSensitive);
    Configuration conf = new Configuration(false);
    String effectiveCatalog = (catalog != null && !catalog.isBlank()) ? catalog : conn.getCatalog();
    for (JParqConnection.TableLocation location : conn.listTables()) {
      String schemaName = location.schemaName();
      if (schemaRegex != null && !matchesRegex(schemaRegex, schemaName, caseSensitive)) {
        continue;
      }
      String base = location.tableName();
      if (tableRegex != null && !matchesRegex(tableRegex, base, caseSensitive)) {
        continue;
      }
      String tableType = "TABLE";
      if (typeFilter != null && !typeFilter.contains(tableType.toUpperCase(Locale.ROOT))) {
        continue;
      }
      String remarks = null;
      Path path = new Path(location.file().toURI());
      if (location.file().isFile()) {
        remarks = readTableRemarks(path, conf);
      }
      rows.add(new Object[]{
          effectiveCatalog, formatSchemaName(schemaName), base, tableType, remarks, null, null, null, null, null
      });
    }
    return JParqUtil.listResultSet(new String[]{
        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
        "SELF_REFERENCING_COL_NAME", "REF_GENERATION"
    }, rows);
  }

  @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {

    List<Object[]> rows = new ArrayList<>();
    Configuration conf = new Configuration(false);
    boolean caseSensitive = conn.isCaseSensitive();
    String columnRegex = buildRegex(columnNamePattern, caseSensitive);

    try (ResultSet tables = getTables(catalog, schemaPattern, tableNamePattern, new String[]{
        "TABLE"
    })) {
      Path hPath;
      while (tables.next()) {
        String tableCatalog = tables.getString("TABLE_CAT");
        String tableSchema = tables.getString("TABLE_SCHEM");
        String table = tables.getString("TABLE_NAME");
        String tableType = tables.getString("TABLE_TYPE");
        JParqConnection.TableLocation location = conn.resolveTable(tableSchema, table);
        File file = location.file();
        String resolvedSchemaName = formatSchemaName(location.schemaName());
        hPath = new Path(file.toURI());
        ColumnMetadata columnMetadata = readColumnMetadata(hPath, conf);
        List<String> columnTypeHints = columnMetadata.columnTypeHints();
        Map<String, String> columnRemarks = columnMetadata.columnRemarks();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
            .<GenericRecord>builder(HadoopInputFile.fromPath(hPath, conf)).build()) {

          GenericRecord rec = reader.read();
          if (rec == null) {
            continue; // empty file
          }

          int pos = 1;
          int columnIndex = 0;
          for (Schema.Field field : rec.getSchema().getFields()) {
            int fieldIndex = columnIndex++;
            String columnName = field.name();
            if (columnRegex != null && !matchesRegex(columnRegex, columnName, caseSensitive)) {
              continue;
            }
            Schema baseSchema = JdbcTypeMapper.nonNullSchema(field.schema());
            int jdbcType = JdbcTypeMapper.mapSchemaToJdbcType(field.schema());
            if (!columnTypeHints.isEmpty() && fieldIndex < columnTypeHints.size()) {
              int hintedType = JdbcTypeMapper.mapJavaClassNameToJdbcType(columnTypeHints.get(fieldIndex));
              if (hintedType != Types.OTHER) {
                jdbcType = hintedType;
              }
            }
            String typeName = JDBCType.valueOf(jdbcType).getName();
            String isNullable = JdbcTypeMapper.isNullable(field.schema()) ? "YES" : "NO";
            Integer charMaxLength = resolveCharacterMaximumLength(jdbcType, baseSchema);
            Integer numericPrecision = resolveNumericPrecision(jdbcType, baseSchema);
            Integer numericScale = resolveNumericScale(jdbcType, baseSchema);
            Integer datetimePrecision = resolveDatetimePrecision(jdbcType, baseSchema);
            String columnDefault = resolveColumnDefault(field);
            String columnRemark = resolveColumnRemark(field, columnRemarks);
            rows.add(new Object[]{
                tableCatalog, resolvedSchemaName, table, tableType, columnName, pos++, isNullable, jdbcType, typeName,
                charMaxLength, numericPrecision, numericScale, null, columnDefault, datetimePrecision, columnRemark
            });
          }
        } catch (Exception e) {
          throw new SQLException(e);
        }
      }
    }

    return JParqUtil.listResultSet(new String[]{
        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "COLUMN_NAME", "ORDINAL_POSITION", "IS_NULLABLE",
        "DATA_TYPE", "TYPE_NAME", "CHARACTER_MAXIMUM_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "COLLATION_NAME",
        "COLUMN_DEF", "DATETIME_PRECISION", "REMARKS"
    }, rows);
  }

  /**
   * Read column metadata (type hints and remarks) from the Parquet file footer.
   *
   * @param path
   *          the Hadoop path to the Parquet file
   * @param conf
   *          the Hadoop configuration to use when accessing the file
   * @return immutable column metadata with optional hints and remarks
   * @throws SQLException
   *           if the Parquet metadata cannot be read
   */
  private ColumnMetadata readColumnMetadata(Path path, Configuration conf) throws SQLException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf),
        ParquetReadOptions.builder().build())) {
      ParquetMetadata metadata = reader.getFooter();
      Map<String, String> kv = metadata.getFileMetaData().getKeyValueMetaData();
      List<String> hints = List.of();
      if (kv != null && !kv.isEmpty()) {
        String columnTypes = kv.get("matrix.columnTypes");
        if (columnTypes == null) {
          columnTypes = kv.get("columnTypes");
        }
        if (columnTypes != null && !columnTypes.isBlank()) {
          List<String> parsedHints = parseColumnTypeHints(columnTypes);
          if (!parsedHints.isEmpty()) {
            hints = List.copyOf(parsedHints);
          }
        }
      }
      Map<String, String> columnRemarks = resolveColumnRemarks(kv);
      return new ColumnMetadata(hints, columnRemarks);
    } catch (IOException e) {
      throw new SQLException("Failed to read column metadata", e);
    }
  }

  /**
   * Immutable container for column metadata resolved from the Parquet footer.
   *
   * @param columnTypeHints
   *          optional list of column type hints
   * @param columnRemarks
   *          optional mapping from column name to remark
   */
  private record ColumnMetadata(List<String> columnTypeHints, Map<String, String> columnRemarks) {

    private ColumnMetadata {
      columnTypeHints = columnTypeHints == null || columnTypeHints.isEmpty() ? List.of() : List.copyOf(columnTypeHints);
      columnRemarks = columnRemarks == null || columnRemarks.isEmpty() ? Map.of() : Map.copyOf(columnRemarks);
    }
  }

  /**
   * Extract column-level remark entries from the Parquet metadata map.
   *
   * @param metadata
   *          raw key-value metadata entries (may be {@code null})
   * @return normalized mapping from lowercase column names to remark strings
   */
  private Map<String, String> resolveColumnRemarks(Map<String, String> metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return Map.of();
    }
    Map<String, String> remarks = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      String key = entry.getKey();
      if (key == null) {
        continue;
      }
      String normalizedKey = key.trim().toLowerCase(Locale.ROOT);
      for (String prefix : COLUMN_REMARK_PREFIXES) {
        if (!normalizedKey.startsWith(prefix)) {
          continue;
        }
        String columnKey = normalizedKey.substring(prefix.length());
        if (columnKey.contains(".")) {
          columnKey = columnKey.substring(columnKey.lastIndexOf('.') + 1);
        }
        String trimmedColumn = columnKey.trim();
        if (trimmedColumn.isEmpty()) {
          continue;
        }
        String value = entry.getValue();
        if (value == null) {
          continue;
        }
        String trimmedValue = value.trim();
        if (!trimmedValue.isEmpty()) {
          remarks.putIfAbsent(trimmedColumn, trimmedValue);
        }
      }
    }
    if (remarks.isEmpty()) {
      return Map.of();
    }
    Map<String, String> normalized = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : remarks.entrySet()) {
      normalized.put(entry.getKey().toLowerCase(Locale.ROOT), entry.getValue());
    }
    return Map.copyOf(normalized);
  }

  /**
   * Determine the remark for the supplied column field.
   *
   * @param field
   *          Avro field describing the column (may be {@code null})
   * @param remarksByColumn
   *          pre-resolved metadata driven remarks keyed by column name
   * @return trimmed remark string or {@code null} when unavailable
   */
  private String resolveColumnRemark(Schema.Field field, Map<String, String> remarksByColumn) {
    if (field == null) {
      return null;
    }
    if (remarksByColumn != null && !remarksByColumn.isEmpty()) {
      String name = field.name();
      if (name != null) {
        String remark = remarksByColumn.get(name.trim().toLowerCase(Locale.ROOT));
        if (remark != null && !remark.isBlank()) {
          return remark;
        }
      }
    }
    String doc = field.doc();
    if (doc != null && !doc.isBlank()) {
      return doc.trim();
    }
    String comment = field.getProp("comment");
    if (comment != null && !comment.isBlank()) {
      return comment.trim();
    }
    String description = field.getProp("description");
    if (description != null && !description.isBlank()) {
      return description.trim();
    }
    return null;
  }

  /**
   * Attempt to resolve a table level remark from the Parquet file metadata.
   *
   * @param path
   *          Hadoop path that identifies the Parquet file
   * @param conf
   *          Hadoop configuration used to access the file system
   * @return the resolved remark or {@code null} when no supported metadata entry
   *         is present
   * @throws SQLException
   *           if the Parquet footer cannot be read
   */
  private String readTableRemarks(Path path, Configuration conf) throws SQLException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf),
        ParquetReadOptions.builder().build())) {
      ParquetMetadata metadata = reader.getFooter();
      Map<String, String> kv = metadata.getFileMetaData().getKeyValueMetaData();
      return resolveTableRemark(kv);
    } catch (IOException e) {
      throw new SQLException("Failed to read table remarks for " + path.getName(), e);
    }
  }

  /**
   * Attempt to find a supported remark entry from the supplied metadata map.
   *
   * @param metadata
   *          parquet key-value metadata entries, may be {@code null}
   * @return first matching remark value in priority order or {@code null} if no
   *         supported entries exist
   */
  private static String resolveTableRemark(Map<String, String> metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return null;
    }
    Map<String, String> normalized = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      String key = entry.getKey();
      if (key == null) {
        continue;
      }
      String normalizedKey = key.trim().toLowerCase(Locale.ROOT);
      normalized.putIfAbsent(normalizedKey, entry.getValue());
    }
    for (String remarkKey : TABLE_REMARK_KEYS) {
      String remark = normalized.get(remarkKey);
      if (remark == null) {
        continue;
      }
      String trimmed = remark.trim();
      if (!trimmed.isEmpty()) {
        return trimmed;
      }
    }
    return null;
  }

  /**
   * Parse a comma separated list of column type hints.
   *
   * <p>
   * Empty entries are preserved to maintain positional alignment with the
   * corresponding table columns.
   *
   * @param columnTypes
   *          the comma separated list, may be {@code null}
   * @return an ordered list of hints, potentially containing empty strings when
   *         gaps are present
   */
  public static List<String> parseColumnTypeHints(String columnTypes) {
    if (columnTypes == null || columnTypes.isBlank()) {
      return List.of();
    }
    String[] parts = columnTypes.split(",", -1);
    List<String> hints = new ArrayList<>(parts.length);
    for (String part : parts) {
      hints.add(part.trim());
    }
    return hints.isEmpty() ? List.of() : List.copyOf(hints);
  }

  private String buildRegex(String pattern, boolean caseSensitive) {
    if (pattern == null) {
      return null;
    }
    String normalized = caseSensitive ? pattern : pattern.toLowerCase(Locale.ROOT);
    return JParqUtil.sqlLikeToRegex(normalized);
  }

  private boolean matchesRegex(String regex, String candidate, boolean caseSensitive) {
    if (regex == null) {
      return true;
    }
    if (candidate == null) {
      return false;
    }
    String normalized = caseSensitive ? candidate : candidate.toLowerCase(Locale.ROOT);
    return normalized.matches(regex);
  }

  private String formatSchemaName(String schemaName) {
    if (schemaName == null) {
      return null;
    }
    return conn.isCaseSensitive() ? schemaName : schemaName.toUpperCase(Locale.ROOT);
  }

  /**
   * Determine the character maximum length for string-based columns.
   *
   * @param jdbcType
   *          the resolved JDBC type
   * @param baseSchema
   *          the non-null Avro schema representing the column (may be
   *          {@code null})
   * @return the maximum length when available, otherwise {@code null}
   */
  private Integer resolveCharacterMaximumLength(int jdbcType, Schema baseSchema) {
    return switch (jdbcType) {
      case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> {
        if (baseSchema != null) {
          Object maxLength = baseSchema.getObjectProp("maxLength");
          if (maxLength instanceof Number number) {
            yield number.intValue();
          }
        }
        yield null;
      }
      default -> null;
    };
  }

  /**
   * Determine the numeric precision for a column.
   *
   * @param jdbcType
   *          the resolved JDBC type
   * @param baseSchema
   *          the non-null Avro schema representing the column (may be
   *          {@code null})
   * @return the column precision, or {@code null} when unavailable
   */
  private Integer resolveNumericPrecision(int jdbcType, Schema baseSchema) {
    if (baseSchema != null && baseSchema.getLogicalType() instanceof LogicalTypes.Decimal decimal) {
      return decimal.getPrecision();
    }
    return switch (jdbcType) {
      case Types.TINYINT -> 3;
      case Types.SMALLINT -> 5;
      case Types.INTEGER -> 10;
      case Types.BIGINT -> 19;
      case Types.REAL -> 7;
      case Types.FLOAT, Types.DOUBLE -> 15;
      default -> null;
    };
  }

  /**
   * Determine the numeric scale for a column.
   *
   * @param jdbcType
   *          the resolved JDBC type
   * @param baseSchema
   *          the non-null Avro schema representing the column (may be
   *          {@code null})
   * @return the column scale, or {@code null} when unavailable
   */
  private Integer resolveNumericScale(int jdbcType, Schema baseSchema) {
    if (baseSchema != null && baseSchema.getLogicalType() instanceof LogicalTypes.Decimal decimal) {
      return decimal.getScale();
    }
    return switch (jdbcType) {
      case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT -> 0;
      default -> null;
    };
  }

  /**
   * Determine the datetime precision for time-based logical types.
   *
   * @param jdbcType
   *          resolved JDBC type
   * @param baseSchema
   *          non-null Avro schema backing the column
   * @return precision expressed in fractional digits or {@code null}
   */
  private Integer resolveDatetimePrecision(int jdbcType, Schema baseSchema) {
    if (baseSchema == null) {
      return null;
    }
    return switch (jdbcType) {
      case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> {
        if (baseSchema.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
          yield 6;
        }
        if (baseSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis) {
          yield 3;
        }
        yield null;
      }
      case Types.TIME, Types.TIME_WITH_TIMEZONE -> {
        if (baseSchema.getLogicalType() instanceof LogicalTypes.TimeMicros) {
          yield 6;
        }
        if (baseSchema.getLogicalType() instanceof LogicalTypes.TimeMillis) {
          yield 3;
        }
        yield null;
      }
      default -> null;
    };
  }

  /**
   * Resolve the default value defined for a column.
   *
   * @param field
   *          Avro field describing the column (may be {@code null})
   * @return textual representation of the default or {@code null}
   */
  private String resolveColumnDefault(Schema.Field field) {
    if (field == null) {
      return null;
    }
    Object defaultValue = field.defaultVal();
    if (defaultValue == null || defaultValue == Schema.Field.NULL_DEFAULT_VALUE) {
      return null;
    }
    return defaultValue.toString();
  }

  // Boilerplate defaults for unimplemented metadata
  @Override
  public boolean allProceduresAreCallable() {
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() {
    return true;
  }

  @Override
  public String getURL() {
    return url;
  }

  @Override
  public String getUserName() {
    return System.getProperty("user.name");
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public boolean nullsAreSortedHigh() {
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() {
    return true;
  }

  @Override
  public boolean nullsAreSortedAtStart() {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() {
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return false;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return "";
  }

  @Override
  public String getSQLKeywords() {
    return String.join(", ", SUPPORTED_KEYWORDS);
  }

  @Override
  public String getNumericFunctions() {
    return java.util.stream.Stream.of(JdbcNumericFunction.values()).map(JdbcNumericFunction::formatted)
        .collect(java.util.stream.Collectors.joining(", "));
  }

  @Override
  public String getStringFunctions() {
    return java.util.stream.Stream.of(JdbcStringFunction.values()).map(JdbcStringFunction::formatted)
        .collect(java.util.stream.Collectors.joining(", "));
  }

  @Override
  public String getSystemFunctions() {
    return java.util.stream.Stream.of(JdbcSystemFunction.values()).map(JdbcSystemFunction::formatted)
        .collect(java.util.stream.Collectors.joining(", "));
  }

  @Override
  public String getTimeDateFunctions() {
    return java.util.stream.Stream.of(JdbcDateTimeFunction.values()).map(JdbcDateTimeFunction::formatted)
        .collect(java.util.stream.Collectors.joining(", "));
  }

  @Override
  public String getSearchStringEscape() {
    return ""; // TODO: what is this?
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return ""; // TODO: what is this?
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return false; // TODO: What is this?
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return false; // TODO: what is this?
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    return false;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return "";
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return "";
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return "";
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    return false;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    return "";
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    return false;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    return 0; // TODO: may this should be 1?
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return 0;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return 0;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    return null;
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
      String columnNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return null;
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    String catalog = conn.getCatalog();
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{
        catalog
    });
    return JParqUtil.listResultSet(new String[]{
        "TABLE_CAT"
    }, rows);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    return null;
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
      throws SQLException {
    return null;
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return null;
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {
    return null;
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
      String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    return null;
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    return null;
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    return false;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  // TODO: this should return an empty result set instead of null
  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
    return null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return conn;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return false; // TODO: i think this should be true
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
      String attributeNamePattern) throws SQLException {
    return null;
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return 0;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return getDriverMajorVersion();
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return getDriverMinorVersion();
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return 4;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return 3;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return sqlStateSQL;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    return null;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    return false;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    return null;
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
      String columnNamePattern) throws SQLException {
    return null;
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {
    return null;
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }
}
