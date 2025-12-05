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
import java.util.Comparator;
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

  private static final Set<Integer> SUPPORTED_CONVERT_TYPES = Set.of(Types.BOOLEAN, Types.BIT, Types.CHAR,
      Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR, Types.CLOB, Types.NUMERIC,
      Types.DECIMAL, Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT, Types.REAL, Types.FLOAT, Types.DOUBLE,
      Types.DATE, Types.TIME, Types.TIMESTAMP);

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

  private static final List<TypeInfo> TYPE_INFO = buildTypeInfo();

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
    PatternSpec tablePattern = buildPattern(tableNamePattern, caseSensitive);
    PatternSpec schemaPatternSpec = buildPattern(schemaPattern, caseSensitive);
    Configuration conf = new Configuration(false);
    String effectiveCatalog = (catalog != null && !catalog.isBlank()) ? catalog : conn.getCatalog();
    for (JParqConnection.TableLocation location : conn.listTables()) {
      String schemaName = location.schemaName();
      if (schemaPatternSpec.regex() != null
          && !matchesRegex(schemaPatternSpec.regex(), schemaName, schemaPatternSpec.caseSensitive())) {
        continue;
      }
      String base = location.tableName();
      if (tablePattern.regex() != null && !matchesRegex(tablePattern.regex(), base, tablePattern.caseSensitive())) {
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
    PatternSpec columnPattern = buildPattern(columnNamePattern, caseSensitive);

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
            if (columnPattern.regex() != null
                && !matchesRegex(columnPattern.regex(), columnName, columnPattern.caseSensitive())) {
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

  private static List<TypeInfo> buildTypeInfo() {
    List<TypeInfo> types = new ArrayList<>();
    types.add(new TypeInfo("ARRAY", Types.ARRAY, null, null, null, null, typeNullable, false, typePredNone, false,
        false, false, "ARRAY", 0, 0, null, null, null));
    types.add(new TypeInfo("BIGINT", Types.BIGINT, 19, null, null, null, typeNullable, false, typeSearchable, false,
        false, false, "BIGINT", 0, 0, null, null, 10));
    types.add(new TypeInfo("BINARY", Types.BINARY, Integer.MAX_VALUE, null, null, "length", typeNullable, false,
        typeSearchable, false, false, false, "BINARY", 0, 0, null, null, null));
    types.add(new TypeInfo("BOOLEAN", Types.BOOLEAN, 1, null, null, null, typeNullable, false, typeSearchable, false,
        false, false, "BOOLEAN", 0, 0, null, null, 2));
    types.add(new TypeInfo("DATE", Types.DATE, 10, "'", "'", null, typeNullable, false, typeSearchable, false, false,
        false, "DATE", 0, 0, null, null, null));
    types.add(new TypeInfo("DECIMAL", Types.DECIMAL, 38, null, null, "precision, scale", typeNullable, false,
        typeSearchable, false, false, false, "DECIMAL", 0, 38, null, null, 10));
    types.add(new TypeInfo("DOUBLE", Types.DOUBLE, 15, null, null, null, typeNullable, false, typeSearchable, false,
        false, false, "DOUBLE", 0, 0, null, null, 10));
    types.add(new TypeInfo("INTEGER", Types.INTEGER, 10, null, null, null, typeNullable, false, typeSearchable, false,
        false, false, "INTEGER", 0, 0, null, null, 10));
    types.add(new TypeInfo("REAL", Types.REAL, 7, null, null, null, typeNullable, false, typeSearchable, false, false,
        false, "REAL", 0, 0, null, null, 10));
    types.add(new TypeInfo("SMALLINT", Types.SMALLINT, 5, null, null, null, typeNullable, false, typeSearchable, false,
        false, false, "SMALLINT", 0, 0, null, null, 10));
    types.add(new TypeInfo("STRUCT", Types.STRUCT, null, null, null, null, typeNullable, false, typePredNone, false,
        false, false, "STRUCT", 0, 0, null, null, null));
    types.add(new TypeInfo("TIME", Types.TIME, 15, "'", "'", null, typeNullable, false, typeSearchable, false, false,
        false, "TIME", 0, 6, null, null, null));
    types.add(new TypeInfo("TIMESTAMP", Types.TIMESTAMP, 26, "'", "'", null, typeNullable, false, typeSearchable, false,
        false, false, "TIMESTAMP", 0, 6, null, null, null));
    types.add(new TypeInfo("TINYINT", Types.TINYINT, 3, null, null, null, typeNullable, false, typeSearchable, false,
        false, false, "TINYINT", 0, 0, null, null, 10));
    types.add(new TypeInfo("VARCHAR", Types.VARCHAR, Integer.MAX_VALUE, "'", "'", "length", typeNullable, true,
        typeSearchable, false, false, false, "VARCHAR", 0, 0, null, null, null));
    types.sort(Comparator.comparingInt(TypeInfo::dataType).thenComparing(TypeInfo::typeName));
    return List.copyOf(types);
  }

  private record TypeInfo(String typeName, int dataType, Integer precision, String literalPrefix, String literalSuffix,
      String createParams, int nullable, boolean caseSensitive, int searchable, boolean unsignedAttribute,
      boolean fixedPrecScale, boolean autoIncrement, String localTypeName, Integer minimumScale, Integer maximumScale,
      Integer sqlDataType, Integer sqlDatetimeSub, Integer numPrecRadix) {

    Object[] toRow() {
      return new Object[]{
          typeName, dataType, precision, literalPrefix, literalSuffix, createParams, nullable, caseSensitive,
          searchable, unsignedAttribute, fixedPrecScale, autoIncrement, localTypeName, minimumScale, maximumScale,
          sqlDataType, sqlDatetimeSub, numPrecRadix
      };
    }
  }

  /**
   * Build an empty metadata {@link ResultSet} exposing the supplied headers.
   *
   * @param headers
   *          column labels to include in the result set
   * @return a {@link ResultSet} with no rows
   */
  private ResultSet emptyResultSet(String... headers) {
    return JParqUtil.listResultSet(headers, List.<Object[]>of());
  }

  private PatternSpec buildPattern(String pattern, boolean caseSensitive) {
    if (pattern == null) {
      return new PatternSpec(null, caseSensitive);
    }
    boolean quoted = pattern.length() > 1 && pattern.startsWith("\"") && pattern.endsWith("\"");
    String body = quoted ? pattern.substring(1, pattern.length() - 1) : pattern;
    boolean effectiveCase = caseSensitive || quoted;
    String normalized = effectiveCase ? body : body.toLowerCase(Locale.ROOT);
    String regex = se.alipsa.jparq.engine.function.StringFunctions.toLikeRegex(normalized, '\\');
    return new PatternSpec(regex, effectiveCase);
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

  private record PatternSpec(String regex, boolean caseSensitive) {
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
    return true;
  }

  @Override
  public boolean nullsAreSortedLow() {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() {
    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() {
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() {
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() {
    return true;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() {
    return conn.isCaseSensitive();
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() {
    return true;
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
    return true;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return "\"";
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
    return "\\";
  }

  /**
   * allows a driver to declare which special characters (symbols) can be used in
   * unquoted identifier names, beyond the standard SQL set.
   *
   * @return an empty string, only standard identifiers allowed
   */
  @Override
  public String getExtraNameCharacters() {
    return "";
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return false; // only read operations supported
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false; // only read operations supported
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
  public boolean supportsConvert() {
    return true;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) {
    return SUPPORTED_CONVERT_TYPES.contains(fromType) && SUPPORTED_CONVERT_TYPES.contains(toType);
  }

  /**
   * Tells if the database supports assigning a temporary alias (a nickname) to a
   * table within a specific SQL query. This "correlation name" is better known to
   * most developers simply as a Table Alias.
   *
   * @return true since this is supported
   */
  @Override
  public boolean supportsTableCorrelationNames() {
    return true;
  }

  /**
   * Is the user forbidden from using an alias (correlation name) that is
   * identical to the table name?
   *
   * @return false, since there is no such restriction
   */
  @Override
  public boolean supportsDifferentTableCorrelationNames() {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() {
    return true;
    // This is supported but has some restrictions that are addressed in
    // expressionsInOrderBy.md
  }

  /**
   * Whether JParq supports ordering by columns not present in the SELECT list,
   * e.g: <code>
   * SELECT model FROM mtcars ORDER BY mpg
   * </code>
   *
   * @return true since this is fully supported
   */
  @Override
  public boolean supportsOrderByUnrelated() {
    return true;
  }

  @Override
  public boolean supportsGroupBy() {
    return true;
  }

  /**
   * Whether JParq supports grouping by fields that are not included in the SELECT
   * list.
   *
   * @return true since this is supported
   */
  @Override
  public boolean supportsGroupByUnrelated() {
    return true;
  }

  /**
   * Whether JParq allows sorting by a grouping column that is not in the SELECT
   * list
   *
   * @return false since this is currently NOT supported
   */
  @Override
  public boolean supportsGroupByBeyondSelect() {
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() {
    return true;
  }

  @Override
  public boolean supportsMultipleResultSets() {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() {
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    return false; // only read operations supported
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    return false; // only read operations supported
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    return false; // only read operations supported
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    return false; // only read operations supported
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    return false; // only read operations supported
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    return false; // only read operations supported
  }

  /**
   * Indicates whether the database supports the Integrity Enhancement Facility
   * (IEF), a specific subset of the SQL-89 standard. i.e. Does the database
   * support Primary Keys, Foreign Keys, and CHECK constraints?
   *
   * @return false since this it is impossible to enforce in externally created
   *         data (parquet files)
   */
  @Override
  public boolean supportsIntegrityEnhancementFacility() {
    return false;
  }

  @Override
  public boolean supportsOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() {
    return true;
  }

  @Override
  public String getSchemaTerm() {
    return "folder";
  }

  /**
   * Stored procedures are not supported.
   *
   * @return an empty string
   */
  @Override
  public String getProcedureTerm() {
    return "";
  }

  @Override
  public String getCatalogTerm() {
    return "database";
  }

  @Override
  public boolean isCatalogAtStart() {
    return true;
  }

  /**
   * The catalog name is part of the connection context but never written in the
   * SQL itself.
   *
   * @return an empty string
   */
  @Override
  public String getCatalogSeparator() {
    return "";
  }

  /**
   * This is supported: <code>SELECT * FROM my_schema.my_table</code>
   *
   * @return true since this is supported
   */
  @Override
  public boolean supportsSchemasInDataManipulation() {
    return true;
  }

  /**
   * Stored procedures are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsSchemasInProcedureCalls() {
    return false;
  }

  /**
   * DDL's are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsSchemasInTableDefinitions() {
    return false;
  }

  /**
   * Indexing are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsSchemasInIndexDefinitions() {
    return false;
  }

  /**
   * Privilege definitions are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() {
    return false;
  }

  /**
   * The catalog name is part of the connection context but never written in the
   * SQL itself.
   *
   * @return false
   */
  @Override
  public boolean supportsCatalogsInDataManipulation() {
    return false;
  }

  /**
   * Stored procedures are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsCatalogsInProcedureCalls() {
    return false;
  }

  /**
   * DDL's are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsCatalogsInTableDefinitions() {
    return false;
  }

  /**
   * Indexing are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsCatalogsInIndexDefinitions() {
    return false;
  }

  /**
   * Privilege definitions are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() {
    return false;
  }

  /**
   * Deletes are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsPositionedDelete() {
    return false;
  }

  /**
   * Updates are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsPositionedUpdate() {
    return false;
  }

  /**
   * Updates are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsSelectForUpdate() {
    return false;
  }

  /**
   * Stored procedures are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsStoredProcedures() {
    return false;
  }

  /**
   * Whether JParq supports SQL syntax where a value is compared against the
   * result of a SELECT statement. E.g:
   * <code>SELECT * FROM t WHERE id = (SELECT id FROM t2 WHERE ...)</code>
   *
   * @return true
   */
  @Override
  public boolean supportsSubqueriesInComparisons() {
    return true;
  }

  /**
   * Whether JParq supports SQL syntax where a value is compared against the
   * result of an EXISTS statement.
   *
   * @return true
   */
  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return true;
  }

  /**
   * Whether JParq supports SQL syntax where a value is compared against the
   * result of an IN statement
   *
   * @return true
   */
  @Override
  public boolean supportsSubqueriesInIns() {
    return true;
  }

  /**
   * indicates whether your database supports Quantified Comparison Operators.
   *
   * @return true
   */
  @Override
  public boolean supportsSubqueriesInQuantifieds() {
    return true;
  }

  /**
   * The JParq codebase has explicit and robust support for correlated subqueries
   *
   * @return true
   */
  @Override
  public boolean supportsCorrelatedSubqueries() {
    return true;
  }

  /**
   * Whether JParq supports union statements.
   *
   * @return true
   */
  @Override
  public boolean supportsUnion() {
    return true;
  }

  /**
   * Whether JParq supports union all statements.
   *
   * @return true
   */
  @Override
  public boolean supportsUnionAll() throws SQLException {
    return true;
  }

  /**
   * Transactions are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsOpenCursorsAcrossCommit() {
    return false;
  }

  /**
   * Transactions are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsOpenCursorsAcrossRollback() {
    return false;
  }

  /**
   * Transactions are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsOpenStatementsAcrossCommit() {
    return false;
  }

  /**
   * Transactions are not supported
   *
   * @return false
   */
  @Override
  public boolean supportsOpenStatementsAcrossRollback() {
    return false;
  }

  /**
   * No limit on binary length
   *
   * @return 0
   */
  @Override
  public int getMaxBinaryLiteralLength() {
    return 0;
  }

  /**
   * No limit on char literal length
   *
   * @return 0
   */
  @Override
  public int getMaxCharLiteralLength() {
    return 0;
  }

  /**
   * No limit on column name length
   *
   * @return 0
   */
  @Override
  public int getMaxColumnNameLength() {
    return 0;
  }

  /**
   * No limit on columns in group by clause
   *
   * @return 0
   */
  @Override
  public int getMaxColumnsInGroupBy() {
    return 0;
  }

  /**
   * Indexes are not supported
   *
   * @return 0
   */
  @Override
  public int getMaxColumnsInIndex() {
    return 0;
  }

  /**
   * No limit to number of columns in the order by.
   *
   * @return 0
   */
  @Override
  public int getMaxColumnsInOrderBy() {
    return 0;
  }

  /**
   * No limit to the max number of columns in the select clause.
   *
   * @return 0
   */
  @Override
  public int getMaxColumnsInSelect() {
    return 0;
  }

  /**
   * No limit to the max number of columns in the table.
   *
   * @return 0
   */
  @Override
  public int getMaxColumnsInTable() {
    return 0;
  }

  /**
   * Jparq supports multiple concurrent connections. The only limit is the
   * available memory.
   *
   * @return 0
   */
  @Override
  public int getMaxConnections() {
    return 0;
  }

  /**
   * Cursors are not supported.
   *
   * @return 0
   */
  @Override
  public int getMaxCursorNameLength() {
    return 0;
  }

  /**
   * Indexes a re not supported.
   *
   * @return 0
   */
  @Override
  public int getMaxIndexLength() {
    return 0;
  }

  /**
   * A schema in Jparq corresponds to a subdirectory under the base dir. The limit
   * depends on the filesystem and os limitations for directory length.
   *
   * @return 0
   */
  @Override
  public int getMaxSchemaNameLength() {
    return 0;
  }

  /**
   * Stored procedures are not supported.
   *
   * @return 0
   */
  @Override
  public int getMaxProcedureNameLength() {
    return 0;
  }

  /**
   * The catalog corresponds to the base dir. The limit depends on the filesystem
   * and os limitations for directory length.
   * 
   * @return 0
   */
  @Override
  public int getMaxCatalogNameLength() {
    return 0;
  }

  /**
   * Since Parquet/Avro row sizes are limited only by memory and configuration
   * there is no limit.
   *
   * @return 0
   */
  @Override
  public int getMaxRowSize() {
    return 0;
  }

  /**
   * Since Parquet/Avro row sizes are limited only by memory and configuration
   * there is no limit.
   *
   * @return true
   */
  @Override
  public boolean doesMaxRowSizeIncludeBlobs() {
    return true;
  }

  /**
   * Statement length is only restricted by available memory.
   *
   * @return 0
   */
  @Override
  public int getMaxStatementLength() {
    return 0;
  }

  /**
   * Multiple `Statement` Objects: You can create multiple Statement or
   * PreparedStatement objects from a single JParqConnection. Each of these
   * statement objects can be used to execute queries independently. The
   * JParqConnection.createStatement() method returns a new JParqStatement
   * instance each time it is called.
   *
   * <p>
   * Sequential Queries on a Single `Statement`: A single Statement object can be
   * used to execute multiple queries sequentially. However, you should close the
   * ResultSet from the previous query before executing the next one, which is
   * standard JDBC practice.
   *
   * <p>
   * It is important to note that JParq does not support executing a string
   * containing multiple SQL statements in a single executeQuery() call (e.g.,
   * <code>SELECT  FROM table1; SELECT  FROM table2;</code>). The batch execution
   * methods (addBatch(), executeBatch()) are also not supported.
   *
   * @return 0 since there is no limit to the number of statements that can be
   *         open at the same time
   */
  @Override
  public int getMaxStatements() {
    return 0;
  }

  /**
   * A table in JParq corresponds to the file name of the parquet file. The limit
   * depends on whether the underlying filesyste/os imposes a limit or not.
   *
   * @return 0 since there is no limit, or it is unknown.
   */
  @Override
  public int getMaxTableNameLength() {
    return 0;
  }

  /**
   * There is no limit imposed on the number of tables in a select. In practice
   * the limit is contained to the number of files that can exists in a directory
   * i.e. it depends on you filesystem/os.
   *
   * @return 0
   */
  @Override
  public int getMaxTablesInSelect() {
    return 0;
  }

  /**
   * In JParq the user is the same as the username logged in i.e.
   * System.getProperty("user.name") thus the max length is os/platform specific.
   *
   * @return 0
   */
  @Override
  public int getMaxUserNameLength() {
    return 0;
  }

  /**
   * Transactions are not supported.
   *
   * @return 0
   */
  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  /**
   * Transactions are not supported.
   *
   * @return false
   */
  @Override
  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  /**
   * Transactions are not supported.
   *
   * @return false
   */
  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return false;
  }

  /**
   * Transactions are not supported.
   *
   * @return false
   */
  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return false;
  }

  /**
   * Transactions are not supported.
   *
   * @return false
   */
  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    return false;
  }

  /**
   * Transactions are not supported.
   *
   * @return false
   */
  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    return false;
  }

  /**
   * Transactions are not supported.
   *
   * @return false
   */
  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    return false;
  }

  /**
   * Stored procedures are not supported.
   *
   * @return false
   */
  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
    return JParqUtil.listResultSet(List.of("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "reserved1",
        "reserved2", "reserved3", "REMARKS", "PROCEDURE_TYPE", "SPECIFIC_NAME"), List.of());
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
      String columnNamePattern) throws SQLException {
    return JParqUtil.listResultSet(List.of("PROCEDURE_CAT", // 1. Catalog
        "PROCEDURE_SCHEM", // 2. Schema
        "PROCEDURE_NAME", // 3. Procedure Name
        "COLUMN_NAME", // 4. Column/Parameter Name
        "COLUMN_TYPE", // 5. Kind of column (IN, OUT, INOUT, etc.)
        "DATA_TYPE", // 6. SQL type from java.sql.Types
        "TYPE_NAME", // 7. SQL type name
        "PRECISION", // 8. Precision
        "LENGTH", // 9. Byte length
        "SCALE", // 10. Scale
        "RADIX", // 11. Radix
        "NULLABLE", // 12. Can it contain Null?
        "REMARKS", // 13. Comment
        "COLUMN_DEF", // 14. Default value
        "SQL_DATA_TYPE", // 15. Reserved for future use
        "SQL_DATETIME_SUB", // 16. Reserved for future use
        "CHAR_OCTET_LENGTH", // 17. max bytes for char types
        "ORDINAL_POSITION", // 18. The order (1, 2, 3...)
        "IS_NULLABLE", // 19. "YES" or "NO"
        "SPECIFIC_NAME" // 20. Specific name
    ), List.of());
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    List<Object[]> rows = new ArrayList<>();
    String catalog = conn.getCatalog();
    for (String schema : conn.listSchemas()) {
      rows.add(new Object[]{
          formatSchemaName(schema), catalog
      });
    }
    return JParqUtil.listResultSet(new String[]{
        "TABLE_SCHEM", "TABLE_CATALOG"
    }, rows);
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    String actualCatalog = conn.getCatalog();
    String effectiveCatalog = (catalog != null && !catalog.isBlank()) ? catalog : actualCatalog;
    if (!effectiveCatalog.equals(actualCatalog)) {
      return JParqUtil.listResultSet(new String[]{
          "TABLE_SCHEM", "TABLE_CATALOG"
      }, List.of());
    }
    List<Object[]> rows = new ArrayList<>();
    boolean caseSensitive = conn.isCaseSensitive();
    PatternSpec schemaPatternSpec = buildPattern(schemaPattern, caseSensitive);
    for (String schema : conn.listSchemas()) {
      if (schemaPatternSpec.regex() != null
          && !matchesRegex(schemaPatternSpec.regex(), schema, schemaPatternSpec.caseSensitive())) {
        continue;
      }
      rows.add(new Object[]{
          formatSchemaName(schema), actualCatalog
      });
    }
    return JParqUtil.listResultSet(new String[]{
        "TABLE_SCHEM", "TABLE_CATALOG"
    }, rows);
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

  /**
   * Report the table types supported by the driver; JParq only exposes Parquet
   * files as standard tables.
   *
   * @return a result set containing a single {@code TABLE} row
   * @throws SQLException
   *           if the in-memory result set cannot be created
   */
  @Override
  public ResultSet getTableTypes() throws SQLException {
    return JParqUtil.listResultSet(new String[]{
        "TABLE_TYPE"
    }, List.<Object[]>of(new Object[]{
        "TABLE"
    }));
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
      throws SQLException {
    return emptyResultSet("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE",
        "IS_GRANTABLE");
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return emptyResultSet("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "GRANTOR", "GRANTEE", "PRIVILEGE", "IS_GRANTABLE");
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {
    return emptyResultSet("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
        "DECIMAL_DIGITS", "PSEUDO_COLUMN");
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    return emptyResultSet("SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
        "DECIMAL_DIGITS", "PSEUDO_COLUMN");
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    return emptyResultSet("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME");
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    return emptyResultSet("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
        "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
        "DEFERRABILITY");
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    return emptyResultSet("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
        "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
        "DEFERRABILITY");
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
      String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    return emptyResultSet("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME", "FKTABLE_CAT",
        "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME",
        "DEFERRABILITY");
  }

  /**
   * Describe supported JDBC types for the Parquet-backed driver.
   *
   * @return a {@link ResultSet} adhering to the JDBC type info contract
   * @throws SQLException
   *           if the in-memory result set cannot be created
   */
  @Override
  public ResultSet getTypeInfo() throws SQLException {
    List<Object[]> rows = new ArrayList<>();
    for (TypeInfo typeInfo : TYPE_INFO) {
      rows.add(typeInfo.toRow());
    }
    return JParqUtil.listResultSet(new String[]{
        "TYPE_NAME", "DATA_TYPE", "PRECISION", "LITERAL_PREFIX", "LITERAL_SUFFIX", "CREATE_PARAMS", "NULLABLE",
        "CASE_SENSITIVE", "SEARCHABLE", "UNSIGNED_ATTRIBUTE", "FIXED_PREC_SCALE", "AUTO_INCREMENT", "LOCAL_TYPE_NAME",
        "MINIMUM_SCALE", "MAXIMUM_SCALE", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "NUM_PREC_RADIX"
    }, rows);
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    return emptyResultSet("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME",
        "TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC", "CARDINALITY", "PAGES", "FILTER_CONDITION");
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    return type == ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  /**
   * Updates are not supported.
   *
   * @param type
   *          the {@code ResultSet} type; one of
   *          {@code ResultSet.TYPE_FORWARD_ONLY},
   *          {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *          {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return false
   */
  @Override
  public boolean ownUpdatesAreVisible(int type) {
    return false;
  }

  /**
   * Deletes are not supported.
   *
   * @param type
   *          the {@code ResultSet} type; one of
   *          {@code ResultSet.TYPE_FORWARD_ONLY},
   *          {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *          {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return false
   */
  @Override
  public boolean ownDeletesAreVisible(int type) {
    return false;
  }

  /**
   * Inserts are not supported.
   *
   * @param type
   *          the {@code ResultSet} type; one of
   *          {@code ResultSet.TYPE_FORWARD_ONLY},
   *          {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *          {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return false
   */
  @Override
  public boolean ownInsertsAreVisible(int type) {
    return false;
  }

  /**
   * Updates are not supported.
   *
   * @param type
   *          the {@code ResultSet} type; one of
   *          {@code ResultSet.TYPE_FORWARD_ONLY},
   *          {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *          {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return false
   */
  @Override
  public boolean othersUpdatesAreVisible(int type) {
    return false;
  }

  /**
   * Deletes are not supported.
   *
   * @param type
   *          the {@code ResultSet} type; one of
   *          {@code ResultSet.TYPE_FORWARD_ONLY},
   *          {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *          {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return false
   */
  @Override
  public boolean othersDeletesAreVisible(int type) {
    return false;
  }

  /**
   * Inserts are not supported.
   *
   * @param type
   *          the {@code ResultSet} type; one of
   *          {@code ResultSet.TYPE_FORWARD_ONLY},
   *          {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *          {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return false
   */
  @Override
  public boolean othersInsertsAreVisible(int type) {
    return false;
  }

  /**
   * Updates are not supported.
   *
   * @param type
   *          the {@code ResultSet} type; one of
   *          {@code ResultSet.TYPE_FORWARD_ONLY},
   *          {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *          {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return false
   */
  @Override
  public boolean updatesAreDetected(int type) {
    return false;
  }

  /**
   * Deletes are not supported.
   *
   * @param type
   *          the {@code ResultSet} type; one of
   *          {@code ResultSet.TYPE_FORWARD_ONLY},
   *          {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *          {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return false
   */
  @Override
  public boolean deletesAreDetected(int type) {
    return false;
  }

  /**
   * Inserts are not supported.
   *
   * @param type
   *          the {@code ResultSet} type; one of
   *          {@code ResultSet.TYPE_FORWARD_ONLY},
   *          {@code ResultSet.TYPE_SCROLL_INSENSITIVE}, or
   *          {@code ResultSet.TYPE_SCROLL_SENSITIVE}
   * @return false
   */
  @Override
  public boolean insertsAreDetected(int type) {
    return false;
  }

  /**
   * Batch updates are not supported.
   *
   * @return false
   */
  @Override
  public boolean supportsBatchUpdates() {
    return false;
  }

  /**
   * User-defined types (UDTs) are not supported so this will always return an
   * empty result set.
   *
   * @param catalog
   *          a catalog name; must match the catalog name as it is stored in the
   *          database; "" retrieves those without a catalog; {@code null} means
   *          that the catalog name should not be used to narrow the search
   * @param schemaPattern
   *          a schema pattern name; must match the schema name as it is stored in
   *          the database; "" retrieves those without a schema; {@code null}
   *          means that the schema name should not be used to narrow the search
   * @param typeNamePattern
   *          a type name pattern; must match the type name as it is stored in the
   *          database; may be a fully qualified name
   * @param types
   *          a list of user-defined types (JAVA_OBJECT, STRUCT, or DISTINCT) to
   *          include; {@code null} returns all types
   * @return an empty result set.
   */
  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
    return JParqUtil.listResultSet(
        List.of("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE"), List.of());
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
    return false; // Named parameters are not supported by this driver.
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
    return emptyResultSet("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME");
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    return emptyResultSet("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME");
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
      String attributeNamePattern) throws SQLException {
    return emptyResultSet("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ATTR_NAME", "DATA_TYPE", "ATTR_TYPE_NAME",
        "ATTR_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "ATTR_DEF", "SQL_DATA_TYPE",
        "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA",
        "SCOPE_TABLE", "SOURCE_DATA_TYPE");
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
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
    return RowIdLifetime.ROWID_UNSUPPORTED;
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
    return emptyResultSet("NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION");
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
    return emptyResultSet("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE",
        "SPECIFIC_NAME");
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
      String columnNamePattern) throws SQLException {
    return emptyResultSet("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE",
        "TYPE_NAME", "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH",
        "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME");
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern) throws SQLException {
    return emptyResultSet("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "COLUMN_SIZE",
        "DECIMAL_DIGITS", "NUM_PREC_RADIX", "COLUMN_USAGE", "REMARKS", "CHAR_OCTET_LENGTH", "IS_NULLABLE");
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw new SQLException("No wrapper for " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }
}
