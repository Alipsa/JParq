package se.alipsa.jparq;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import se.alipsa.jparq.helper.JParqUtil;

/**
 * An implementation of the java.sql.DatabaseMetaData interface for parquet
 * files.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class JParqDatabaseMetaData implements DatabaseMetaData {
  private static final String SUPPORTED_NUMERIC_FUNCTIONS = String.join(",",
      List.of("ABS", "CEIL", "CEILING", "FLOOR", "ROUND", "SQRT", "TRUNC", "TRUNCATE", "MOD", "POWER", "POW", "EXP",
          "LOG", "LOG10", "RAND", "RANDOM", "SIGN", "SIN", "COS", "TAN", "ASIN", "ACOS", "ATAN", "ATAN2", "DEGREES",
          "RADIANS"));
  private final JParqConnection conn;

  /**
   * Constructor for JParqDatabaseMetaData.
   *
   * @param conn
   *          the JParqConnection
   */
  public JParqDatabaseMetaData(JParqConnection conn) {
    this.conn = conn;
  }

  @Override
  public String getDatabaseProductName() {
    return "JParq";
  }

  @Override
  public String getDatabaseProductVersion() {
    return "0.10.0";
  }

  @Override
  public String getDriverName() {
    return "JParq JDBC";
  }

  @Override
  public String getDriverVersion() {
    return "0.10.0";
  }

  @Override
  public int getDriverMajorVersion() {
    return 0;
  }

  @Override
  public int getDriverMinorVersion() {
    return 10;
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

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) {
    File dir = conn.getBaseDir();
    File[] files = dir.listFiles((d, n) -> n.toLowerCase(Locale.ROOT).endsWith(".parquet"));
    List<Object[]> rows = new ArrayList<>();
    if (files != null) {
      for (File f : files) {
        String base = f.getName();
        int dot = base.lastIndexOf('.');
        if (dot > 0) {
          base = base.substring(0, dot);
        }
        if (tableNamePattern == null || base.matches(JParqUtil.sqlLikeToRegex(tableNamePattern))) {
          rows.add(new Object[]{
              catalog, schemaPattern, base, "TABLE"
          });
        }
      }
    }
    return JParqUtil.listResultSet(new String[]{
        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE"
    }, rows);
  }

  @SuppressWarnings("PMD.AvoidInstantiatingObjectsInLoops")
  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {

    List<Object[]> rows = new ArrayList<>();
    // Reuse one Configuration
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(false);

    try (ResultSet tables = getTables(catalog, schemaPattern, tableNamePattern, new String[]{
        "TABLE"
    })) {
      org.apache.hadoop.fs.Path hPath;
      while (tables.next()) {
        String table = tables.getString("TABLE_NAME");
        File file = conn.tableFile(table);

        hPath = new org.apache.hadoop.fs.Path(file.toURI());
        try (ParquetReader<GenericRecord> reader = AvroParquetReader
            .<GenericRecord>builder(HadoopInputFile.fromPath(hPath, conf)).build()) {

          GenericRecord rec = reader.read();
          if (rec == null) {
            continue; // empty file
          }

          int pos = 1;
          for (org.apache.avro.Schema.Field f : rec.getSchema().getFields()) {
            String col = f.name();
            if (columnNamePattern != null && !col.matches(JParqUtil.sqlLikeToRegex(columnNamePattern))) {
              continue;
            }
            rows.add(new Object[]{
                catalog, // TABLE_CAT
                schemaPattern, // TABLE_SCHEM
                table, // TABLE_NAME
                col, // COLUMN_NAME
                java.sql.Types.VARCHAR, // DATA_TYPE (simplified for now)
                "VARCHAR", // TYPE_NAME
                pos++, // ORDINAL_POSITION
                0, // COLUMN_SIZE (unknown)
                null, // BUFFER_LENGTH (unused)
                null // DECIMAL_DIGITS (unknown)
            });
          }
        } catch (Exception e) {
          throw new SQLException(e);
        }
      }
    } catch (SQLException e) {
      // Preserve original behavior surface (wrap/propagate)
      throw e;
    }

    return JParqUtil.listResultSet(new String[]{
        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "ORDINAL_POSITION",
        "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS"
    }, rows);
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
    return null;
  }

  @Override
  public String getUserName() {
    return null;
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
  public String getSQLKeywords() throws SQLException {
    return "";
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return SUPPORTED_NUMERIC_FUNCTIONS;
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return "";
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    return "";
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return "";
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return "";
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return "";
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
    return false;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    return false;
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
    return false;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    return false;
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
    return false;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    return false;
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
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    return false;
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
    return 0;
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
    return null;
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

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    return null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return null;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    return false;
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
    return 0;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return 0;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return 0;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    return 0;
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
