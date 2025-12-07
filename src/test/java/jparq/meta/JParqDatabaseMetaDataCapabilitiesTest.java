package jparq.meta;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Additional coverage for {@link se.alipsa.jparq.JParqDatabaseMetaData}. */
class JParqDatabaseMetaDataCapabilitiesTest {

  private Connection connection;
  private String jdbcUrl;

  @BeforeEach
  void setUp() throws URISyntaxException, SQLException {
    URL acmesUrl = getClass().getResource("/acme");
    assertNotNull(acmesUrl, "acme dataset must be available on the classpath");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jdbcUrl = "jdbc:jparq:" + acmePath.toAbsolutePath();
    connection = DriverManager.getConnection(jdbcUrl);
  }

  @AfterEach
  void tearDown() throws SQLException {
    connection.close();
  }

  @Test
  void reportsSupportedCapabilitiesAndDefaults() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    assertEquals("JParq", metaData.getDatabaseProductName());
    assertEquals("1.1.0", metaData.getDatabaseProductVersion());
    assertEquals("se.alipsa.jparq.JParqDriver", metaData.getDriverName());
    assertTrue(metaData.usesLocalFiles());
    assertTrue(metaData.usesLocalFilePerTable());
    assertFalse(metaData.supportsMixedCaseIdentifiers());
    assertTrue(metaData.supportsMixedCaseQuotedIdentifiers());
    assertTrue(metaData.isReadOnly());
    assertFalse(metaData.allProceduresAreCallable());
    assertTrue(metaData.allTablesAreSelectable());
    var rs = metaData.getCatalogs();
    rs.next();
    assertEquals("acme", rs.getString("TABLE_CAT"));
    assertTrue(metaData.nullsAreSortedHigh());
    assertFalse(metaData.nullsAreSortedLow());
    assertFalse(metaData.nullsAreSortedAtStart());
    assertFalse(metaData.nullsAreSortedAtEnd());
    assertEquals("\"", metaData.getIdentifierQuoteString());
    assertTrue(metaData.storesMixedCaseIdentifiers());
    assertFalse(metaData.storesLowerCaseIdentifiers());
    assertTrue(metaData.storesMixedCaseQuotedIdentifiers());
    assertEquals(RowIdLifetime.ROWID_UNSUPPORTED, metaData.getRowIdLifetime());
    assertEquals("ILIKE, LIMIT, REGEXP_LIKE", metaData.getSQLKeywords());
    String expectedNumericFunctions = "{fn ABS}, {fn ACOS}, {fn ASIN}, {fn ATAN}, {fn ATAN2}, {fn CEILING}, "
        + "{fn COS}, {fn COT}, {fn DEGREES}, {fn EXP}, {fn FLOOR}, {fn LOG}, {fn LOG10}, {fn MOD}, {fn PI}, "
        + "{fn POWER}, {fn RADIANS}, {fn RAND}, {fn ROUND}, {fn SIGN}, {fn SIN}, {fn SQRT}, {fn TAN}, {fn TRUNCATE}";
    assertEquals(expectedNumericFunctions, metaData.getNumericFunctions());
    String expectedStringFunctions = "{fn ASCII}, {fn CHAR}, {fn CONCAT}, {fn DIFFERENCE}, {fn INSERT}, {fn LCASE}, "
        + "{fn LEFT}, {fn LENGTH}, {fn LOCATE}, {fn LTRIM}, {fn REPEAT}, {fn REPLACE}, {fn RIGHT}, {fn RTRIM}, "
        + "{fn SOUNDEX}, {fn SPACE}, {fn SUBSTRING}, {fn UCASE}";
    assertEquals(expectedStringFunctions, metaData.getStringFunctions());
    assertEquals("{fn DATABASE}, {fn IFNULL}, {fn USER}", metaData.getSystemFunctions());
    String expectedTimeDateFunctions = "{fn CURDATE}, {fn CURTIME}, {fn NOW}, {fn DAYOFWEEK}, {fn DAYOFMONTH}, "
        + "{fn DAYOFYEAR}, {fn HOUR}, {fn MINUTE}, {fn MONTH}, {fn QUARTER}, {fn SECOND}, {fn WEEK}, {fn YEAR}, "
        + "{fn TIMESTAMPADD}, {fn TIMESTAMPDIFF}";
    assertEquals(expectedTimeDateFunctions, metaData.getTimeDateFunctions());
    assertEquals("\\", metaData.getSearchStringEscape());
    assertEquals("", metaData.getExtraNameCharacters());
    assertTrue(metaData.supportsGroupBy());
    assertTrue(metaData.supportsUnion());
    assertTrue(metaData.supportsUnionAll());
    assertTrue(metaData.supportsOuterJoins());
    assertTrue(metaData.supportsFullOuterJoins());
    assertFalse(metaData.supportsStoredProcedures());
    assertTrue(metaData.supportsLikeEscapeClause());
    assertTrue(metaData.supportsExpressionsInOrderBy());
    assertTrue(metaData.supportsOrderByUnrelated());
    assertTrue(metaData.supportsConvert());
    assertTrue(metaData.supportsConvert(Types.VARCHAR, Types.INTEGER));
    assertFalse(metaData.supportsConvert(Types.BLOB, Types.INTEGER));
    assertFalse(metaData.supportsBatchUpdates());
    assertFalse(metaData.supportsSavepoints());
    assertTrue(metaData.supportsNamedParameters());
    assertEquals("", metaData.getCatalogSeparator());
    assertEquals("folder", metaData.getSchemaTerm());
    assertEquals("", metaData.getProcedureTerm());
    assertTrue(metaData.supportsSchemasInDataManipulation());
    assertFalse(metaData.supportsSchemasInProcedureCalls());
    assertFalse(metaData.supportsSchemasInTableDefinitions());
    assertFalse(metaData.supportsSchemasInIndexDefinitions());
    assertFalse(metaData.supportsSchemasInPrivilegeDefinitions());
    assertFalse(metaData.supportsCatalogsInDataManipulation());
    assertFalse(metaData.supportsCatalogsInProcedureCalls());
    assertFalse(metaData.supportsCatalogsInTableDefinitions());
    assertFalse(metaData.supportsCatalogsInIndexDefinitions());
    assertFalse(metaData.supportsCatalogsInPrivilegeDefinitions());
    assertTrue(metaData.supportsTableCorrelationNames());
    assertFalse(metaData.supportsDifferentTableCorrelationNames());
    assertTrue(metaData.supportsCorrelatedSubqueries());
    assertFalse(metaData.supportsGetGeneratedKeys());
    assertFalse(metaData.supportsMultipleResultSets());
    assertFalse(metaData.supportsMultipleTransactions());
    assertFalse(metaData.supportsIntegrityEnhancementFacility());
    assertFalse(metaData.supportsStoredFunctionsUsingCallSyntax());
    assertEquals(1, metaData.getDatabaseMajorVersion());
    assertEquals(1, metaData.getDatabaseMinorVersion());
    assertEquals(4, metaData.getJDBCMajorVersion());
    assertEquals(3, metaData.getJDBCMinorVersion());
    assertEquals(2, metaData.getSQLStateType());
    assertTrue(metaData.supportsNonNullableColumns());
    assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, metaData.getResultSetHoldability());
    assertTrue(metaData.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
    assertFalse(metaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    assertTrue(metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
    assertFalse(metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
    assertTrue(metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
    assertFalse(metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
    assertFalse(metaData.supportsTransactions());
    assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
    assertFalse(metaData.supportsDataDefinitionAndDataManipulationTransactions());
    assertFalse(metaData.supportsDataManipulationTransactionsOnly());
  }

  @Test
  void shouldExposeTableTypesAndTypeInfo() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet tableTypes = metaData.getTableTypes()) {
      ResultSetMetaData meta = tableTypes.getMetaData();
      assertEquals(1, meta.getColumnCount());
      assertTrue(tableTypes.next());
      assertEquals("TABLE", tableTypes.getString("TABLE_TYPE"));
      assertFalse(tableTypes.next());
    }
    try (ResultSet typeInfo = metaData.getTypeInfo()) {
      ResultSetMetaData meta = typeInfo.getMetaData();
      assertEquals(18, meta.getColumnCount());
      boolean sawVarchar = false;
      boolean sawInteger = false;
      boolean sawTimestamp = false;
      while (typeInfo.next()) {
        String typeName = typeInfo.getString("TYPE_NAME");
        if ("VARCHAR".equals(typeName)) {
          sawVarchar = true;
          assertEquals(Types.VARCHAR, typeInfo.getInt("DATA_TYPE"));
          assertEquals(DatabaseMetaData.typeNullable, typeInfo.getInt("NULLABLE"));
          assertEquals("length", typeInfo.getString("CREATE_PARAMS"));
          assertEquals("'", typeInfo.getString("LITERAL_PREFIX"));
          assertEquals("'", typeInfo.getString("LITERAL_SUFFIX"));
          Object caseSensitive = typeInfo.getObject("CASE_SENSITIVE");
          assertTrue(caseSensitive instanceof Boolean && (Boolean) caseSensitive);
          assertEquals(DatabaseMetaData.typeSearchable, typeInfo.getInt("SEARCHABLE"));
        } else if ("INTEGER".equals(typeName)) {
          sawInteger = true;
          assertEquals(Types.INTEGER, typeInfo.getInt("DATA_TYPE"));
          assertEquals(10, typeInfo.getInt("NUM_PREC_RADIX"));
        } else if ("TIMESTAMP".equals(typeName)) {
          sawTimestamp = true;
          assertEquals(Types.TIMESTAMP, typeInfo.getInt("DATA_TYPE"));
        }
      }
      assertTrue(sawVarchar, "VARCHAR type info should be present");
      assertTrue(sawInteger, "INTEGER type info should be present");
      assertTrue(sawTimestamp, "TIMESTAMP type info should be present");
    }
  }

  @Test
  void respectsCaseSensitivityFlag() throws SQLException {
    Properties props = new Properties();
    props.setProperty("caseSensitive", "true");
    try (Connection sensitiveConnection = DriverManager.getConnection(jdbcUrl + "?caseSensitive=true", props)) {
      DatabaseMetaData metaData = sensitiveConnection.getMetaData();
      assertTrue(metaData.supportsMixedCaseIdentifiers());
    }
  }

  @Test
  void unsupportedMetadataEndpointsReturnEmptyResultSets() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet privileges = metaData.getTablePrivileges(null, null, "employees")) {
      assertFalse(privileges.next());
      assertEquals(7, privileges.getMetaData().getColumnCount());
    }
    try (ResultSet pk = metaData.getPrimaryKeys(null, null, "employees")) {
      assertFalse(pk.next());
      assertEquals(6, pk.getMetaData().getColumnCount());
    }
    try (ResultSet indexes = metaData.getIndexInfo(null, null, "employees", false, false)) {
      assertFalse(indexes.next());
      assertEquals(13, indexes.getMetaData().getColumnCount());
    }
    try (ResultSet functions = metaData.getFunctions(null, null, "%")) {
      assertFalse(functions.next());
      assertEquals(6, functions.getMetaData().getColumnCount());
    }
    try (ResultSet clientInfo = metaData.getClientInfoProperties()) {
      assertFalse(clientInfo.next());
      assertEquals(4, clientInfo.getMetaData().getColumnCount());
    }
  }

  @Test
  void reportsMaximaAsZero() throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    assertEquals(0, metaData.getMaxBinaryLiteralLength());
    assertEquals(0, metaData.getMaxCharLiteralLength());
    assertEquals(0, metaData.getMaxColumnNameLength());
    assertEquals(0, metaData.getMaxColumnsInGroupBy());
    assertEquals(0, metaData.getMaxColumnsInIndex());
    assertEquals(0, metaData.getMaxColumnsInOrderBy());
    assertEquals(0, metaData.getMaxColumnsInSelect());
    assertEquals(0, metaData.getMaxColumnsInTable());
    assertEquals(0, metaData.getMaxConnections());
    assertEquals(0, metaData.getMaxCursorNameLength());
    assertEquals(0, metaData.getMaxIndexLength());
    assertEquals(0, metaData.getMaxSchemaNameLength());
    assertEquals(0, metaData.getMaxProcedureNameLength());
    assertEquals(0, metaData.getMaxCatalogNameLength());
    assertEquals(0, metaData.getMaxRowSize());
    assertEquals(0, metaData.getMaxStatementLength());
    assertEquals(0, metaData.getMaxStatements());
    assertEquals(0, metaData.getMaxTableNameLength());
    assertEquals(0, metaData.getMaxTablesInSelect());
    assertEquals(0, metaData.getMaxUserNameLength());
  }
}
