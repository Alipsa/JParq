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
import java.sql.SQLException;
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
    assertFalse(metaData.nullsAreSortedHigh());
    assertTrue(metaData.nullsAreSortedLow());
    assertFalse(metaData.nullsAreSortedAtStart());
    assertTrue(metaData.nullsAreSortedAtEnd());
    assertEquals("\"", metaData.getIdentifierQuoteString());
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
    assertEquals("", metaData.getSearchStringEscape());
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
    assertFalse(metaData.supportsBatchUpdates());
    assertFalse(metaData.supportsSavepoints());
    assertFalse(metaData.supportsNamedParameters());
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
    assertEquals(0, metaData.getResultSetHoldability());
    assertFalse(metaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    assertFalse(metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
    assertFalse(metaData.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
    assertFalse(metaData.supportsTransactions());
    assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
    assertFalse(metaData.supportsDataDefinitionAndDataManipulationTransactions());
    assertFalse(metaData.supportsDataManipulationTransactionsOnly());
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
