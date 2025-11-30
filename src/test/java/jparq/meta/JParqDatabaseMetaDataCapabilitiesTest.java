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
import se.alipsa.jparq.JParqDatabaseMetaData;

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
    assertFalse(metaData.supportsMixedCaseQuotedIdentifiers());
    assertTrue(metaData.isReadOnly());
    assertFalse(metaData.allProceduresAreCallable());
    assertTrue(metaData.allTablesAreSelectable());
    assertEquals("acme", ((JParqDatabaseMetaData) metaData).getDatabaseName());
    assertFalse(metaData.nullsAreSortedHigh());
    assertTrue(metaData.nullsAreSortedLow());
    assertFalse(metaData.nullsAreSortedAtStart());
    assertTrue(metaData.nullsAreSortedAtEnd());
    assertEquals("", metaData.getIdentifierQuoteString());
    assertEquals("", metaData.getSQLKeywords());
    assertTrue(metaData.getNumericFunctions().startsWith("ABS"));
    assertEquals(String.join(",", JParqDatabaseMetaData.SUPPORTED_STRING_FUNCTIONS), metaData.getStringFunctions());
    assertEquals("", metaData.getSystemFunctions());
    assertEquals("", metaData.getTimeDateFunctions());
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
    assertFalse(metaData.supportsOrderByUnrelated());
    assertFalse(metaData.supportsBatchUpdates());
    assertFalse(metaData.supportsSavepoints());
    assertFalse(metaData.supportsNamedParameters());
    assertEquals("", metaData.getCatalogSeparator());
    assertEquals("", metaData.getSchemaTerm());
    assertEquals("", metaData.getProcedureTerm());
    assertFalse(metaData.supportsSchemasInDataManipulation());
    assertFalse(metaData.supportsSchemasInProcedureCalls());
    assertFalse(metaData.supportsSchemasInTableDefinitions());
    assertFalse(metaData.supportsSchemasInIndexDefinitions());
    assertFalse(metaData.supportsSchemasInPrivilegeDefinitions());
    assertFalse(metaData.supportsCatalogsInDataManipulation());
    assertFalse(metaData.supportsCatalogsInProcedureCalls());
    assertFalse(metaData.supportsCatalogsInTableDefinitions());
    assertFalse(metaData.supportsCatalogsInIndexDefinitions());
    assertFalse(metaData.supportsCatalogsInPrivilegeDefinitions());
    assertFalse(metaData.supportsTableCorrelationNames());
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
