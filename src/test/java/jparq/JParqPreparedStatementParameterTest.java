package jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.JParqConnection;
import se.alipsa.jparq.JParqPreparedStatement;

/**
 * Verifies parameter binding and SQL injection protection for prepared
 * statements.
 */
class JParqPreparedStatementParameterTest {

  /**
   * Ignores parameter markers and comment delimiters inside quoted literals.
   *
   * @param literal
   *          a literal containing parameter-like text
   * @throws SQLException
   *           if preparation, binding, or execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "'? :ignored -- /*'", "'a''? :ignored -- /*'", "$$? :ignored -- /*$$", "$$'\"`? :ignored$$"
  })
  void bindsParametersAroundQuotedLiterals(String literal) throws SQLException {
    String sql = "SELECT :first AS first_value, " + literal + " AS text_value, ? AS last_value "
        + "FROM mtcars /* ' \" ` $$ ? :ignored */ LIMIT 1 -- ' \" ` $$ ? :ignored";
    try (JParqPreparedStatement ps = connection.prepareStatement(sql)) {
      assertEquals(2, ps.getParameterMetaData().getParameterCount());
      ps.setInt("first", 7);
      ps.setInt(2, 9);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(7, rs.getInt(1));
        String expected = literal.startsWith("$$")
            ? literal.substring(2, literal.length() - 2)
            : literal.substring(1, literal.length() - 1).replace("''", "'");
        assertEquals(expected, rs.getString(2));
        assertEquals(9, rs.getInt(3));
        assertFalse(rs.next());
      }
    }
  }

  /**
   * Preserves quoted alias text while binding positional and named parameters.
   *
   * @param alias
   *          a delimited alias, possibly containing a doubled delimiter
   * @throws SQLException
   *           if preparation, binding, or execution fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "\"? :ignored -- /*\"", "\"a\"\"? :ignored$$\"", "`? :ignored -- /*`", "`a``? :ignored$$`"
  })
  void bindsParametersAroundQuotedIdentifiers(String alias) throws SQLException {
    String sql = "SELECT ? AS " + alias + ", :last AS last_value FROM mtcars LIMIT 1";
    try (JParqPreparedStatement ps = connection.prepareStatement(sql)) {
      assertEquals(2, ps.getParameterMetaData().getParameterCount());
      ps.setInt(1, 7);
      ps.setInt("last", 9);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        String delimiter = alias.substring(0, 1);
        assertEquals(alias.substring(1, alias.length() - 1).replace(delimiter + delimiter, delimiter),
            rs.getMetaData().getColumnLabel(1));
        assertEquals(7, rs.getInt(1));
        assertEquals(9, rs.getInt(2));
        assertFalse(rs.next());
      }
    }
  }

  /**
   * Translates shared quote-scanner errors to JDBC syntax exceptions.
   *
   * @param quote
   *          the unmatched opening delimiter
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "'", "\"", "`", "$$"
  })
  void rejectsUnterminatedQuotedRegions(String quote) {
    var exception = assertThrows(java.sql.SQLSyntaxErrorException.class,
        () -> connection.prepareStatement("SELECT ? AS value, " + quote + "unfinished"));
    assertEquals("42000", exception.getSQLState());
    assertInstanceOf(IllegalArgumentException.class, exception.getCause());
  }

  @TempDir
  File tempDir;

  private JParqConnection connection;

  @BeforeEach
  void setUp() throws SQLException, IOException {
    File parquetFile = new File(tempDir, "mtcars.parquet");
    try (var in = getClass().getResourceAsStream("/datasets/mtcars.parquet")) {
      Files.copy(in, parquetFile.toPath());
    }
    connection = new JParqConnection("jdbc:jparq:" + tempDir.getAbsolutePath(), new Properties());
  }

  @AfterEach
  void tearDown() throws SQLException {
    connection.close();
  }

  @Test
  void bindsParametersAndFilters() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE cyl = ?")) {
      ps.setInt(1, 4);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
      }
    }
  }

  @Test
  void reportsDeferredThenAppliedPushdownForParameterizedQueries() throws SQLException {
    try (JParqPreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE cyl = ?")) {
      JParqPreparedStatement.PushdownInfo deferredInfo = ps.getPushdownInfo();
      assertFalse(deferredInfo.parquetPredicateAttached());
      assertFalse(deferredInfo.residualFilterPresent());
      assertTrue(deferredInfo.message().contains("deferred"));

      ps.setInt(1, 4);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
      }

      JParqPreparedStatement.PushdownInfo boundInfo = ps.getPushdownInfo();
      assertTrue(boundInfo.parquetPredicateAttached());
      assertFalse(boundInfo.residualFilterPresent());
      assertTrue(boundInfo.statisticsFilteringEnabled());
      assertTrue(boundInfo.columnIndexFilteringEnabled());
      assertTrue(boundInfo.dictionaryFilteringEnabled());
      assertTrue(boundInfo.bloomFilteringEnabled());
    }
  }

  @Test
  void invalidatesBoundPushdownInfoWhenParametersChange() throws SQLException {
    try (JParqPreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE cyl = ?")) {
      ps.setInt(1, 4);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
      }

      JParqPreparedStatement.PushdownInfo boundInfo = ps.getPushdownInfo();
      assertTrue(boundInfo.parquetPredicateAttached());
      assertFalse(boundInfo.residualFilterPresent());

      ps.setInt(1, 6);
      JParqPreparedStatement.PushdownInfo updatedInfo = ps.getPushdownInfo();
      assertFalse(updatedInfo.parquetPredicateAttached());
      assertFalse(updatedInfo.residualFilterPresent());
      assertTrue(updatedInfo.message().contains("deferred"));

      ps.clearParameters();
      JParqPreparedStatement.PushdownInfo clearedInfo = ps.getPushdownInfo();
      assertFalse(clearedInfo.parquetPredicateAttached());
      assertFalse(clearedInfo.residualFilterPresent());
      assertTrue(clearedInfo.message().contains("deferred"));
    }
  }

  @Test
  void reexecutionReplacesPreviousBoundStatement() throws SQLException {
    try (JParqPreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE cyl = ?")) {
      ps.setInt(1, 4);
      try (ResultSet firstResult = ps.executeQuery()) {
        assertTrue(firstResult.next());
        assertEquals(11, firstResult.getInt(1));

        ps.setInt(1, 6);
        try (ResultSet secondResult = ps.executeQuery()) {
          assertTrue(firstResult.isClosed());
          assertSame(secondResult, ps.getResultSet());
          assertTrue(secondResult.next());
          assertEquals(7, secondResult.getInt(1));
        }
      }
    }
  }

  @Test
  void missingParameterFailsFast() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT * FROM mtcars WHERE cyl = ?")) {
      assertThrows(SQLException.class, ps::executeQuery);
    }
  }

  @Test
  void preventsSqlInjection() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      ps.setString(1, "Ford Pantera L");
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      ps.setString(1, "Ford Pantera L' OR 1=1 --");
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  void parameterIndexOutOfRangeIsRejected() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT * FROM mtcars WHERE cyl = ?")) {
      ps.setInt(2, 4);
      assertThrows(SQLException.class, ps::executeQuery);
    }
  }

  @Test
  void handlesEscapedQuotesInSqlString() throws SQLException {
    try (PreparedStatement ps = connection
        .prepareStatement("SELECT * FROM mtcars WHERE model = 'O''Brien' AND cyl = ?")) {
      ps.setInt(1, 4);
      try (ResultSet rs = ps.executeQuery()) {
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void bindsDateParameter() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      Date testDate = Date.valueOf("2023-12-15");
      ps.setDate(1, testDate);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  void exposesBasicParameterMetadata() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT * FROM mtcars WHERE cyl = ? AND model = ?")) {
      ParameterMetaData metaData = ps.getParameterMetaData();
      assertNotNull(metaData);
      assertEquals(2, metaData.getParameterCount());
      assertEquals(ParameterMetaData.parameterNullableUnknown, metaData.isNullable(1));
      assertEquals("VARCHAR", metaData.getParameterTypeName(2));
      assertEquals(String.class.getName(), metaData.getParameterClassName(1));
      assertEquals(ParameterMetaData.parameterModeIn, metaData.getParameterMode(2));
    }
  }

  @Test
  void handlesMultipleEscapedQuotesAndParameters() throws SQLException {
    try (PreparedStatement ps = connection
        .prepareStatement("SELECT * FROM mtcars WHERE model = 'O''Brien''s Car' AND cyl = ? AND hp > ?")) {
      ps.setInt(1, 4);
      ps.setInt(2, 100);
      try (ResultSet rs = ps.executeQuery()) {
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void bindsTimestampParameter() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      Timestamp testTimestamp = Timestamp.valueOf("2023-12-15 14:30:45.123456789");
      ps.setTimestamp(1, testTimestamp);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsTimeParameter() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = ?")) {
      Time testTime = Time.valueOf("14:30:45");
      ps.setTime(1, testTime);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsBigDecimalParameter() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE mpg < ?")) {
      ps.setBigDecimal(1, new BigDecimal("15.0"));
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(5, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsByteArrayParameter() throws SQLException {
    try (PreparedStatement ps = connection
        .prepareStatement("SELECT COUNT(*) FROM mtcars WHERE CAST(model AS VARBINARY) = ?")) {
      for (String model : List.of("Hello", "Mazda RX4")) {
        ps.setBytes(1, model.getBytes(StandardCharsets.UTF_8));
        try (ResultSet rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertEquals(model.equals("Mazda RX4") ? 1 : 0, rs.getInt(1));
        }
      }
    }
  }

  @Test
  void bindsNullParameter() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE ? IS NULL")) {
      ps.setNull(1, java.sql.Types.VARCHAR);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(32, rs.getInt(1));
      }
    }
  }

  @Test
  void handlesQuestionMarkInsideEscapedQuoteString() throws SQLException {
    try (PreparedStatement ps = connection
        .prepareStatement("SELECT * FROM mtcars WHERE model = 'O''Brien?''s' AND cyl = ?")) {
      ps.setInt(1, 4);
      try (ResultSet rs = ps.executeQuery()) {
        assertFalse(rs.next());
      }
    }
  }

  @Test
  void bindsMultipleParametersOfDifferentTypes() throws SQLException {
    try (PreparedStatement ps = connection
        .prepareStatement("SELECT COUNT(*) FROM mtcars WHERE cyl = ? AND vs = ? AND mpg > ? AND model != ?")) {
      ps.setInt(1, 4);
      ps.setInt(2, 1);
      ps.setDouble(3, 20.0);
      ps.setString(4, "NonexistentModel");
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
      }
    }
  }

  @Test
  void bindsNamedParameterAndFilters() throws SQLException {
    try (JParqPreparedStatement ps = connection.prepareStatement("SELECT COUNT(*) FROM mtcars WHERE cyl = :cyl")) {
      assertInstanceOf(JParqPreparedStatement.class, ps);
      ps.setInt("cyl", 4);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
      }
    }
  }

  @Test
  void appliesSameValueToRepeatedNamedParameter() throws SQLException {
    try (JParqPreparedStatement ps = connection
        .prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = :model OR model = :model")) {
      ps.setString("model", "Ford Pantera L");
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
      }
    }
  }

  @Test
  void ignoresNamedMarkerInsideStringLiteral() throws SQLException {
    try (JParqPreparedStatement ps = connection
        .prepareStatement("SELECT COUNT(*) FROM mtcars WHERE model = ':literal' OR cyl = :cyl")) {
      ps.setInt("cyl", 4);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
      }
    }
  }

  @Test
  void ignoresNamedMarkerInsideLineComment() throws SQLException {
    try (JParqPreparedStatement ps = connection
        .prepareStatement("SELECT COUNT(*) FROM mtcars -- comment contains :ignored\nWHERE cyl = :cyl")) {
      ps.setInt("cyl", 4);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
      }
    }
  }

  @Test
  void ignoresNamedMarkerInsideBlockComment() throws SQLException {
    try (JParqPreparedStatement ps = connection
        .prepareStatement("SELECT COUNT(*) FROM mtcars /* comment with :ignored */ WHERE cyl = :cyl")) {
      ps.setInt("cyl", 4);
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
      }
    }
  }

  @Test
  void doesNotTreatPostgresCastAsNamedParameter() throws SQLException {
    try (PreparedStatement ps = connection.prepareStatement("SELECT cyl::int FROM mtcars WHERE cyl = 4")) {
      ParameterMetaData metaData = ps.getParameterMetaData();
      assertEquals(0, metaData.getParameterCount());
      try (ResultSet rs = ps.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
      }
    }
  }

  @Test
  void syntaxErrorDetectedAtPreparationTime() {
    SQLException exception = assertThrows(SQLException.class,
        () -> connection.prepareStatement("SELECT * FORM mtcars WHERE cyl = ?"));
    assertNotNull(exception.getMessage());
    assertTrue(exception.getMessage().toLowerCase().contains("failed to prepare query")
        || exception.getMessage().toLowerCase().contains("syntax")
        || exception.getMessage().toLowerCase().contains("parse"));
  }
}
