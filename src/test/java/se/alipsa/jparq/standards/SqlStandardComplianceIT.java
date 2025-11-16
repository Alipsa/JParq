package se.alipsa.jparq.standards;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Executes the SQL standard compliance suite described under
 * {@code src/test/resources/standards}.
 *
 * <p>
 * The suite is data driven: every {@code .sql} file in the hierarchy is
 * executed and compared against the sibling {@code .csv} file that contains the
 * expected result set.
 * </p>
 */
public class SqlStandardComplianceIT {

  private static Path standardsRoot;
  private static String acmeJdbcUrl;

  /**
   * Discovers, executes and validates every SQL standard compliance test case.
   *
   * @throws IOException
   *           if the test data cannot be read
   */
  @Test
  void executeStandardComplianceSuite() throws IOException {
    List<TestCase> testCases = discoverTestCases();
    assertFalse(testCases.isEmpty(), "No SQL standard compliance tests were discovered");
    List<Path> exclusions = List.of(
        // Example exclusions:
        // standardsRoot.resolve("some/subdir/test-to-exclude.sql"),
        //standardsRoot.resolve("Subqueries/Correlated/subquery_correlated_filters.sql"),
        standardsRoot.resolve("TableValueFunctions/Unnest/unnest_table_wrapper.sql"),
        standardsRoot.resolve("Wildcards/Unqualified/unqualified_star.sql"));
    for (TestCase testCase : testCases) {
      if (exclusions.contains(testCase.sqlPath)) {
        System.out.println("Skipping excluded test: " + describe(testCase.sqlPath));
        continue;
      }
      executeAndVerify(testCase);
    }
  }

  /**
   * Initializes required resources before any tests execute.
   *
   * @throws IllegalStateException
   *           if either the standards hierarchy or the ACME dataset cannot be
   *           resolved
   */
  @BeforeAll
  static void setUpSuite() {
    standardsRoot = locateStandardsRoot();
    Path acmeRoot = locateAcmeRoot();
    acmeJdbcUrl = "jdbc:jparq:" + acmeRoot.toAbsolutePath();
  }

  private static List<TestCase> discoverTestCases() throws IOException {
    try (Stream<Path> stream = Files.walk(standardsRoot)) {
      return stream.filter(Files::isRegularFile).filter(path -> path.getFileName().toString().endsWith(".sql"))
          .map(SqlStandardComplianceIT::createTestCase)
          .sorted(Comparator.comparing(testCase -> testCase.sqlPath().toString()))
          .collect(Collectors.toCollection(ArrayList::new));
    }
  }

  private static TestCase createTestCase(Path sqlPath) {
    Path csvPath = replaceExtension(sqlPath, ".csv");
    if (!Files.exists(csvPath)) {
      fail("Missing expected result file for SQL test: " + describe(sqlPath));
    }
    String sql;
    try {
      sql = Files.readString(sqlPath, StandardCharsets.UTF_8);
    } catch (IOException e) {
      fail("Unable to read SQL file: " + sqlPath, e);
      return new TestCase(sqlPath, csvPath, "");
    }
    return new TestCase(sqlPath, csvPath, sql);
  }

  private static Path replaceExtension(Path sqlPath, String newExtension) {
    String fileName = sqlPath.getFileName().toString();
    int idx = fileName.lastIndexOf('.');
    if (idx == -1) {
      throw new IllegalArgumentException("SQL file must have an extension: " + sqlPath);
    }
    String updatedName = fileName.substring(0, idx) + newExtension;
    return sqlPath.resolveSibling(updatedName);
  }

  private static void executeAndVerify(TestCase testCase) {
    ExpectedResult expected = readExpected(testCase.csvPath());
    QueryResult actual = executeQuery(testCase);
    compareColumns(expected, actual, testCase.sqlPath());
    compareRows(expected, actual, testCase.sqlPath());
  }

  private static void compareColumns(ExpectedResult expected, QueryResult actual, Path sqlPath) {
    if (expected.columns().size() != actual.columns().size()) {
      fail("Column count mismatch for " + describe(sqlPath) + ": expected " + expected.columns().size()
          + " but received " + actual.columns().size());
    }
    for (int i = 0; i < expected.columns().size(); i++) {
      String expectedName = expected.columns().get(i);
      String actualName = actual.columns().get(i);
      if (expectedName == null || actualName == null) {
        fail("Column metadata is missing for " + describe(sqlPath) + " at position " + (i + 1) + ": expected name '"
            + expectedName + "', actual '" + actualName + "'");
      }
      if (!Objects.equals(expectedName, actualName)) {
        fail("Column name mismatch for " + describe(sqlPath) + " at position " + (i + 1) + ": expected '" + expectedName
            + "' but received '" + actualName + "'");
      }
    }
  }

  private static void compareRows(ExpectedResult expected, QueryResult actual, Path sqlPath) {
    if (expected.rows().size() != actual.rows().size()) {
      fail("Row count mismatch for " + describe(sqlPath) + ": expected " + expected.rows().size() + " but received "
          + actual.rows().size());
    }
    for (int rowIndex = 0; rowIndex < expected.rows().size(); rowIndex++) {
      List<String> expectedRow = expected.rows().get(rowIndex);
      List<String> actualRow = actual.rows().get(rowIndex);
      if (expectedRow.size() != actualRow.size()) {
        fail("Column count mismatch on row " + (rowIndex + 1) + " for " + describe(sqlPath));
      }
      for (int colIndex = 0; colIndex < expectedRow.size(); colIndex++) {
        String expectedValue = normalizeExpected(expectedRow.get(colIndex));
        String actualValue = actualRow.get(colIndex);
        if (!Objects.equals(expectedValue, actualValue)) {
          fail("Value mismatch for " + describe(sqlPath) + " at row " + (rowIndex + 1) + ", column " + (colIndex + 1)
              + ": expected '" + expectedValue + "' but received '" + actualValue + "'");
        }
      }
    }
  }

  private static ExpectedResult readExpected(Path csvPath) {
    List<String> lines;
    try {
      lines = Files.readAllLines(csvPath, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to read expected results file: " + csvPath, e);
    }
    List<String> filtered = lines.stream().map(SqlStandardComplianceIT::stripLeadingBom)
        .filter(line -> !line.trim().isEmpty()).toList();
    if (filtered.isEmpty()) {
      fail("Expected results file is empty: " + describe(csvPath));
    }
    List<String> header = parseCsvLine(filtered.getFirst());
    List<List<String>> rows = new ArrayList<>();
    for (int i = 1; i < filtered.size(); i++) {
      rows.add(parseCsvLine(filtered.get(i)));
    }
    return new ExpectedResult(header, rows);
  }

  private static QueryResult executeQuery(TestCase testCase) {
    String sql = testCase.sql();
    List<String> columnLabels = new ArrayList<>();
    List<List<String>> rows = new ArrayList<>();
    try (Connection conn = DriverManager.getConnection(acmeJdbcUrl);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      ResultSetMetaData meta = rs.getMetaData();
      for (int i = 1; i <= meta.getColumnCount(); i++) {
        columnLabels.add(meta.getColumnLabel(i));
      }
      final int columnCount = columnLabels.size();
      while (rs.next()) {
        List<String> row = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
          row.add(normalizeActualValue(rs.getObject(i)));
        }
        // Collections.unmodifiableList allows null values which occur in result sets;
        // List.copyOf does not.
        rows.add(Collections.unmodifiableList(new ArrayList<>(row)));
      }
    } catch (SQLException e) {
      throw new AssertionError(testCase.sqlPath + " Query execution failed for statement: " + sql, e);
    }
    return new QueryResult(List.copyOf(columnLabels), List.copyOf(rows));
  }

  private static String normalizeActualValue(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof java.sql.Date date) {
      return date.toLocalDate().toString();
    }
    if (value instanceof java.sql.Time time) {
      return time.toLocalTime().toString();
    }
    if (value instanceof java.sql.Timestamp timestamp) {
      return timestamp.toInstant().toString();
    }
    if (value instanceof BigDecimal bigDecimal) {
      return bigDecimal.stripTrailingZeros().toPlainString();
    }
    if (value instanceof Double doubleValue) {
      // Use String constructor to avoid binary to decimal rounding issues when
      // normalizing.
      return new BigDecimal(Double.toString(doubleValue)).stripTrailingZeros().toPlainString();
    }
    if (value instanceof Float floatValue) {
      // Convert via String to prevent double rounding when converting float -> double
      // -> BigDecimal.
      return new BigDecimal(Float.toString(floatValue)).stripTrailingZeros().toPlainString();
    }
    if (value instanceof Number number) {
      return number.toString();
    }
    return value.toString();
  }

  private static List<String> parseCsvLine(String line) {
    List<String> values = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean inQuotes = false;
    boolean expectingSeparator = false;
    for (int i = 0; i < line.length(); i++) {
      char ch = line.charAt(i);
      if (expectingSeparator) {
        if (ch == ',') {
          values.add(current.toString());
          current.setLength(0);
          expectingSeparator = false;
          continue;
        }
        if (Character.isWhitespace(ch)) {
          continue;
        }
        throw new IllegalArgumentException("Malformed CSV line (expected comma after closing quote): " + line);
      }
      if (ch == '"') {
        if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          current.append('"');
          i++;
        } else {
          inQuotes = !inQuotes;
          if (!inQuotes) {
            expectingSeparator = true;
          }
        }
      } else if (ch == ',' && !inQuotes) {
        values.add(current.toString());
        current.setLength(0);
      } else {
        current.append(ch);
      }
    }
    if (inQuotes) {
      throw new IllegalArgumentException("Malformed CSV line (missing closing quote): " + line);
    }
    values.add(current.toString());
    return values;
  }

  private static String normalizeExpected(String value) {
    String trimmed = value.trim();
    if (trimmed.equalsIgnoreCase("NULL")) {
      return null;
    }
    return trimmed;
  }

  private static Path locateStandardsRoot() {
    URL standardsUrl = SqlStandardComplianceIT.class.getClassLoader().getResource("standards");
    if (standardsUrl == null) {
      throw new IllegalStateException("standards resource directory is missing from the classpath");
    }
    try {
      return Paths.get(standardsUrl.toURI());
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Unable to resolve standards resource directory", e);
    }
  }

  private static Path locateAcmeRoot() {
    URL acmeUrl = SqlStandardComplianceIT.class.getResource("/acme");
    if (acmeUrl == null) {
      throw new IllegalStateException("acme dataset must be available on the test classpath");
    }
    try {
      return Paths.get(acmeUrl.toURI());
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Unable to resolve acme dataset directory", e);
    }
  }

  private static String describe(Path path) {
    try {
      Path relative = standardsRoot.relativize(path.toAbsolutePath());
      return relative.toString().replace('\\', '/');
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Path " + path.toAbsolutePath() + " is not under the standards root "
          + standardsRoot.toAbsolutePath() + ". This may indicate a test misconfiguration.", e);
    }
  }

  /**
   * Removes a leading byte-order mark if present, without touching legitimate
   * occurrences later in the line.
   *
   * @param line
   *          the line to inspect
   * @return the sanitized line
   */
  private static String stripLeadingBom(String line) {
    return line.startsWith("\uFEFF") ? line.substring(1) : line;
  }

  private record TestCase(Path sqlPath, Path csvPath, String sql) {
  }

  private record ExpectedResult(List<String> columns, List<List<String>> rows) {
  }

  private record QueryResult(List<String> columns, List<List<String>> rows) {
  }
}
