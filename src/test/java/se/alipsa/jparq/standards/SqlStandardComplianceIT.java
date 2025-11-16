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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Executes the SQL standard compliance suite described under {@code src/test/resources/standards}.
 *
 * <p>
 * The suite is data driven: every {@code .sql} file in the hierarchy is executed and compared against the
 * sibling {@code .csv} file that contains the expected result set.
 * </p>
 */
public class SqlStandardComplianceIT {

  private static final Path STANDARDS_ROOT = locateStandardsRoot();
  private static final JParqSql ACME_SQL = new JParqSql("jdbc:jparq:" + locateAcmeRoot().toAbsolutePath());

  /**
   * Discovers, executes and validates every SQL standard compliance test case.
   *
   * @throws IOException
   *     if the test data cannot be read
   */
  @Test
  void executeStandardComplianceSuite() throws IOException {
    List<TestCase> testCases = discoverTestCases();
    assertFalse(testCases.isEmpty(), "No SQL standard compliance tests were discovered");
    for (TestCase testCase : testCases) {
      executeAndVerify(testCase);
    }
  }

  private static List<TestCase> discoverTestCases() throws IOException {
    try (Stream<Path> stream = Files.walk(STANDARDS_ROOT)) {
      return stream.filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".sql"))
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
      throw new IllegalStateException("Unable to read SQL file: " + sqlPath, e);
    }
    return new TestCase(sqlPath, csvPath, sql);
  }

  private static Path replaceExtension(Path sqlPath, String newExtension) {
    String fileName = sqlPath.getFileName().toString();
    int idx = fileName.lastIndexOf('.') + 1;
    String updatedName = fileName.substring(0, idx) + newExtension.substring(1);
    return sqlPath.resolveSibling(updatedName);
  }

  private static void executeAndVerify(TestCase testCase) {
    ExpectedResult expected = readExpected(testCase.csvPath());
    QueryResult actual = executeQuery(testCase.sql());
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
      if (!Objects.equals(expectedName, actualName)) {
        fail("Column name mismatch for " + describe(sqlPath) + " at position " + (i + 1) + ": expected '"
            + expectedName + "' but received '" + actualName + "'");
      }
    }
  }

  private static void compareRows(ExpectedResult expected, QueryResult actual, Path sqlPath) {
    if (expected.rows().size() != actual.rows().size()) {
      fail("Row count mismatch for " + describe(sqlPath) + ": expected " + expected.rows().size()
          + " but received " + actual.rows().size());
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
          fail("Value mismatch for " + describe(sqlPath) + " at row " + (rowIndex + 1) + ", column "
              + (colIndex + 1) + ": expected '" + expectedValue + "' but received '" + actualValue + "'");
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
    List<String> filtered = lines.stream()
        .map(line -> line.replace("\uFEFF", ""))
        .filter(line -> !line.trim().isEmpty())
        .toList();
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

  private static QueryResult executeQuery(String sql) {
    List<String> columnLabels = new ArrayList<>();
    List<List<String>> rows = new ArrayList<>();
    ACME_SQL.query(sql, rs -> {
      try {
        ResultSetMetaData meta = rs.getMetaData();
        if (columnLabels.isEmpty()) {
          for (int i = 1; i <= meta.getColumnCount(); i++) {
            columnLabels.add(meta.getColumnLabel(i));
          }
        }
        final int columnCount = columnLabels.size();
        while (rs.next()) {
          List<String> row = new ArrayList<>(columnCount);
          for (int i = 1; i <= columnCount; i++) {
            row.add(normalizeActualValue(rs.getObject(i)));
          }
          rows.add(Collections.unmodifiableList(row));
        }
      } catch (SQLException e) {
        throw new AssertionError("Query execution failed", e);
      }
    });
    return new QueryResult(Collections.unmodifiableList(columnLabels), Collections.unmodifiableList(rows));
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
      return BigDecimal.valueOf(doubleValue).stripTrailingZeros().toPlainString();
    }
    if (value instanceof Float floatValue) {
      return BigDecimal.valueOf(floatValue.doubleValue()).stripTrailingZeros().toPlainString();
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
    for (int i = 0; i < line.length(); i++) {
      char ch = line.charAt(i);
      if (ch == '"') {
        if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
          current.append('"');
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (ch == ',' && !inQuotes) {
        values.add(current.toString());
        current.setLength(0);
      } else {
        current.append(ch);
      }
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
    Path relative = STANDARDS_ROOT.relativize(path.toAbsolutePath());
    return relative.toString().replace('\\', '/');
  }

  private record TestCase(Path sqlPath, Path csvPath, String sql) {
  }

  private record ExpectedResult(List<String> columns, List<List<String>> rows) {
  }

  private record QueryResult(List<String> columns, List<List<String>> rows) {
  }
}
