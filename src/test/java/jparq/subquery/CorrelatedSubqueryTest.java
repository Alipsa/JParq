package jparq.subquery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import se.alipsa.jparq.JParqResultSet;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;
import se.alipsa.jparq.engine.SqlParser;

/**
 * Integration tests for correlated sub queries to ensure qualifier-aware column resolution
 * returns the expected rows.
 */
class CorrelatedSubqueryTest {

  private static Path tempDirectory;
  private static JParqSql jparqSql;
  private static Schema departmentsSchema;
  private static Schema employeeDepartmentSchema;
  private static Schema salarySchema;

  @BeforeAll
  static void setUp() throws IOException {
    tempDirectory = Files.createTempDirectory("jparq-correlated-subquery");
    departmentsSchema = buildDepartmentsSchema();
    employeeDepartmentSchema = buildEmployeeDepartmentSchema();
    salarySchema = buildSalarySchema();
    writeDepartments();
    writeEmployeeDepartment();
    writeSalary();
    jparqSql = new JParqSql("jdbc:jparq:" + tempDirectory.toAbsolutePath());
  }

  @AfterAll
  static void tearDown() throws IOException {
    if (tempDirectory == null) {
      return;
    }
    try (Stream<Path> paths = Files.walk(tempDirectory)) {
      paths.sorted((left, right) -> right.compareTo(left)).forEach(path -> {
        try {
          Files.deleteIfExists(path);
        } catch (IOException e) {
          // Best-effort cleanup to avoid masking test results.
        }
      });
    }
  }

  @Test
  void correlatedFiltersReturnExpectedRows() {
    assertBaseSalaryRows();
    assertDerivedEmployees();

    String sql = """
        WITH high_salary AS (
          SELECT DISTINCT employee
          FROM salary
          WHERE salary >= 180000.0
        )
        SELECT derived.employee_id,
               (SELECT d.department FROM departments d WHERE d.id = derived.department_id) AS department_name,
               (SELECT COUNT(*) FROM salary s WHERE s.employee = derived.employee_id) AS salary_change_count
        FROM (
          SELECT ed.employee AS employee_id, ed.department AS department_id
          FROM employee_department ed
          JOIN high_salary hs ON hs.employee = ed.employee
        ) AS derived
        WHERE EXISTS (
          SELECT 1
          FROM salary s
          WHERE s.employee = derived.employee_id
            AND s.salary >= 180000.0
        )
        ORDER BY derived.employee_id
        """;

    QualifierSnapshot qualifiers = extractQualifiers(sql);
    List<String> parsedQualifiers = parseQualifiers(sql);
    Assertions.assertTrue(parsedQualifiers.stream().anyMatch(q -> "derived".equalsIgnoreCase(q)),
        "parser should retain the derived table alias");
    Assertions.assertTrue(qualifiers.queryQualifiers().stream().anyMatch(q -> "derived".equalsIgnoreCase(q)),
        () -> "outer query should expose the derived table alias for correlation but had: "
            + qualifiers.queryQualifiers() + " mapping qualifiers: " + qualifiers.mappingQualifiers()
            + " provided qualifiers: " + qualifiers.providedQualifiers() + " table name: "
            + qualifiers.tableName());

    List<String> rows = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        while (rs.next()) {
          rows.add(rs.getInt("employee_id") + ":" + rs.getString("department_name") + ":"
              + rs.getLong("salary_change_count"));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to evaluate correlated filters scenario", e);
      }
    });

    Assertions.assertEquals(List.of("2:IT:1", "4:Sales:1", "5:Sales:1"), rows);
  }

  private QualifierSnapshot extractQualifiers(String sql) {
    List<String> qualifiers = new ArrayList<>();
    List<String> mappingQualifiers = new ArrayList<>();
    List<String> providedQualifiers = new ArrayList<>();
    StringBuilder tableName = new StringBuilder();
    jparqSql.query(sql, rs -> {
      try {
        JParqResultSet jrs = rs.unwrap(JParqResultSet.class);
        Optional.ofNullable(jrs).ifPresent(resultSet -> {
          try {
            var qualifierField = JParqResultSet.class.getDeclaredField("queryQualifiers");
            qualifierField.setAccessible(true);
            Object value = qualifierField.get(resultSet);
            if (value instanceof List<?> list) {
              for (Object entry : list) {
                if (entry instanceof String s) {
                  qualifiers.add(s);
                }
              }
            }

            var providedField = JParqResultSet.class.getDeclaredField("providedQualifiers");
            providedField.setAccessible(true);
            Object providedValue = providedField.get(resultSet);
            if (providedValue instanceof List<?> list) {
              for (Object entry : list) {
                if (entry instanceof String s) {
                  providedQualifiers.add(s);
                }
              }
            }

            var mappingField = JParqResultSet.class.getDeclaredField("qualifierColumnMapping");
            mappingField.setAccessible(true);
            Object mappingValue = mappingField.get(resultSet);
            if (mappingValue instanceof Map<?, ?> map) {
              for (Object key : map.keySet()) {
                if (key instanceof String s) {
                  mappingQualifiers.add(s);
                }
              }
            }

            var tableField = JParqResultSet.class.getDeclaredField("tableName");
            tableField.setAccessible(true);
            Object tableValue = tableField.get(resultSet);
            if (tableValue instanceof String s && !s.isBlank()) {
              tableName.append(s);
            }
          } catch (Exception e) {
            throw new IllegalStateException("Unable to inspect query qualifiers", e);
          }
        });
      } catch (Exception e) {
        throw new IllegalStateException("Failed to unwrap JParqResultSet", e);
      }
    });
    return new QualifierSnapshot(qualifiers, mappingQualifiers, providedQualifiers, tableName.toString());
  }

  private record QualifierSnapshot(List<String> queryQualifiers, List<String> mappingQualifiers,
      List<String> providedQualifiers, String tableName) {
  }

  private List<String> parseQualifiers(String sql) {
    SqlParser.Select parsed = SqlParser.parseSelect(sql);
    List<String> qualifiers = new ArrayList<>();
    Optional.ofNullable(parsed.tableReferences()).orElse(List.of()).stream()
        .flatMap(ref -> Stream.of(ref.tableAlias(), ref.tableName()))
        .filter(alias -> alias != null && !alias.isBlank()).forEach(qualifiers::add);
    return qualifiers;
  }

  private void assertBaseSalaryRows() {
    List<Integer> employees = new ArrayList<>();
    jparqSql.query("SELECT employee FROM salary ORDER BY employee", rs -> {
      try {
        while (rs.next()) {
          employees.add(rs.getInt("employee"));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to inspect base salary rows", e);
      }
    });
    Assertions.assertEquals(List.of(2, 4, 5), employees, "salary table should expose three high earners");
  }

  private void assertDerivedEmployees() {
    String sql = """
        WITH high_salary AS (
          SELECT DISTINCT employee
          FROM salary
          WHERE salary >= 180000.0
        )
        SELECT ed.employee AS employee_id
        FROM employee_department ed
        JOIN high_salary hs ON hs.employee = ed.employee
        ORDER BY ed.employee
        """;
    List<Integer> employees = new ArrayList<>();
    List<String> columnNames = new ArrayList<>();
    jparqSql.query(sql, rs -> {
      try {
        var meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          columnNames.add(meta.getColumnName(i));
        }
        while (rs.next()) {
          employees.add(rs.getInt("employee_id"));
        }
      } catch (Exception e) {
        throw new IllegalStateException("Failed to query derived employees", e);
      }
    });
    Assertions.assertEquals(List.of("employee"), columnNames,
        "physical column name should remain aligned with the underlying field");
    Assertions.assertEquals(List.of(2, 4, 5), employees, "derived join should retain high-salary employees");
  }

  private static Schema buildDepartmentsSchema() {
    return SchemaBuilder.record("departments").namespace("jparq.subquery").fields().requiredInt("id")
        .requiredString("department").endRecord();
  }

  private static Schema buildEmployeeDepartmentSchema() {
    return SchemaBuilder.record("employee_department").namespace("jparq.subquery").fields().requiredInt("employee")
        .requiredInt("department").endRecord();
  }

  private static Schema buildSalarySchema() {
    return SchemaBuilder.record("salary").namespace("jparq.subquery").fields().requiredInt("employee")
        .requiredDouble("salary").endRecord();
  }

  private static void writeDepartments() throws IOException {
    var outputPath = new org.apache.hadoop.fs.Path(tempDirectory.resolve("departments.parquet").toUri());
    Configuration conf = new Configuration(false);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(outputPath, conf))
        .withSchema(departmentsSchema).withConf(conf).withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
      writer.write(buildDepartment(10, "IT"));
      writer.write(buildDepartment(20, "Sales"));
    }
  }

  private static void writeEmployeeDepartment() throws IOException {
    var outputPath = new org.apache.hadoop.fs.Path(tempDirectory.resolve("employee_department.parquet").toUri());
    Configuration conf = new Configuration(false);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(outputPath, conf))
        .withSchema(employeeDepartmentSchema).withConf(conf)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
      writer.write(buildEmployeeDepartment(2, 10));
      writer.write(buildEmployeeDepartment(4, 20));
      writer.write(buildEmployeeDepartment(5, 20));
    }
  }

  private static void writeSalary() throws IOException {
    var outputPath = new org.apache.hadoop.fs.Path(tempDirectory.resolve("salary.parquet").toUri());
    Configuration conf = new Configuration(false);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(HadoopOutputFile.fromPath(outputPath, conf))
        .withSchema(salarySchema).withConf(conf).withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
      writer.write(buildSalary(2, 180000.0));
      writer.write(buildSalary(4, 185000.0));
      writer.write(buildSalary(5, 180000.0));
    }
  }

  private static GenericRecord buildDepartment(int id, String department) {
    GenericData.Record record = new GenericData.Record(departmentsSchema);
    record.put("id", id);
    record.put("department", department);
    return record;
  }

  private static GenericRecord buildEmployeeDepartment(int employee, int department) {
    GenericData.Record record = new GenericData.Record(employeeDepartmentSchema);
    record.put("employee", employee);
    record.put("department", department);
    return record;
  }

  private static GenericRecord buildSalary(int employee, double salary) {
    GenericData.Record record = new GenericData.Record(salarySchema);
    record.put("employee", employee);
    record.put("salary", salary);
    return record;
  }
}
