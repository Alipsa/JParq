package jparq.window.navigation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.SQLSyntaxErrorException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.helper.TemporalInterval;

/**
 * Verifies navigation defaults whose SQL type has multiple engine
 * representations.
 */
class NavigationDefaultTypeTest {
  @TempDir
  Path directory;

  /**
   * Early interval and schema-bound default errors share preparation diagnostics.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void incompatibleDefaultsSharePreparationContext() throws Exception {
    Path fixtures = Path.of("src/test/resources").toAbsolutePath();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + fixtures)) {
      for (String function : List.of("LAG", "LEAD")) {
        for (String arguments : List.of("INTERVAL '1' DAY,1,INTERVAL '1' YEAR", "gear,1,INTERVAL '1' DAY")) {
          String query = "SELECT " + function + "(" + arguments + ") OVER (ORDER BY gear) FROM mtcars";
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(query));
          assertEquals("42000", failure.getSQLState());
          assertTrue(failure.getMessage().startsWith("Failed to prepare query: "), failure.getMessage());
          assertTrue(failure.getMessage().contains(function + " default type INTERVAL"), failure.getMessage());
          assertFalse(failure.getMessage().contains("Failed to parse SQL"), failure.getMessage());
        }
      }
    }
  }

  /**
   * Reports nested ARRAY element types instead of only repeating ARRAY.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void describesArrayDefaultElementTypes() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:.")) {
      for (String function : List.of("LAG", "LEAD")) {
        for (boolean nested : List.of(false, true)) {
          String value = "ARRAY[TIMESTAMP '2024-07-01 10:00:00']";
          String fallback = "ARRAY[CAST(1 AS INTEGER)]";
          String sourceType = "ARRAY(INTEGER)";
          String targetType = "ARRAY(TIMESTAMP)";
          if (nested) {
            value = "ARRAY[" + value + "]";
            fallback = "ARRAY[" + fallback + "]";
            sourceType = "ARRAY(" + sourceType + ")";
            targetType = "ARRAY(" + targetType + ")";
          }
          String query = "SELECT " + function + "(" + value + ",1," + fallback
              + ") OVER (ORDER BY id) FROM (VALUES (1)) t(id)";
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(query));
          assertEquals("42000", failure.getSQLState());
          assertTrue(failure.getMessage().contains(sourceType + " is not assignable to " + targetType),
              failure.getMessage());
        }
      }
    }
  }

  /**
   * Rejects cross-kind INTERVAL defaults while accepting conversions within a
   * kind.
   *
   * @throws Exception
   *           if JDBC evaluation fails
   */
  @Test
  void validatesIntervalDefaultKinds() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:.")) {
      for (String function : List.of("LAG", "LEAD")) {
        for (String suffix : List.of("", " WHERE id > 5")) {
          for (String arguments : List.of("INTERVAL '1' DAY,1,INTERVAL '1' YEAR",
              "INTERVAL '1' YEAR,1,INTERVAL '1' DAY")) {
            String query = "SELECT " + function + "(" + arguments + ") OVER (ORDER BY id) FROM (VALUES (1)) t(id)"
                + suffix;
            var failure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(query));
            assertEquals("42000", failure.getSQLState());
            assertTrue(failure.getMessage().contains("INTERVAL DAY"), failure.getMessage());
            assertTrue(failure.getMessage().contains("INTERVAL YEAR"), failure.getMessage());
          }
        }
        String query = "SELECT " + function + "(INTERVAL '1' MONTH,5,INTERVAL '1' YEAR) OVER (ORDER BY id)," + function
            + "(INTERVAL '1' HOUR,5,INTERVAL '1' DAY) OVER (ORDER BY id) FROM (VALUES (1)) t(id)";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(query)) {
          assertTrue(result.next());
          assertEquals(12, ((TemporalInterval) result.getObject(1)).period().toTotalMonths());
          assertEquals(1, ((TemporalInterval) result.getObject(2)).period().getDays());
          assertFalse(result.next());
        }
      }
    }
  }

  /**
   * Rejects incompatible defaults during preparation even if the query has no
   * rows.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void rejectsIncompatibleDefaultsBeforeExecution() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:.")) {
      for (String function : List.of("LAG", "LEAD")) {
        for (String suffix : List.of("", " WHERE id > 5")) {
          String query = "SELECT id," + function + "(id,5,'x') OVER (ORDER BY id) " + "FROM (VALUES (1),(2)) t(id)"
              + suffix;
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(query), query);
          assertEquals("42000", failure.getSQLState(), query);
        }
      }
      String array = "SELECT LAG(ARRAY[id],1,ARRAY['x']) OVER (ORDER BY id) FROM (VALUES (1)) t(id)";
      assertEquals("42000",
          assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(array)).getSQLState());
    }
  }

  /**
   * Rejects incompatible navigation defaults in correlated subqueries at
   * preparation, resolving outer column types through the outer scope so the
   * check does not depend on the data.
   *
   * @throws Exception
   *           if JDBC setup fails
   */
  @Test
  void rejectsCorrelatedDefaultsBeforeExecution() throws Exception {
    Path fixtures = Path.of("src/test/resources").toAbsolutePath();
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + fixtures)) {
      for (String function : List.of("LAG", "LEAD")) {
        for (String suffix : List.of("", " WHERE 1=0")) {
          String query = "SELECT (SELECT " + function + "(m.gear,1,'a') OVER (ORDER BY gear)"
              + " FROM mtcars FETCH FIRST 1 ROW ONLY) FROM mtcars m" + suffix;
          var failure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(query), query);
          assertEquals("42000", failure.getSQLState(), query);
          assertTrue(failure.getMessage().contains(function + " default type VARCHAR is not assignable to INTEGER"),
              failure.getMessage());
          assertFalse(failure.getMessage().contains("__JPARQ_CORRELATED_VALUE"), failure.getMessage());
        }
      }
    }
  }

  /**
   * An input's alias hides its table name: when an earlier table's name equals a
   * later input's alias, the alias must win so the outer column type is read from
   * the right table.
   *
   * @throws Exception
   *           if fixture writing or JDBC access fails
   */
  @Test
  void aliasHidesTableNameForOuterResolution() throws Exception {
    writeTable("first", Schema.Type.STRING);
    writeTable("second", Schema.Type.INT);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      String select = "SELECT (SELECT LAG(first.gear,1,%s) OVER (ORDER BY gear) FROM second FETCH FIRST 1 ROW ONLY)"
          + " FROM first AS c JOIN second AS first ON c.id = first.id";
      var failure = assertThrows(SQLSyntaxErrorException.class,
          () -> connection.prepareStatement(String.format(select, "'a'")));
      assertEquals("42000", failure.getSQLState(), failure.getMessage());
      assertTrue(failure.getMessage().contains("LAG default type VARCHAR is not assignable to INTEGER"),
          failure.getMessage());
      // A compatible default confirms the alias resolved to the INTEGER column:
      // the first row's LAG has no previous row, so it returns the default.
      try (var prepared = connection.prepareStatement(String.format(select, "7"));
          var result = prepared.executeQuery()) {
        assertTrue(result.next());
        assertEquals(7, result.getInt(1));
        assertFalse(result.next());
      }
    }
  }

  /**
   * Writes a single-id table whose gear column uses the requested type.
   *
   * @param name
   *          table name
   * @param gearType
   *          Avro type of the gear column
   * @throws Exception
   *           if fixture writing fails
   */
  private void writeTable(String name, Schema.Type gearType) throws Exception {
    writeTable(name, gearType, "gear");
  }

  /**
   * Writes a single-id table whose gear column uses the requested type and field
   * name.
   *
   * @param name
   *          table name
   * @param gearType
   *          Avro type of the gear column
   * @param fieldName
   *          name of the gear column
   * @throws Exception
   *           if fixture writing fails
   */
  private void writeTable(String name, Schema.Type gearType, String fieldName) throws Exception {
    Schema schema = SchemaBuilder.record(name).fields().requiredInt("id").name(fieldName).type(Schema.create(gearType))
        .noDefault().endRecord();
    var configuration = new Configuration(false);
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve(name + ".parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericRecord>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      var row = new GenericData.Record(schema);
      row.put("id", 1);
      row.put(fieldName, gearType == Schema.Type.STRING ? "x" : 1);
      writer.write(row);
    }
  }

  /**
   * Quoted mixed-case outer references match their equally named source field, so
   * an incompatible default is rejected during preparation instead of skipping
   * the check.
   *
   * @throws Exception
   *           if fixture writing or JDBC access fails
   */
  @Test
  void quotedMixedCaseOuterReferenceIsValidated() throws Exception {
    writeTable("mixed", Schema.Type.INT, "Gear");
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory)) {
      String query = "SELECT (SELECT LAG(m.\"Gear\",1,'a') OVER (ORDER BY \"Gear\")"
          + " FROM mixed FETCH FIRST 1 ROW ONLY) FROM mixed m";
      var failure = assertThrows(SQLSyntaxErrorException.class, () -> connection.prepareStatement(query));
      assertEquals("42000", failure.getSQLState(), failure.getMessage());
      assertTrue(failure.getMessage().contains("LAG default type VARCHAR is not assignable to INTEGER"),
          failure.getMessage());
    }
  }

  /**
   * Numeric assignment truncates fractions toward zero without discarding leading
   * digits.
   *
   * @throws Exception
   *           if JDBC evaluation fails
   */
  @Test
  void truncatesNumericDefaultsAndRejectsOverflow() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:.")) {
      for (String fraction : List.of("1.5", "-1.5")) {
        String query = "SELECT id,LEAD(id,5," + fraction + ") OVER (ORDER BY id)," + "LAG(id,1," + fraction
            + ") OVER (ORDER BY id) FROM (VALUES (1),(2)) t(id) ORDER BY id";
        try (var statement = connection.createStatement(); var result = statement.executeQuery(query)) {
          assertTrue(result.next(), query);
          int truncated = fraction.startsWith("-") ? -1 : 1;
          assertEquals(truncated, result.getInt(2));
          assertEquals(truncated, result.getInt(3));
          assertEquals(result.getMetaData().getColumnClassName(2), result.getObject(2).getClass().getName());
          assertTrue(result.next(), query);
          assertEquals(truncated, result.getInt(2));
          assertEquals(1, result.getInt(3));
          assertFalse(result.next(), query);
        }
      }
      String overflow = "SELECT LAG(CAST(id AS INTEGER),1,2147483648) OVER (ORDER BY id) FROM (VALUES (1)) t(id)";
      try (var prepared = connection.prepareStatement(overflow)) {
        assertEquals("22003", assertThrows(SQLDataException.class, prepared::executeQuery).getSQLState());
      }
    }
  }

  /**
   * Keeps ARRAY defaults assignable without requiring one concrete Java array
   * class.
   *
   * @throws Exception
   *           if JDBC evaluation fails
   */
  @Test
  void retainsArrayDefaults() throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:.")) {
      for (String function : List.of("LAG", "LEAD")) {
        String query = "SELECT " + function + "(ARRAY[id],1,ARRAY[9]) OVER (ORDER BY id) "
            + "FROM (VALUES (1),(2)) t(id) ORDER BY id";
        int[] expected = function.equals("LAG") ? new int[]{
            9, 1
        } : new int[]{
            2, 9
        };
        try (var statement = connection.createStatement(); var result = statement.executeQuery(query)) {
          for (int value : expected) {
            assertTrue(result.next(), query);
            Object[] array = (Object[]) result.getArray(1).getArray();
            assertEquals(1, array.length, query);
            assertEquals(value, ((Number) array[0]).intValue(), query);
          }
          assertFalse(result.next(), query);
        }
      }
    }
  }
}
