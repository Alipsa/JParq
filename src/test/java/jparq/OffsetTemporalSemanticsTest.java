package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import se.alipsa.jparq.engine.SqlComparisons;
import se.alipsa.jparq.engine.function.DateTimeFunctions;
import se.alipsa.jparq.helper.TemporalInterval;
import se.alipsa.jparq.helper.TemporalValueType;

/** Verifies offset datetime semantics independently of written clock fields. */
@Isolated("Changes the JVM default time zone")
class OffsetTemporalSemanticsTest {
  @TempDir
  Path directory;

  /**
   * Checks local Java and JDBC representations compare by the same clock fields.
   */
  @Test
  void comparesLocalRepresentations() {
    var local = LocalDateTime.parse("2024-07-01T10:00:00.123456789");
    assertEquals(0, SqlComparisons.typedCompare(local, Timestamp.valueOf(local)));
    assertEquals(0, SqlComparisons.typedCompare(Timestamp.valueOf(local), local));
    var time = Time.valueOf("10:00:00");
    time.setTime(time.getTime() + 500);
    assertEquals(0, SqlComparisons.typedCompare(LocalTime.parse("10:00:00.5"), time));
    assertEquals(0, SqlComparisons.typedCompare(time, LocalTime.parse("10:00:00.5")));
  }

  /**
   * Preserves instants in predicates, keys, arithmetic, casts, and JDBC getters.
   *
   * @param zone
   *          JVM time zone
   * @throws Exception
   *           if fixture creation or querying fails
   */
  @ParameterizedTest
  @ValueSource(strings = {
      "UTC", "Europe/Stockholm", "America/New_York", "Australia/Sydney"
  })
  void usesUtcSemantics(String zone) throws Exception {
    TimeZone original = TimeZone.getDefault();
    String property = System.getProperty("user.timezone");
    try {
      System.setProperty("user.timezone", zone);
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      writeFixture();
      for (boolean pushdown : List.of(true, false)) {
        try (var connection = DriverManager.getConnection(
            "jdbc:jparq:" + directory + "?predicatePushdown=" + pushdown + "&projectionPushdown=" + pushdown)) {
          assertPredicates(connection);
          assertKeysAndOrder(connection);
          assertCastsAndGetters(connection);
          assertCorrelations(connection);
          assertGroupedCorrelations(connection);
          assertLocalFieldCorrelations(connection);
          assertAggregateDeclarations(connection);
          assertHavingSubqueryDeclarations(connection);
          assertWindowDeclarations(connection);
          assertLocalPredicates(connection);
          assertCorrelatedRepresentations(connection);
          assertWindowOrder(connection);
          assertTimeGetters(connection);
          assertArithmeticRepresentation(connection);
        }
      }
    } finally {
      TimeZone.setDefault(original);
      if (property == null) {
        System.clearProperty("user.timezone");
      } else {
        System.setProperty("user.timezone", property);
      }
    }
  }

  /**
   * Checks comparison operators and subtraction use UTC values.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if a query fails
   */
  private static void assertPredicates(Connection connection) throws Exception {
    for (String predicate : List.of("CAST(o AS TIMESTAMP WITH TIME ZONE) = o",
        "o = CAST(o AS TIMESTAMP WITH TIME ZONE)", "CAST(o AS TIMESTAMP WITH TIME ZONE) IN (o)",
        "o IN (CAST(o AS TIMESTAMP WITH TIME ZONE))", "a = CAST(o AS TIMESTAMP WITH TIME ZONE)",
        "a IS NOT DISTINCT FROM CAST(o AS TIMESTAMP WITH TIME ZONE)",
        "CAST(o AS TIMESTAMP WITH TIME ZONE) IN (SELECT o FROM t)", "o IN (SELECT a FROM t)",
        "CASE o WHEN a THEN TRUE ELSE FALSE END", "NULLIF(o,a) IS NULL",
        "CAST(o AS TIMESTAMP WITH TIME ZONE) = ANY(SELECT o FROM t)", "o = ANY(SELECT a FROM t)",
        "o = ALL(ARRAY[a])")) {
      assertCount(connection, "SELECT COUNT(*) FROM t WHERE " + predicate, 2);
    }
    for (String predicate : List.of("a = b", "a IN (b)", "ta = tb", "ta IN (tb)", "a <= b")) {
      assertCount(connection, "SELECT COUNT(*) FROM t WHERE " + predicate, 1);
    }
    assertCount(connection, "SELECT COUNT(*) FROM t WHERE a < b", 0);
    try (var statement = connection.createStatement();
        var result = statement
            .executeQuery("SELECT CAST(o AS TIMESTAMP WITH TIME ZONE) - o,a - b,ta - tb FROM t WHERE id = 1")) {
      assertTrue(result.next());
      for (int column = 1; column <= 3; column++) {
        assertEquals(java.time.Duration.ZERO, ((TemporalInterval) result.getObject(column)).duration());
      }
    }
    assertEquals(0, SqlComparisons.typedCompare(OffsetTime.parse("00:30:00+01:00"), OffsetTime.parse("23:30:00Z")));
  }

  /**
   * Checks relational hashing and sorting agree with comparison equality.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if a query fails
   */
  private static void assertKeysAndOrder(Connection connection) throws Exception {
    for (String[] pair : List.of(new String[]{
        "a", "b"
    }, new String[]{
        "ta", "tb"
    }, new String[]{
        "ARRAY[a]", "ARRAY[b]"
    }, new String[]{
        "ARRAY[ta]", "ARRAY[tb]"
    })) {
      String first = "SELECT " + pair[0] + " AS x FROM t WHERE id = 1";
      String second = "SELECT " + pair[1] + " AS x FROM t WHERE id = 1";
      assertCount(connection, "SELECT COUNT(*) FROM (" + first + " UNION " + second + ") q", 1);
      assertCount(connection,
          "SELECT COUNT(*) FROM (SELECT DISTINCT x FROM (" + first + " UNION ALL " + second + ") q) d", 1);
      try (var statement = connection.createStatement();
          var result = statement
              .executeQuery("SELECT COUNT(*) FROM (" + first + " UNION ALL " + second + ") q GROUP BY x")) {
        assertTrue(result.next());
        assertEquals(2, result.getInt(1));
        assertFalse(result.next());
      }
    }
    try (var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT x FROM (SELECT a AS x FROM t WHERE id = 1 "
            + "UNION ALL SELECT b AS x FROM t WHERE id = 2) q ORDER BY x")) {
      assertTrue(result.next());
      assertEquals(Instant.parse("2024-07-01T09:30:00Z"), result.getObject(1, OffsetDateTime.class).toInstant());
      assertTrue(result.next());
      assertEquals(Instant.parse("2024-07-01T10:00:00Z"), result.getObject(1, OffsetDateTime.class).toInstant());
      assertFalse(result.next());
    }
  }

  /**
   * Checks timestamp-to-time casts keep offsets and JDBC getters convert
   * instants.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if a query fails
   */
  private static void assertCastsAndGetters(Connection connection) throws Exception {
    try (var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT CAST(o AS TIME WITH TIME ZONE),"
            + "CAST(CAST(o AS TIMESTAMP WITH TIME ZONE) AS TIME WITH TIME ZONE),"
            + "CAST(COALESCE(o,CURRENT_TIMESTAMP) AS TIME WITH TIME ZONE),"
            + "CAST(o AS TIMESTAMP WITH TIME ZONE),ta,edge,CURRENT_TIME FROM t WHERE id = 1")) {
      assertTrue(result.next());
      for (int column = 1; column <= 3; column++) {
        assertEquals(OffsetTime.parse("10:00:00Z"), result.getObject(column, OffsetTime.class));
      }
      Timestamp timestamp = Timestamp.from(Instant.parse("2024-07-01T10:00:00Z"));
      assertEquals(timestamp, result.getTimestamp(4));
      assertEquals(new java.sql.Date(timestamp.getTime()), result.getDate(4));
      assertEquals(DateTimeFunctions.toTime(timestamp), result.getTime(4));
      int displacement = result.getObject(7, OffsetTime.class).getOffset().getTotalSeconds();
      assertEquals(Time.valueOf(LocalTime.of(10, 0).plusSeconds(displacement)), result.getTime(5));
      Timestamp edge = Timestamp.from(result.getObject(6, OffsetDateTime.class).toInstant());
      assertEquals(new java.sql.Date(edge.getTime()), result.getDate(6));
      assertEquals(DateTimeFunctions.toTime(edge), result.getTime(6));
    }
  }

  /**
   * Keeps UTC provenance through correlated predicates and scalar subqueries.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if querying fails
   */
  private static void assertCorrelations(Connection connection) throws Exception {
    for (String predicate : List.of(
        "CAST(t.o AS TIMESTAMP WITH TIME ZONE) IN (SELECT i.o FROM correlated i WHERE i.id = t.id)",
        "CAST(t.o AS TIMESTAMP WITH TIME ZONE) = ANY(SELECT i.o FROM correlated i WHERE i.id = t.id)",
        "EXISTS(SELECT 1 FROM correlated i WHERE i.o = CAST(t.o AS TIMESTAMP WITH TIME ZONE))",
        "EXISTS(SELECT 1 FROM correlated i WHERE CAST(i.o AS TIMESTAMP WITH TIME ZONE) = t.o)")) {
      assertCount(connection, "SELECT COUNT(*) FROM correlated t WHERE " + predicate, 2);
    }
    try (var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT (SELECT COUNT(*) FROM correlated i "
            + "WHERE CAST(i.o AS TIMESTAMP WITH TIME ZONE) = t.o) FROM correlated t")) {
      for (int row = 0; row < 2; row++) {
        assertTrue(result.next());
        assertEquals(1, result.getInt(1));
      }
      assertFalse(result.next());
    }
  }

  /**
   * Keeps grouped column declarations in HAVING and scalar subquery bindings.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if querying fails
   */
  private static void assertGroupedCorrelations(Connection connection) throws Exception {
    String predicate = "EXISTS(SELECT 1 FROM correlated i WHERE CAST(i.o AS TIMESTAMP WITH TIME ZONE) = t.o)";
    String having = "SELECT id FROM correlated t GROUP BY id,o HAVING " + predicate;
    assertCount(connection, "SELECT COUNT(*) FROM (" + having + ") q", 2);
    for (String query : List.of(
        "SELECT id,(SELECT COUNT(*) FROM correlated i WHERE CAST(i.o AS TIMESTAMP WITH TIME ZONE) = t.o) "
            + "FROM correlated t GROUP BY id,o ORDER BY id",
        "SELECT id,0 + (SELECT COUNT(*) FROM correlated i WHERE CAST(i.o AS TIMESTAMP WITH TIME ZONE) = t.o) "
            + "FROM correlated t GROUP BY id,o ORDER BY id")) {
      try (var statement = connection.createStatement(); var result = statement.executeQuery(query)) {
        for (int id : List.of(1, 2)) {
          assertTrue(result.next(), query);
          assertEquals(id, result.getInt(1));
          assertEquals(1, result.getInt(2));
        }
        assertFalse(result.next());
      }
    }
    String rollup = "SELECT GROUPING(o),(SELECT COUNT(*) FROM correlated i "
        + "WHERE CAST(i.o AS TIMESTAMP WITH TIME ZONE) = t.o) FROM correlated t GROUP BY ROLLUP(o)";
    try (var statement = connection.createStatement(); var result = statement.executeQuery(rollup)) {
      int groups = 0;
      while (result.next()) {
        assertEquals(result.getInt(1) == 1 ? 0 : 1, result.getInt(2));
        groups++;
      }
      assertEquals(3, groups);
    }
  }

  /**
   * Binds raw local fields even when JDBC Timestamp would normalize a DST gap.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if querying fails
   */
  private static void assertLocalFieldCorrelations(Connection connection) throws Exception {
    String inner = "SELECT COUNT(*) FROM t i WHERE CAST(i.l AS TIMESTAMP WITH TIME ZONE) = t.l";
    for (String suffix : List.of("", " GROUP BY id,l")) {
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT (" + inner + ") FROM t" + suffix)) {
        for (int row = 0; row < 2; row++) {
          assertTrue(result.next());
          assertEquals(2, result.getInt(1));
        }
        assertFalse(result.next());
      }
    }
    assertCount(connection,
        "SELECT COUNT(*) FROM t WHERE EXISTS(SELECT 1 FROM t i " + "WHERE CAST(i.l AS TIMESTAMP WITH TIME ZONE) = t.l)",
        2);
    assertCount(connection, "SELECT COUNT(*) FROM (SELECT id FROM t GROUP BY id,l "
        + "HAVING EXISTS(SELECT 1 FROM t i WHERE CAST(i.l AS TIMESTAMP WITH TIME ZONE) = t.l)) q", 2);
  }

  /**
   * Aggregate comparisons retain MIN/MAX input declarations in both directions.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if querying fails
   */
  private static void assertAggregateDeclarations(Connection connection) throws Exception {
    for (String aggregate : List.of("MIN", "MAX")) {
      String nullable = "SELECT CAST(n AS TIMESTAMP WITH TIME ZONE) = " + aggregate
          + "(n) FROM correlated GROUP BY id,n ORDER BY id";
      try (var statement = connection.createStatement(); var result = statement.executeQuery(nullable)) {
        assertTrue(result.next(), nullable);
        assertNull(result.getObject(1), nullable);
        assertTrue(result.next(), nullable);
        assertTrue(result.getBoolean(1), nullable);
        assertFalse(result.next(), nullable);
      }
      for (String expression : List.of("CAST(o AS TIMESTAMP WITH TIME ZONE) = " + aggregate + "(o)",
          aggregate + "(o) = CAST(o AS TIMESTAMP WITH TIME ZONE)",
          aggregate + "(CAST(o AS TIMESTAMP WITH TIME ZONE)) = o")) {
        assertCount(connection,
            "SELECT COUNT(*) FROM (SELECT o FROM correlated GROUP BY o HAVING " + expression + ") q", 2);
        try (var statement = connection.createStatement();
            var result = statement.executeQuery("SELECT " + expression + " FROM correlated GROUP BY o")) {
          int rows = 0;
          while (result.next()) {
            assertTrue(result.getBoolean(1), expression);
            rows++;
          }
          assertEquals(2, rows, expression);
        }
      }
    }
  }

  /**
   * HAVING retains subquery declarations for scalar and quantified comparisons.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if querying fails
   */
  private static void assertHavingSubqueryDeclarations(Connection connection) throws Exception {
    String offset = "CAST(o AS TIMESTAMP WITH TIME ZONE)";
    String[] predicates = {
        offset + " = ANY (SELECT i.o FROM correlated i)", offset + " = ANY (SELECT MAX(i.o) FROM correlated i)",
        offset + " = ALL (SELECT i.o FROM correlated i WHERE i.id = 1)",
        offset + " = (SELECT MAX(i.o) FROM correlated i)", offset + " IN (SELECT i.o FROM correlated i)",
        offset + " = ANY (SELECT i.o FROM correlated i WHERE i.o = t.o)",
        offset + " = (SELECT MAX(i.o) FROM correlated i WHERE i.o = t.o)",
        offset + " = (SELECT i.n FROM correlated i WHERE i.id = 1)"
    };
    int[] counts = {
        2, 1, 1, 1, 2, 2, 2, 0
    };
    for (int index = 0; index < predicates.length; index++) {
      assertCount(connection,
          "SELECT COUNT(*) FROM (SELECT o FROM correlated t GROUP BY o HAVING " + predicates[index] + ") q",
          counts[index]);
      assertCount(connection, "SELECT COUNT(*) FROM correlated t WHERE " + predicates[index], counts[index]);
    }
  }

  /**
   * Value-returning windows retain UTC input declarations in scalar predicates.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if querying fails
   */
  private static void assertWindowDeclarations(Connection connection) throws Exception {
    String offset = "CAST(o AS TIMESTAMP WITH TIME ZONE)";
    String query = "SELECT id," + offset + " = FIRST_VALUE(o) OVER (ORDER BY id)," + offset
        + " = MIN(o) OVER (PARTITION BY id)," + offset + " = MAX(o) OVER ()," + offset
        + " = LAST_VALUE(o) OVER (ORDER BY id)," + offset + " = NTH_VALUE(o,1) OVER (ORDER BY id)," + offset
        + " - INTERVAL '1' HOUR = LAG(o) OVER (ORDER BY id)," + offset
        + " + INTERVAL '1' HOUR = LEAD(o) OVER (ORDER BY id) FROM correlated ORDER BY id";
    try (var statement = connection.createStatement(); var result = statement.executeQuery(query)) {
      assertTrue(result.next(), query);
      assertEquals(1, result.getInt(1));
      for (int column : List.of(2, 3, 5, 6, 8)) {
        assertEquals(Boolean.TRUE, result.getObject(column), query + " column " + column);
      }
      assertEquals(Boolean.FALSE, result.getObject(4), query);
      assertNull(result.getObject(7), query);
      assertTrue(result.next(), query);
      assertEquals(2, result.getInt(1));
      for (int column : List.of(3, 4, 5, 7)) {
        assertEquals(Boolean.TRUE, result.getObject(column), query + " column " + column);
      }
      for (int column : List.of(2, 6)) {
        assertEquals(Boolean.FALSE, result.getObject(column), query + " column " + column);
      }
      assertNull(result.getObject(8), query);
      assertFalse(result.next(), query);
    }
  }

  /**
   * Ordinary local comparisons use the same engine representation on both sides.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if querying fails
   */
  private static void assertLocalPredicates(Connection connection) throws Exception {
    for (String column : List.of("l", "w")) {
      String literal = column.equals("l") ? "2024-03-31 02:30:00" : "2024-01-02 03:04:05";
      for (String predicate : List.of("COALESCE(" + column + "," + column + ") = " + column,
          "COALESCE(" + column + "," + column + ") = TIMESTAMP '" + literal + "'",
          "CASE WHEN id > 0 THEN " + column + " END BETWEEN " + column + " AND " + column,
          "COALESCE(" + column + "," + column + ") IN (" + column + ")")) {
        assertCount(connection, "SELECT COUNT(*) FROM t WHERE " + predicate, 2);
      }
    }
  }

  /**
   * Correlations preserve JDBC values, classes, arithmetic, and character forms.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if querying fails
   */
  private static void assertCorrelatedRepresentations(Connection connection) throws Exception {
    for (String column : List.of("l", "w")) {
      for (String suffix : List.of("", " GROUP BY " + column)) {
        String inner = " FROM t i WHERE i.id = 1)";
        String query = "SELECT " + column + ",(SELECT t." + column + inner + ",CAST(" + column
            + " AS VARCHAR(40)),(SELECT CAST(t." + column + " AS VARCHAR(40))" + inner + "," + column
            + " + INTERVAL '1' HOUR,(SELECT t." + column + " + INTERVAL '1' HOUR" + inner + " FROM t" + suffix;
        try (var statement = connection.createStatement(); var result = statement.executeQuery(query)) {
          int rows = 0;
          while (result.next()) {
            assertInstanceOf(Timestamp.class, result.getObject(2), query);
            assertEquals(result.getObject(1), result.getObject(2), query);
            if (!suffix.isEmpty()) {
              assertEquals(result.getMetaData().getColumnClassName(1), result.getMetaData().getColumnClassName(2),
                  query);
            }
            assertEquals(result.getString(3), result.getString(4), query);
            assertInstanceOf(Timestamp.class, result.getObject(6), query);
            assertEquals(result.getObject(5), result.getObject(6), query);
            rows++;
          }
          assertEquals(suffix.isEmpty() ? 2 : 1, rows, query);
        }
      }
    }
  }

  /**
   * Shares instant ordering for window ranks, peers, and extrema.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if querying fails
   */
  private static void assertWindowOrder(Connection connection) throws Exception {
    for (String[] columns : List.of(new String[]{
        "a", "b"
    }, new String[]{
        "ta", "tb"
    })) {
      String source = "SELECT " + columns[0] + " AS x FROM t WHERE id = 1 UNION ALL SELECT " + columns[1]
          + " AS x FROM t WHERE id = 1 UNION ALL SELECT " + columns[1] + " AS x FROM t WHERE id = 2";
      try (var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT x,ROW_NUMBER() OVER(ORDER BY x),RANK() OVER(ORDER BY x),"
              + "DENSE_RANK() OVER(ORDER BY x),MIN(x) OVER(),MAX(x) OVER(),COUNT(*) OVER(PARTITION BY x) " + "FROM ("
              + source + ") q ORDER BY x")) {
        var rowNumbers = new HashSet<Integer>();
        for (int index = 0; index < 3; index++) {
          assertTrue(result.next());
          assertEquals(index == 0 ? 1 : 2, result.getInt(3));
          assertEquals(index == 0 ? 1 : 2, result.getInt(4));
          assertEquals(index == 0 ? 1 : 2, result.getInt(7));
          if (index == 0) {
            assertEquals(1, result.getInt(2));
          } else {
            rowNumbers.add(result.getInt(2));
          }
          Object minimum = result.getObject(5);
          Object maximum = result.getObject(6);
          if (minimum instanceof OffsetDateTime timestamp) {
            assertEquals(Instant.parse("2024-07-01T09:30:00Z"), timestamp.toInstant());
            assertEquals(Instant.parse("2024-07-01T10:00:00Z"), ((OffsetDateTime) maximum).toInstant());
          } else {
            assertEquals(LocalTime.of(9, 30),
                ((OffsetTime) minimum).withOffsetSameInstant(ZoneOffset.UTC).toLocalTime());
            assertEquals(LocalTime.of(10, 0),
                ((OffsetTime) maximum).withOffsetSameInstant(ZoneOffset.UTC).toLocalTime());
          }
        }
        assertEquals(Set.of(2, 3), rowNumbers);
        assertFalse(result.next());
      }
    }
  }

  /**
   * Checks fixed SQL clock values rather than reproducing a date-anchor formula.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if querying fails
   */
  private static void assertTimeGetters(Connection connection) throws Exception {
    try (var statement = connection.createStatement();
        var result = statement
            .executeQuery("SELECT LOCALTIME,CURRENT_TIME," + "CAST('10:00:00' AS TIME WITH TIME ZONE)")) {
      assertTrue(result.next());
      assertEquals(result.getTime(1), result.getTime(2));
      assertEquals(Time.valueOf("10:00:00"), result.getTime(3));
    }
  }

  /**
   * Keeps the same JDBC class and SQL character form for addition and
   * subtraction. Local-field normalization in DST gaps remains tracked separately
   * in #446.
   *
   * @param connection
   *          open connection
   * @throws Exception
   *           if querying fails
   */
  private static void assertArithmeticRepresentation(Connection connection) throws Exception {
    String minus = "l - INTERVAL '1' HOUR";
    String plus = "l + INTERVAL '1' HOUR";
    try (var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT " + minus + "," + plus + ",CAST(" + minus
            + " AS VARCHAR(100)),CAST(" + plus + " AS VARCHAR(100)) FROM t")) {
      while (result.next()) {
        for (int column = 1; column <= 2; column++) {
          assertInstanceOf(Timestamp.class, result.getObject(column));
          String text = result.getString(column);
          assertFalse(text.contains("T"));
          assertTrue(text.matches("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.*"));
          assertEquals(text, result.getString(column + 2));
        }
      }
    }
  }

  /**
   * Checks a scalar count query.
   *
   * @param connection
   *          open connection
   * @param sql
   *          scalar count query
   * @param expected
   *          expected count
   * @throws Exception
   *           if querying fails
   */
  private static void assertCount(Connection connection, String sql, int expected) throws Exception {
    try (var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
      assertTrue(result.next(), sql);
      assertEquals(expected, result.getInt(1), sql);
    }
  }

  /**
   * Writes UTC-adjusted storage and native offset columns with equal instants.
   *
   * @throws Exception
   *           if fixture creation fails
   */
  private void writeFixture() throws Exception {
    writeFixture("t", false);
    writeFixture("correlated", true);
  }

  /**
   * Writes a physical table, optionally using distinct UTC instants per row.
   *
   * @param table
   *          table file name
   * @param distinct
   *          whether each row has a different UTC instant
   * @throws Exception
   *           if fixture creation fails
   */
  private void writeFixture(String table, boolean distinct) throws Exception {
    Schema schema = SchemaBuilder.record("offsets").fields().requiredInt("id").name("o")
        .type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault().name("a")
        .type(TemporalValueType.OFFSET_TIMESTAMP.schema()).noDefault().name("b")
        .type(TemporalValueType.OFFSET_TIMESTAMP.schema()).noDefault().name("ta")
        .type(TemporalValueType.OFFSET_TIME.schema()).noDefault().name("tb")
        .type(TemporalValueType.OFFSET_TIME.schema()).noDefault().name("edge")
        .type(TemporalValueType.OFFSET_TIMESTAMP.schema()).noDefault().name("l")
        .type(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault().name("w")
        .type(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault().name("n")
        .type(Schema.createUnion(Schema.create(Schema.Type.NULL),
            LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))))
        .noDefault().endRecord();
    Configuration configuration = new Configuration();
    var output = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(directory.resolve(table + ".parquet").toUri()),
        configuration);
    try (var writer = AvroParquetWriter.<GenericData.Record>builder(output).withSchema(schema).withConf(configuration)
        .build()) {
      for (int id : List.of(1, 2)) {
        var row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("o",
            (Instant.parse("2024-07-01T10:00:00Z").getEpochSecond() + (distinct ? (id - 1) * 3600L : 0)) * 1_000_000L);
        row.put("l", LocalDateTime.parse("2024-03-31T02:30:00").toEpochSecond(ZoneOffset.UTC) * 1_000_000L);
        row.put("n", id == 1 ? null : Instant.parse("2024-07-01T10:00:00Z").getEpochSecond() * 1_000_000L);
        row.put("w", LocalDateTime.parse("2024-01-02T03:04:05").toEpochSecond(ZoneOffset.UTC) * 1_000_000L);
        row.put("a", "2024-07-01T10:00:00Z");
        row.put("b", id == 1 ? "2024-07-01T11:00:00+01:00" : "2024-07-01T10:30:00+01:00");
        row.put("edge", "2024-07-01T00:30:00+14:00");
        row.put("ta", "10:00:00Z");
        row.put("tb", id == 1 ? "11:00:00+01:00" : "10:30:00+01:00");
        writer.write(row);
      }
    }
  }
}
