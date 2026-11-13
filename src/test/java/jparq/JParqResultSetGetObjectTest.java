package jparq;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.Array;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.engine.RecordReader;
import se.alipsa.jparq.engine.SqlParser;
import se.alipsa.jparq.engine.SubqueryExecutor;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/**
 * Verifies the typed {@code getObject} overloads of {@link JParqResultSet}.
 */
class JParqResultSetGetObjectTest {

  /**
   * Exposes the typed values used by the getObject tests.
   *
   * @return a positioned result over one row with array, numeric, temporal,
   *         string, null, and struct columns
   * @throws Exception
   *           if setup fails
   */
  private static JParqResultSet positionedResult() throws Exception {
    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema pointSchema = SchemaBuilder.record("point").namespace("example").fields().requiredInt("x").endRecord();
    Schema schema = SchemaBuilder.record("Row").fields().name("a").type().array().items().intType().noDefault()
        .requiredInt("n").requiredLong("lng").name("d").type(dateSchema).noDefault().name("t").type(timeSchema)
        .noDefault().requiredString("s").optionalInt("nullable").name("st").type(pointSchema).noDefault().endRecord();
    var row = new GenericData.Record(schema);
    row.put("a", List.of(1));
    row.put("n", 42);
    row.put("lng", 84L);
    row.put("d", (int) LocalDate.of(2026, 10, 26).toEpochDay());
    row.put("t", (int) (LocalTime.of(12, 34).toNanoOfDay() / 1_000_000L));
    row.put("s", "hello");
    row.put("nullable", null);
    var point = new GenericData.Record(pointSchema);
    point.put("x", 7);
    row.put("st", point);
    RecordReader reader = mock(RecordReader.class);
    when(reader.read()).thenReturn(row).thenReturn(null);
    var select = SqlParser.parseSelect("SELECT a, n, lng, d, t, s, nullable, st FROM input");
    var result = new JParqResultSet(reader, select, "input", "PUBLIC", null, null,
        List.of("a", "n", "lng", "d", "t", "s", "nullable", "st"),
        List.of("a", "n", "lng", "d", "t", "s", "nullable", "st"), mock(SubqueryExecutor.class));
    assertTrue(result.next());
    return result;
  }

  @Test
  void typedGetObjectReturnsIdentityValues() throws Exception {
    try (JParqResultSet result = positionedResult()) {
      Object[] array = result.getObject(1, Object[].class);
      assertArrayEquals(new Object[]{
          1
      }, array);
      assertEquals(42, result.getObject(2, Integer.class));
      assertEquals(42, result.getObject("n", Integer.class));
      assertEquals("hello", result.getObject(6, String.class));
      assertEquals("hello", result.getObject("s", String.class));
    }
  }

  @Test
  void typedGetObjectConvertsNumericsAndTemporals() throws Exception {
    try (JParqResultSet result = positionedResult()) {
      assertEquals(42L, result.getObject(2, Long.class));
      assertEquals(42.0d, result.getObject(2, Double.class));
      assertEquals(BigDecimal.valueOf(42), result.getObject(2, BigDecimal.class));
      assertEquals(LocalDate.of(2026, 10, 26), result.getObject(4, LocalDate.class));
      assertEquals(Date.valueOf(LocalDate.of(2026, 10, 26)), result.getObject(4, Date.class));
      assertEquals(Timestamp.valueOf(LocalDateTime.of(2026, 10, 26, 0, 0)), result.getObject(4, Timestamp.class));
      assertEquals(LocalDateTime.of(2026, 10, 26, 0, 0), result.getObject(4, LocalDateTime.class));
      OffsetDateTime expectedOffsetDateTime = Timestamp.valueOf(LocalDateTime.of(2026, 10, 26, 0, 0)).toInstant()
          .atOffset(ZoneOffset.UTC);
      assertEquals(expectedOffsetDateTime, result.getObject(4, OffsetDateTime.class));
      assertEquals(LocalTime.of(12, 34), result.getObject(5, LocalTime.class));
      OffsetTime expectedOffsetTime = LocalTime.of(12, 34).atOffset(DateTimeFunctions.sessionOffset())
          .withOffsetSameInstant(ZoneOffset.UTC);
      assertEquals(expectedOffsetTime, result.getObject(5, OffsetTime.class));
    }
  }

  @Test
  void typedGetObjectSupportsArrayTarget() throws Exception {
    try (JParqResultSet result = positionedResult()) {
      Array array = result.getObject(1, Array.class);
      assertArrayEquals(new Object[]{
          1
      }, (Object[]) array.getArray());
      Array byLabel = result.getObject("a", Array.class);
      assertArrayEquals(new Object[]{
          1
      }, (Object[]) byLabel.getArray());
      SQLDataException failure = assertThrows(SQLDataException.class, () -> result.getObject(2, Array.class));
      assertEquals("22005", failure.getSQLState());
    }
  }

  @Test
  void typedGetObjectReturnsNullForSqlNull() throws Exception {
    try (JParqResultSet result = positionedResult()) {
      assertNull(result.getObject(7, Integer.class));
      assertNull(result.getObject("nullable", String.class));
    }
  }

  @Test
  void typedGetObjectRejectsUnsupportedConversions() throws Exception {
    try (JParqResultSet result = positionedResult()) {
      SQLDataException numericFailure = assertThrows(SQLDataException.class,
          () -> result.getObject(6, BigInteger.class));
      assertEquals("22005", numericFailure.getSQLState());
      SQLDataException textFailure = assertThrows(SQLDataException.class, () -> result.getObject(6, Integer.class));
      assertEquals("22018", textFailure.getSQLState());
      assertEquals("22005",
          assertThrows(SQLException.class, () -> result.getObject(2, (Class<Integer>) null)).getSQLState());
    }
  }

  @Test
  void mapGetObjectUsesStandardMapping() throws Exception {
    try (JParqResultSet result = positionedResult()) {
      assertEquals(42, result.getObject(2, Map.of()));
      assertEquals(42, result.getObject("n", Map.of()));
      assertEquals(42, result.getObject(2, (Map<String, Class<?>>) null));
      assertEquals(42, result.getObject(2, Map.of("unrelated", String.class)));
      Object struct = result.getObject(8);
      assertEquals(struct, result.getObject(8, Map.of()));
      assertEquals(struct, result.getObject(8, Map.of("unrelated", String.class)));
    }
  }

  @Test
  void mapGetObjectRejectsMatchingStructMapping() throws Exception {
    try (JParqResultSet result = positionedResult()) {
      String typeName = result.getMetaData().getColumnTypeName(8);
      SQLFeatureNotSupportedException failure = assertThrows(SQLFeatureNotSupportedException.class,
          () -> result.getObject(8, Map.of(typeName, String.class)));
      assertEquals("0A000", failure.getSQLState());
    }
  }

  @Test
  void mapGetObjectReportsInvalidColumnBeforeTypeMap() throws Exception {
    try (JParqResultSet result = positionedResult()) {
      SQLException failure = assertThrows(SQLException.class, () -> result.getObject(99, Map.of()));
      assertFalse(failure instanceof SQLFeatureNotSupportedException);
    }
  }

  @Test
  void typedGetObjectThroughDriver(@TempDir Path directory) throws Exception {
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory.toAbsolutePath());
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT ARRAY[1] a, 42 n")) {
      assertTrue(result.next());
      assertArrayEquals(new Object[]{
          1
      }, (Object[]) result.getObject(1));
      assertEquals(42, result.getObject(2, Integer.class));
      assertEquals(42, result.getObject("n", Integer.class));
      Array sqlArray = result.getObject(1, Array.class);
      assertArrayEquals(new Object[]{
          1
      }, (Object[]) sqlArray.getArray());
      assertEquals(Map.of(), connection.getTypeMap());
      assertEquals(42, result.getObject(2, connection.getTypeMap()));
      assertEquals(42, result.getObject(2, (Map<String, Class<?>>) null));
      assertEquals(42, result.getObject("n", Map.of("unrelated", String.class)));
    }
  }
}
