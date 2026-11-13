package jparq;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqResultSet;
import se.alipsa.jparq.engine.RecordReader;
import se.alipsa.jparq.engine.SqlParser;

/**
 * Verifies the typed {@code getObject} overloads of {@link JParqResultSet}.
 */
class JParqResultSetGetObjectTest {

  /**
   * Exposes the typed values used by the getObject tests.
   *
   * @return a positioned result over one row with array, numeric, temporal,
   *         string, and null columns
   * @throws Exception
   *           if setup fails
   */
  private static JParqResultSet positionedResult() throws Exception {
    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema schema = SchemaBuilder.record("Row").fields().name("a").type().array().items().intType().noDefault()
        .requiredInt("n").requiredLong("lng").name("d").type(dateSchema).noDefault().name("t").type(timeSchema)
        .noDefault().requiredString("s").optionalInt("nullable").endRecord();
    var row = new GenericData.Record(schema);
    row.put("a", List.of(1));
    row.put("n", 42);
    row.put("lng", 84L);
    row.put("d", (int) LocalDate.of(2026, 10, 26).toEpochDay());
    row.put("t", (int) (LocalTime.of(12, 34).toNanoOfDay() / 1_000_000L));
    row.put("s", "hello");
    row.put("nullable", null);
    RecordReader reader = mock(RecordReader.class);
    when(reader.read()).thenReturn(row).thenReturn(null);
    var select = SqlParser.parseSelect("SELECT a, n, lng, d, t, s, nullable FROM input");
    var result = new JParqResultSet(reader, select, "input", "PUBLIC", null, null,
        List.of("a", "n", "lng", "d", "t", "s", "nullable"), List.of("a", "n", "lng", "d", "t", "s", "nullable"), null);
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
      assertEquals(LocalDate.of(2026, 10, 26), result.getObject(4, LocalDate.class));
      assertEquals(Date.valueOf(LocalDate.of(2026, 10, 26)), result.getObject(4, Date.class));
      assertEquals(LocalDateTime.of(2026, 10, 26, 0, 0), result.getObject(4, LocalDateTime.class));
      assertEquals(LocalTime.of(12, 34), result.getObject(5, LocalTime.class));
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
      assertThrows(SQLException.class, () -> result.getObject(5, BigInteger.class));
      assertThrows(SQLException.class, () -> result.getObject(5, Integer.class));
      assertEquals("22005",
          assertThrows(SQLException.class, () -> result.getObject(2, (Class<Integer>) null)).getSQLState());
    }
  }

  @Test
  void mapGetObjectSignalsUnsupportedTypeMaps() throws Exception {
    try (JParqResultSet result = positionedResult()) {
      Map<String, Class<?>> typeMap = Map.of();
      assertThrows(SQLFeatureNotSupportedException.class, () -> result.getObject(2, typeMap));
      assertThrows(SQLFeatureNotSupportedException.class, () -> result.getObject("n", typeMap));
    }
  }
}
