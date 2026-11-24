package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.InMemoryRecordReader;
import se.alipsa.jparq.engine.MaterializedNames;
import se.alipsa.jparq.engine.QueryProcessor;
import se.alipsa.jparq.engine.SqlParser;

/**
 * Pins the buffered sort's key resolution: positional references are validated
 * against the fields a wildcard projection exposes, and an unresolvable column
 * reference fails instead of degrading to a no-op sort that compares every key
 * equal.
 */
class QueryProcessorOrderingTest {

  /**
   * Addresses positional references against the visible select list of a schema
   * that carries a hidden column, as a USING join does for the peer copy of a
   * join column: the hidden field takes no ordinal position.
   *
   * @throws Exception
   *           if processing fails
   */
  @Test
  void positionsAddressVisibleFieldsOnly() throws Exception {
    Schema schema = usingJoinSchema();
    List<GenericRecord> records = List.of(record(schema, 1, 30, 5), record(schema, 2, 10, 6), record(schema, 3, 20, 4));
    assertEquals(List.of(3, 1, 2), sortedIds(records, "3", true));
    // The hidden field takes no ordinal position, so position 4 is outside
    // the three visible columns even though the record holds four fields.
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> newProcessor(records, "4", true));
    assertEquals("ORDER BY position is outside the select list: 4", failure.getMessage());
    assertEquals("42000", stateOf(failure));
  }

  /**
   * Resolves a quoted identifier case-sensitively to its physical field, and
   * fails an ordering column that resolves to no field with the syntax
   * classification instead of comparing every key equal.
   *
   * @throws Exception
   *           if processing fails
   */
  @Test
  void unresolvableColumnsFailInsteadOfSortingNothing() throws Exception {
    Schema schema = Schema.createRecord("rows", null, null, false);
    schema.setFields(List.of(new Schema.Field("n", Schema.create(Schema.Type.INT), null, null)));
    List<GenericRecord> records = List.of(record(schema, 2), record(schema, 1), record(schema, 3));
    assertEquals(List.of(1, 2, 3), sortedValues(records, "n", true));
    // A quoted identifier resolves to the physical field with the same
    // declared case.
    assertEquals(List.of(3, 2, 1), sortedValues(records, "\"n\"", false));
    for (String reference : List.of("nosuch", "\"N\"")) {
      IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
          () -> newProcessor(records, reference, true));
      assertEquals("Unknown column for ORDER BY: " + reference, failure.getMessage());
      assertEquals("42000", stateOf(failure));
    }
  }

  /**
   * Emits the {@code id} values of a processor over the given rows, sorted by the
   * given reference.
   *
   * @param records
   *          buffered input rows
   * @param reference
   *          the ORDER BY column reference as written
   * @param ascending
   *          whether to order ascending
   * @return the emitted ids in output order
   * @throws Exception
   *           if processing fails
   */
  private static List<Integer> sortedIds(List<GenericRecord> records, String reference, boolean ascending)
      throws Exception {
    List<Integer> ids = new ArrayList<>();
    try (QueryProcessor processor = newProcessor(records, reference, ascending)) {
      GenericRecord record;
      while ((record = processor.nextMatching()) != null) {
        ids.add((Integer) record.get("id"));
      }
    }
    return ids;
  }

  /**
   * Emits the {@code n} values of a processor over the given rows, sorted by the
   * given reference.
   *
   * @param records
   *          buffered input rows
   * @param reference
   *          the ORDER BY column reference as written
   * @param ascending
   *          whether to order ascending
   * @return the emitted values in output order
   * @throws Exception
   *           if processing fails
   */
  private static List<Integer> sortedValues(List<GenericRecord> records, String reference, boolean ascending)
      throws Exception {
    List<Integer> values = new ArrayList<>();
    try (QueryProcessor processor = newProcessor(records, reference, ascending)) {
      GenericRecord record;
      while ((record = processor.nextMatching()) != null) {
        values.add((Integer) record.get("n"));
      }
    }
    return values;
  }

  /**
   * Builds a processor over the given records ordered by the given column
   * reference.
   *
   * @param records
   *          buffered input rows
   * @param reference
   *          the ORDER BY column reference as written
   * @param ascending
   *          whether to order ascending
   * @return the constructed processor
   */
  private static QueryProcessor newProcessor(List<GenericRecord> records, String reference, boolean ascending) {
    return new QueryProcessor(new InMemoryRecordReader(records), List.of("*"), null, -1,
        QueryProcessor.Options.builder().schema(records.getFirst().getSchema())
            .orderBy(List.of(new SqlParser.OrderKey(reference, ascending, null, null, null))));
  }

  /**
   * Builds a record with the fields of a USING join schema: the merged key, the
   * left value, the hidden peer copy of the key, and the right value.
   *
   * @param schema
   *          join schema to populate
   * @param id
   *          merged key value
   * @param leftValue
   *          left table value
   * @param rightValue
   *          right table value
   * @return the populated record
   */
  private static GenericRecord record(Schema schema, int id, int leftValue, int rightValue) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", id);
    record.put("a__v", leftValue);
    record.put("b__id", id);
    record.put("b__v", rightValue);
    return record;
  }

  /**
   * Builds a record with a single {@code n} field.
   *
   * @param schema
   *          schema to populate
   * @param value
   *          the field value
   * @return the populated record
   */
  private static GenericRecord record(Schema schema, int value) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("n", value);
    return record;
  }

  /**
   * Builds the shape of a USING join record schema: visible merged key and values
   * around a hidden peer copy of the key.
   *
   * @return the join schema
   */
  private static Schema usingJoinSchema() {
    Schema schema = Schema.createRecord("join_record", null, null, false);
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("id", Schema.create(Schema.Type.INT), null, null));
    fields.add(new Schema.Field("a__v", Schema.create(Schema.Type.INT), null, null));
    Schema.Field hidden = new Schema.Field("b__id", Schema.create(Schema.Type.INT), null, null);
    hidden.addProp(MaterializedNames.QUALIFIED_ONLY, "true");
    fields.add(hidden);
    fields.add(new Schema.Field("b__v", Schema.create(Schema.Type.INT), null, null));
    schema.setFields(fields);
    return schema;
  }

  /**
   * Reads the SQLState carried by the cause of an engine classification failure.
   *
   * @param failure
   *          the engine failure to inspect
   * @return the SQLState of its {@link SQLException} cause
   */
  private static String stateOf(IllegalArgumentException failure) {
    Throwable cause = failure.getCause();
    assertTrue(cause instanceof SQLException);
    return ((SQLException) cause).getSQLState();
  }
}
