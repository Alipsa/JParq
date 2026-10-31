package jparq.aggregate;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.AggregateResultSetMetaData;
import se.alipsa.jparq.helper.IntervalType;
import se.alipsa.jparq.helper.TemporalInterval;

public class AggregateResultSetMetaDataTest {

  private AggregateResultSetMetaData metaData;
  private List<String> labels;
  private List<Integer> sqlTypes;

  @BeforeEach
  void setUp() {
    labels = List.of("col1", "col2", "col3");
    sqlTypes = List.of(Types.VARCHAR, Types.INTEGER, Types.BIGINT);
    metaData = new AggregateResultSetMetaData(labels, sqlTypes, "test_table", "test_schema", "test_catalog");
  }

  @Test
  void testGetColumnCount() {
    assertEquals(3, metaData.getColumnCount());
  }

  @Test
  void testGetColumnLabel() {
    assertEquals("col1", metaData.getColumnLabel(1));
    assertEquals("col2", metaData.getColumnLabel(2));
    assertEquals("col3", metaData.getColumnLabel(3));
  }

  @Test
  void testGetColumnName() {
    assertEquals("col1", metaData.getColumnName(1));
    assertEquals("col2", metaData.getColumnName(2));
    assertEquals("col3", metaData.getColumnName(3));
  }

  @Test
  void testGetTableName() {
    assertEquals("test_table", metaData.getTableName(1));
  }

  @Test
  void testGetSchemaName() {
    assertEquals("test_schema", metaData.getSchemaName(1));
  }

  @Test
  void testGetSchemaNameNull() {
    metaData = new AggregateResultSetMetaData(labels, sqlTypes, "test_table", null, "test_catalog");
    assertEquals("", metaData.getSchemaName(1));
  }

  @Test
  void testGetCatalogName() {
    assertEquals("test_catalog", metaData.getCatalogName(1));
  }

  @Test
  void testGetCatalogNameNull() {
    metaData = new AggregateResultSetMetaData(labels, sqlTypes, "test_table", "test_schema", null);
    assertEquals("", metaData.getCatalogName(1));
  }

  @Test
  void testGetColumnType() {
    assertEquals(Types.VARCHAR, metaData.getColumnType(1));
    assertEquals(Types.INTEGER, metaData.getColumnType(2));
    assertEquals(Types.BIGINT, metaData.getColumnType(3));
  }

  @Test
  void testGetColumnTypeEmpty() {
    metaData = new AggregateResultSetMetaData(labels, Collections.emptyList(), "test_table", "test_schema",
        "test_catalog");
    assertEquals(Types.OTHER, metaData.getColumnType(1));
  }

  @Test
  void testGetColumnTypeOutOfBounds() {
    assertEquals(Types.OTHER, metaData.getColumnType(0));
    assertEquals(Types.OTHER, metaData.getColumnType(4));
  }

  @Test
  void testGetColumnTypeNull() {
    sqlTypes = Arrays.asList(Types.VARCHAR, null, Types.BIGINT);
    metaData = new AggregateResultSetMetaData(labels, sqlTypes, "test_table", "test_schema", "test_catalog");
    assertEquals(Types.OTHER, metaData.getColumnType(2));
  }

  @Test
  void testGetColumnTypeName() {
    assertEquals(JDBCType.VARCHAR.getName(), metaData.getColumnTypeName(1));
    assertEquals(JDBCType.INTEGER.getName(), metaData.getColumnTypeName(2));
    assertEquals(JDBCType.BIGINT.getName(), metaData.getColumnTypeName(3));
  }

  @Test
  void testIsNullable() {
    assertEquals(AggregateResultSetMetaData.columnNullableUnknown, metaData.isNullable(1));
  }

  /**
   * Combines the largest integer part and fractional scale across decimal rows.
   */
  @Test
  void decimalBoundsCoverEveryMaterializedValue() {
    var metadata = new AggregateResultSetMetaData(List.of("amount", "id"), List.of(Types.DECIMAL, Types.INTEGER),
        "totals", null, null,
        List.of(List.of(new BigDecimal("1.25"), 1), List.of(new BigDecimal("10000.0"), 2), Arrays.asList(null, 3)));
    assertEquals(7, metadata.getPrecision(1));
    assertEquals(2, metadata.getScale(1));
    assertEquals(10, metadata.getPrecision(2));
    assertEquals(0, metadata.getScale(2));
  }
  /**
   * Covers small fractions, negative scales, zero, and columns without values.
   */
  @Test
  void decimalBoundsHandleFractionalAndScientificValues() {
    var metadata = new AggregateResultSetMetaData(List.of("mixed", "zero", "empty"),
        List.of(Types.DECIMAL, Types.NUMERIC, Types.DECIMAL), "totals", null, null,
        List.of(Arrays.asList(new BigDecimal("0.05"), new BigDecimal("0.0000"), null),
            Arrays.asList(new BigDecimal("1E+3"), null, null)));
    assertEquals(6, metadata.getPrecision(1));
    assertEquals(2, metadata.getScale(1));
    assertEquals(4, metadata.getPrecision(2));
    assertEquals(4, metadata.getScale(2));
    assertEquals(0, metadata.getPrecision(3));
    assertEquals(0, metadata.getScale(3));
  }

  /**
   * Copies inferred decimal and interval metadata after the input rows are
   * discarded, including repeated copies used after metadata invalidation.
   */
  @Test
  void inferredMetadataCanBeCopiedWithoutMaterializedRows() {
    List<List<Object>> rows = new ArrayList<>();
    rows.add(List.of(new BigDecimal("1.25"), TemporalInterval.parse("1", "YEAR"), 1L));
    rows.add(List.of(new BigDecimal("10000.0"), TemporalInterval.parse("0.123", "SECOND"), 2L));
    var inferred = new AggregateResultSetMetaData(List.of("amount", "elapsed", "count"),
        List.of(Types.DECIMAL, Types.OTHER, Types.BIGINT), "totals", "PUBLIC", "catalog", rows);
    rows.clear();

    var copied = new AggregateResultSetMetaData(new AggregateResultSetMetaData(inferred));
    assertNotSame(inferred, copied);
    assertEquals(3, copied.getColumnCount());
    assertEquals("amount", copied.getColumnLabel(1));
    assertEquals("totals", copied.getTableName(1));
    assertEquals("PUBLIC", copied.getSchemaName(1));
    assertEquals("catalog", copied.getCatalogName(1));
    assertEquals(Types.DECIMAL, copied.getColumnType(1));
    assertEquals(7, copied.getPrecision(1));
    assertEquals(2, copied.getScale(1));
    assertEquals("INTERVAL", copied.getColumnTypeName(2));
    assertEquals(TemporalInterval.class.getName(), copied.getColumnClassName(2));
    assertEquals(inferred.columnValueSchema(2), copied.columnValueSchema(2));
    assertEquals(3, IntervalType.precision(copied.columnValueSchema(2)));
    assertEquals(19, copied.getPrecision(3));
    assertEquals(0, copied.getScale(3));
  }

}
