package jparq.aggregate;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.AggregateResultSetMetaData;

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
}