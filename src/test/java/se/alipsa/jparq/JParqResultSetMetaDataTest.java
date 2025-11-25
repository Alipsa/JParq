package se.alipsa.jparq;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Types;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

public class JParqResultSetMetaDataTest {

  @Test
  public void testMetaData() {
    Schema schema = SchemaBuilder.record("test").fields().name("name").type().stringType().noDefault().name("age")
        .type().intType().noDefault().endRecord();

    JParqResultSetMetaData metaData = new JParqResultSetMetaData(schema, List.of("name", "age"), List.of("name", "age"),
        List.of("name", "age"), "test_table", "test_schema", "test_catalog", List.of());

    assertEquals(2, metaData.getColumnCount());
    assertEquals("name", metaData.getColumnLabel(1));
    assertEquals("age", metaData.getColumnLabel(2));
    assertEquals("name", metaData.getColumnName(1));
    assertEquals("age", metaData.getColumnName(2));
    assertEquals("test_table", metaData.getTableName(1));
    assertEquals("test_schema", metaData.getSchemaName(1));
    assertEquals("test_catalog", metaData.getCatalogName(1));
    assertEquals(Types.VARCHAR, metaData.getColumnType(1));
    assertEquals(Types.INTEGER, metaData.getColumnType(2));
    assertEquals("VARCHAR", metaData.getColumnTypeName(1));
    assertEquals("INTEGER", metaData.getColumnTypeName(2));
    assertEquals(String.class.getName(), metaData.getColumnClassName(1));
    assertEquals(Integer.class.getName(), metaData.getColumnClassName(2));
    assertEquals(java.sql.ResultSetMetaData.columnNullableUnknown, metaData.isNullable(1));
  }
}
