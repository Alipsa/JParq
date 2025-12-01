package jparq.meta;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import jparq.usage.AcmeTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

public class JParqDatabaseMetaDataTest {

  private static JParqSql jparqSql;

  static String jdbcUrl;
  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = AcmeTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jdbcUrl = "jdbc:jparq:" + acmePath.toAbsolutePath();
  }

  @Test
  void databaseMetaDataTest() throws SQLException {
    try (Connection con = DriverManager.getConnection(jdbcUrl)) {
      var metaData = con.getMetaData();
      Assertions.assertEquals("JParq", metaData.getDatabaseProductName());
      Assertions.assertEquals("se.alipsa.jparq.JParqDriver", metaData.getDriverName());
      Assertions.assertEquals(
          "{fn ABS}, {fn ACOS}, {fn ASIN}, {fn ATAN}, {fn ATAN2}, {fn CEILING}, {fn COS}, {fn COT}, {fn DEGREES}, "
              + "{fn EXP}, {fn FLOOR}, {fn LOG}, {fn LOG10}, {fn MOD}, {fn PI}, {fn POWER}, {fn RADIANS}, {fn RAND}, "
              + "{fn ROUND}, {fn SIGN}, {fn SIN}, {fn SQRT}, {fn TAN}, {fn TRUNCATE}",
          metaData.getNumericFunctions());

      try (ResultSet tables = metaData.getTables(null, null, null, new String[]{
          "TABLE"
      })) {
        ResultSetMetaData tableMeta = tables.getMetaData();
        List<String> expectedTableColumns = List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
            "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "SELF_REFERENCING_COL_NAME", "REF_GENERATION");
        Assertions.assertEquals(expectedTableColumns.size(), tableMeta.getColumnCount());
        for (int i = 0; i < tableMeta.getColumnCount(); i++) {
          Assertions.assertEquals(expectedTableColumns.get(i), tableMeta.getColumnName(i + 1));
        }
        Set<String> tableNames = new LinkedHashSet<>();
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          tableNames.add(tableName);
          Assertions.assertEquals("TABLE", tables.getString("TABLE_TYPE"));
          Assertions.assertNull(tables.getObject("REMARKS"));
          Assertions.assertNull(tables.getObject("TYPE_NAME"));
        }
        Assertions.assertEquals(Set.of("departments", "employees", "employee_department", "salary"), tableNames);
      }

      try (ResultSet columns = metaData.getColumns(null, null, "employees", null)) {
        ResultSetMetaData columnMeta = columns.getMetaData();
        List<String> expectedColumnHeaders = List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE",
            "COLUMN_NAME", "ORDINAL_POSITION", "IS_NULLABLE", "DATA_TYPE", "TYPE_NAME", "CHARACTER_MAXIMUM_LENGTH",
            "NUMERIC_PRECISION", "NUMERIC_SCALE", "COLLATION_NAME", "COLUMN_DEF", "DATETIME_PRECISION", "REMARKS");
        Assertions.assertEquals(expectedColumnHeaders.size(), columnMeta.getColumnCount());
        for (int i = 0; i < columnMeta.getColumnCount(); i++) {
          Assertions.assertEquals(expectedColumnHeaders.get(i), columnMeta.getColumnName(i + 1));
        }

        Map<String, ColumnInfo> columnsByName = new LinkedHashMap<>();
        while (columns.next()) {
          String name = columns.getString("COLUMN_NAME");
          int ordinal = columns.getInt("ORDINAL_POSITION");
          String nullable = columns.getString("IS_NULLABLE");
          int dataType = columns.getInt("DATA_TYPE");
          String typeName = columns.getString("TYPE_NAME");
          Integer charMax = integerOrNull(columns, "CHARACTER_MAXIMUM_LENGTH");
          Integer precision = integerOrNull(columns, "NUMERIC_PRECISION");
          Integer scale = integerOrNull(columns, "NUMERIC_SCALE");
          columnsByName.put(name, new ColumnInfo(columns.getString("TABLE_TYPE"), ordinal, nullable, dataType, typeName,
              charMax, precision, scale));
        }

        Assertions.assertEquals(Set.of("id", "first_name", "last_name"), columnsByName.keySet());

        ColumnInfo idColumn = columnsByName.get("id");
        Assertions.assertNotNull(idColumn);
        Assertions.assertEquals("TABLE", idColumn.tableType());
        Assertions.assertEquals(1, idColumn.ordinalPosition());
        Assertions.assertEquals("YES", idColumn.isNullable());
        Assertions.assertEquals(Types.INTEGER, idColumn.dataType());
        Assertions.assertEquals("INTEGER", idColumn.typeName());
        Assertions.assertEquals(Integer.valueOf(10), idColumn.numericPrecision());
        Assertions.assertEquals(Integer.valueOf(0), idColumn.numericScale());

        ColumnInfo firstName = columnsByName.get("first_name");
        Assertions.assertNotNull(firstName);
        Assertions.assertEquals(Types.VARCHAR, firstName.dataType());
        Assertions.assertEquals("VARCHAR", firstName.typeName());
        Assertions.assertEquals(2, firstName.ordinalPosition());
        Assertions.assertEquals("YES", firstName.isNullable());
        Assertions.assertNull(firstName.characterMaximumLength());
        Assertions.assertNull(firstName.numericPrecision());
        Assertions.assertNull(firstName.numericScale());

        ColumnInfo lastName = columnsByName.get("last_name");
        Assertions.assertNotNull(lastName);
        Assertions.assertEquals(Types.VARCHAR, lastName.dataType());
        Assertions.assertEquals("VARCHAR", lastName.typeName());
        Assertions.assertEquals(3, lastName.ordinalPosition());
        Assertions.assertEquals("YES", lastName.isNullable());
        Assertions.assertNull(lastName.characterMaximumLength());
        Assertions.assertNull(lastName.numericPrecision());
        Assertions.assertNull(lastName.numericScale());
      }
    }
  }

  private Integer integerOrNull(ResultSet rs, String columnLabel) throws SQLException {
    int value = rs.getInt(columnLabel);
    return rs.wasNull() ? null : Integer.valueOf(value);
  }

  private record ColumnInfo(String tableType, int ordinalPosition, String isNullable, int dataType, String typeName,
      Integer characterMaximumLength, Integer numericPrecision, Integer numericScale) {
  }
}
