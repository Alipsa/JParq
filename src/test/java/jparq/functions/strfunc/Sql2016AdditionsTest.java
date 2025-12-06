package jparq.functions.strfunc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.JParqSql;

/**
 * Tests for SQL:2016+ additions like NORMALIZE, STRING_AGG and JSON helpers.
 */
class Sql2016AdditionsTest {

  private static JParqSql sql;

  @BeforeAll
  static void setup() {
    sql = StringFunctionTestSupport.createSql();
  }

  @Test
  void testNormalize() {
    AtomicReference<String> normalized = new AtomicReference<>();

    sql.query("SELECT NORMALIZE('é', 'NFD') AS normalized FROM mtcars LIMIT 1", rs -> {
      try {
        assertTrue(rs.next(), "Expected a row");
        normalized.set(rs.getString("normalized"));
      } catch (SQLException e) {
        fail(e);
      }
    });

    String expected = Normalizer.normalize("é", Normalizer.Form.NFD);
    assertEquals(expected, normalized.get(), "NORMALIZE should use requested Unicode form");
  }

  @Test
  void testStringAgg() {
    List<String> models = new ArrayList<>();

    sql.query("SELECT model FROM mtcars WHERE cyl = 4", rs -> {
      try {
        while (rs.next()) {
          models.add(rs.getString("model"));
        }
      } catch (SQLException e) {
        fail(e);
      }
    });

    String expected = String.join(", ", models);
    AtomicReference<String> aggregated = new AtomicReference<>();

    sql.query("SELECT STRING_AGG(model, ', ') AS names FROM mtcars WHERE cyl = 4", rs -> {
      try {
        assertTrue(rs.next(), "Expected aggregated row");
        aggregated.set(rs.getString("names"));
      } catch (SQLException e) {
        fail(e);
      }
    });

    assertEquals(expected, aggregated.get(), "STRING_AGG should concatenate values with separator");
  }

  @Test
  void testJsonFunctions() {
    AtomicReference<String> firstModel = new AtomicReference<>();
    AtomicInteger firstCyl = new AtomicInteger();

    sql.query("SELECT model, cyl FROM mtcars LIMIT 1", rs -> {
      try {
        assertTrue(rs.next(), "Expected a row");
        firstModel.set(rs.getString("model"));
        firstCyl.set(rs.getInt("cyl"));
      } catch (SQLException e) {
        fail(e);
      }
    });

    AtomicReference<String> jsonName = new AtomicReference<>();
    AtomicInteger jsonSecond = new AtomicInteger();
    AtomicReference<String> jsonNums = new AtomicReference<>();
    AtomicReference<String> jsonObject = new AtomicReference<>();
    AtomicReference<String> jsonArray = new AtomicReference<>();

    sql.query("SELECT JSON_VALUE('{\"name\":\"Alice\",\"nums\":[1,2,3]}', '$.name') AS json_name, "
        + "JSON_VALUE('{\"name\":\"Alice\",\"nums\":[1,2,3]}', '$.nums[1]') AS json_second, "
        + "JSON_QUERY('{\"name\":\"Alice\",\"nums\":[1,2,3]}', '$.nums') AS json_nums, "
        + "JSON_OBJECT('model', model, 'cyl', cyl) AS json_object, JSON_ARRAY(1, 'two', 3) AS json_array "
        + "FROM mtcars LIMIT 1", rs -> {
          try {
            assertTrue(rs.next(), "Expected a row");
            jsonName.set(rs.getString("json_name"));
            jsonSecond.set(rs.getInt("json_second"));
            jsonNums.set(rs.getString("json_nums"));
            jsonObject.set(rs.getString("json_object"));
            jsonArray.set(rs.getString("json_array"));
          } catch (SQLException e) {
            fail(e);
          }
        });

    assertEquals("Alice", jsonName.get(), "JSON_VALUE should extract scalar values");
    assertEquals(2, jsonSecond.get(), "JSON_VALUE should support array subscripts");
    assertEquals("[1,2,3]", jsonNums.get(), "JSON_QUERY should return JSON text");
    String expectedObject = String.format("{\"model\":\"%s\",\"cyl\":%d}", firstModel.get(), firstCyl.get());
    assertEquals(expectedObject, jsonObject.get(), "JSON_OBJECT should build JSON structures");
    assertEquals("[1,\"two\",3]", jsonArray.get(), "JSON_ARRAY should build JSON arrays");
  }
}
