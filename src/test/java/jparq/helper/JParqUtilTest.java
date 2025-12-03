package jparq.helper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.helper.JParqUtil;

/** Tests for {@link JParqUtil}. */
class JParqUtilTest {

  @Test
  void parsesQueryStrings() {
    Properties props = JParqUtil.parseUrlQuery("?a=1&b=hello%20world&empty=&ignored");
    assertEquals("1", props.getProperty("a"));
    assertEquals("hello world", props.getProperty("b"));
    assertEquals("", props.getProperty("empty"));
    assertTrue(JParqUtil.parseUrlQuery(null).isEmpty());
    assertTrue(JParqUtil.parseUrlQuery("").isEmpty());
  }

  @Test
  void convertsSqlLikeToRegex() {
    assertEquals("a.*\\.c", JParqUtil.sqlLikeToRegex("a%.c"));
    assertEquals("a.b.*", JParqUtil.sqlLikeToRegex("a_b%"));
  }

  @Test
  void normalizesQualifiers() {
    assertEquals("Q:table", JParqUtil.normalizeQualifier("\"table\""));
    assertEquals("Q:column", JParqUtil.normalizeQualifier("`column`"));
    assertEquals("Q:schema", JParqUtil.normalizeQualifier("[schema]"));
    assertNull(JParqUtil.normalizeQualifier("   "));
    assertNull(JParqUtil.normalizeQualifier(null));
  }

  @Test
  void convertsArrayLikeObjectsToIterable() {
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    GenericData.Array<String> avroArray = new GenericData.Array<>(2, arraySchema);
    avroArray.add("a");
    avroArray.add("b");

    Iterable<?> iterable = JParqUtil.toIterable(avroArray);
    assertNotNull(iterable);
    List<Object> values = new ArrayList<>();
    iterable.forEach(values::add);
    assertEquals(List.of("a", "b"), values);

    Iterable<?> fromJavaArray = JParqUtil.toIterable(new Object[]{
        "x", "y"
    });
    List<Object> arrayValues = new ArrayList<>();
    fromJavaArray.forEach(arrayValues::add);
    assertEquals(List.of("x", "y"), arrayValues);

    assertNull(JParqUtil.toIterable("not-array"));
  }
}
