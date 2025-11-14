package se.alipsa.jparq;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JParqDatabaseMetaDataHintParsingTest {

  @Test
  void preservesEmptyEntriesInColumnTypeHints() {
    List<String> hints = JParqDatabaseMetaData.parseColumnTypeHints("java.lang.String,,java.lang.Integer");
    Assertions.assertEquals(3, hints.size());
    Assertions.assertEquals("java.lang.String", hints.get(0));
    Assertions.assertEquals("", hints.get(1));
    Assertions.assertEquals("java.lang.Integer", hints.get(2));
  }

  @Test
  void trimsWhitespaceAroundHints() {
    List<String> hints = JParqDatabaseMetaData.parseColumnTypeHints("  java.lang.Long  ,  java.lang.Double ");
    Assertions.assertEquals(List.of("java.lang.Long", "java.lang.Double"), hints);
  }

  @Test
  void handlesNullOrBlankInputs() {
    Assertions.assertTrue(JParqDatabaseMetaData.parseColumnTypeHints(null).isEmpty());
    Assertions.assertTrue(JParqDatabaseMetaData.parseColumnTypeHints("   ").isEmpty());
  }
}
