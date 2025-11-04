package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.ColumnMappingUtil;

/** Tests for {@link ColumnMappingUtil}. */
class ColumnMappingUtilTest {

  @Test
  void canonicalOrderColumnResolvesQualifiedMappings() {
    Map<String, Map<String, String>> qualifierMapping = ColumnMappingUtil
        .normaliseQualifierMapping(Map.of("Cars", Map.of("MPG", "cars__mpg")));
    Map<String, String> unqualifiedMapping = ColumnMappingUtil.normaliseUnqualifiedMapping(Map.of("MPG", "cars__mpg"));

    String resolved = ColumnMappingUtil.canonicalOrderColumn("MpG", "CARS", qualifierMapping, unqualifiedMapping);

    assertEquals("cars__mpg", resolved, "Qualified lookup should be case-insensitive");
  }

  @Test
  void canonicalOrderColumnFallsBackToUnqualifiedMappings() {
    Map<String, Map<String, String>> qualifierMapping = ColumnMappingUtil
        .normaliseQualifierMapping(Map.of("Cars", Map.of("WEIGHT", "cars__weight")));
    Map<String, String> unqualifiedMapping = ColumnMappingUtil.normaliseUnqualifiedMapping(Map.of("Hp", "cars__hp"));

    String resolved = ColumnMappingUtil.canonicalOrderColumn("HP", null, qualifierMapping, unqualifiedMapping);

    assertEquals("cars__hp", resolved, "Unqualified lookup should use normalized mapping");
  }

  @Test
  void canonicalOrderColumnUsesUnqualifiedMappingWhenQualifierMapEmpty() {
    Map<String, Map<String, String>> qualifierMapping = ColumnMappingUtil.normaliseQualifierMapping(Map.of());
    Map<String, String> unqualifiedMapping = ColumnMappingUtil
        .normaliseUnqualifiedMapping(Map.of("WEIGHT", "cars__weight"));

    String resolved = ColumnMappingUtil.canonicalOrderColumn("weight", null, qualifierMapping, unqualifiedMapping);

    assertEquals("cars__weight", resolved, "Should honor unqualified mappings without qualifier definitions");
  }

  @Test
  void canonicalOrderColumnReturnsOriginalWhenUnknown() {
    Map<String, Map<String, String>> qualifierMapping = ColumnMappingUtil.normaliseQualifierMapping(Map.of());
    Map<String, String> unqualifiedMapping = ColumnMappingUtil.normaliseUnqualifiedMapping(Map.of());

    String resolved = ColumnMappingUtil.canonicalOrderColumn("unknown", null, qualifierMapping, unqualifiedMapping);

    assertEquals("unknown", resolved, "Unknown columns should remain unchanged");
  }

  @Test
  void canonicalOrderColumnHandlesNullColumn() {
    Map<String, Map<String, String>> qualifierMapping = ColumnMappingUtil.normaliseQualifierMapping(Map.of());
    Map<String, String> unqualifiedMapping = ColumnMappingUtil.normaliseUnqualifiedMapping(Map.of());

    assertNull(ColumnMappingUtil.canonicalOrderColumn(null, "cars", qualifierMapping, unqualifiedMapping),
        "Null columns should yield null");
  }
}
