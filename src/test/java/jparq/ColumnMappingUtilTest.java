package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.ColumnMappingUtil;

/** Tests for {@link ColumnMappingUtil}. */
class ColumnMappingUtilTest {

  /** Keeps prefix-like text intact when mappings are normalized repeatedly. */
  @Test
  void normalizedMappingsRetainPrefixLikeIdentifierText() {
    var mapping = Map.of("Q:u:x", Map.of("Q:q:y", "field"), "Q:Q:x", Map.of("U:z", "other"));
    assertEquals(mapping, ColumnMappingUtil.normaliseQualifierMapping(mapping));
    assertEquals(mapping,
        ColumnMappingUtil.normaliseQualifierMapping(ColumnMappingUtil.normaliseQualifierMapping(mapping)));
    assertEquals("field", ColumnMappingUtil.canonicalOrderColumn("\"q:y\"", "\"u:x\"", mapping, Map.of()));
  }

  /**
   * Ensures a real mapping wins over a fallback even when its text is unchanged.
   */
  @Test
  void unchangedCanonicalNameWinsOverProjectionFallback() {
    assertEquals("x", ColumnMappingUtil.canonicalOrderColumn("x", "m", Map.of("U:m", Map.of("U:x", "x")), Map.of(),
        "projection_alias"));
    assertEquals("x",
        ColumnMappingUtil.canonicalOrderColumn("x", null, Map.of(), Map.of("U:x", "x"), "projection_alias"));
    assertEquals("projection_alias",
        ColumnMappingUtil.canonicalOrderColumn("missing", null, Map.of(), Map.of(), "projection_alias"));
  }

  /** Verifies the shared fallback for lowercase quoted identifier keys. */
  @Test
  void quotedLowercaseKeysHaveUnquotedAlternatives() {
    assertEquals("U:m", ColumnMappingUtil.alternativeColumnKey("Q:m"));
    assertNull(ColumnMappingUtil.alternativeColumnKey("Q:M"));
    assertNull(ColumnMappingUtil.alternativeColumnKey("U:m"));
    assertNull(ColumnMappingUtil.alternativeColumnKey(null));
    var mappings = Map.of("U:m", Map.of("U:x", "mx"), "Q:m", Map.of("U:x", "quoted_mx"));
    assertEquals("quoted_mx", ColumnMappingUtil.qualifierColumns(mappings, "\"m\"").get("U:x"));
    assertEquals("mx", ColumnMappingUtil.qualifierColumns(Map.of("U:m", Map.of("U:x", "mx")), "\"m\"").get("U:x"));
    assertEquals(Map.of(), ColumnMappingUtil.qualifierColumns(mappings, "\"M\""));
  }

  @Test
  void correlationCandidatesIncludePhysicalNamesAndProjectionAliases() {
    assertEquals(java.util.Set.of("U:id", "U:g", "U:grp"), ColumnMappingUtil.correlationColumns(
        Map.of("U:id", "id", "U:g", "g"), Map.of("U:id", "id"), Map.of("U:b", Map.of("U:g", "g", "U:grp", "g"))));
  }

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
