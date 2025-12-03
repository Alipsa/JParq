package jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.ColumnNameLookup;

/**
 * Tests for {@link ColumnNameLookup}.
 */
class ColumnNameLookupTest {

  @Test
  void canonicalNamePrefersCanonicalThenPhysicalThenLabel() {
    List<String> canonical = Arrays.asList("id", null, null);
    List<String> physical = Arrays.asList("id_col", "first_name", null);
    List<String> labels = Arrays.asList("ID", "FirstName", "SalaryAlias");

    assertEquals("id", ColumnNameLookup.canonicalName(canonical, physical, labels, 1));
    assertEquals("first_name", ColumnNameLookup.canonicalName(canonical, physical, labels, 2));
    assertEquals("SalaryAlias", ColumnNameLookup.canonicalName(canonical, physical, labels, 3));
    assertNull(ColumnNameLookup.canonicalName(canonical, physical, labels, 4));
  }

  @Test
  void buildCaseInsensitiveIndexIncludesCanonicalPhysicalAndLabels() {
    List<String> canonical = Arrays.asList("id", null, null);
    List<String> physical = Arrays.asList("id_col", "first_name", null);
    List<String> labels = Arrays.asList("ID", "FirstName", "SalaryAlias");

    Map<String, Integer> index = ColumnNameLookup.buildCaseInsensitiveIndex(labels.size(), canonical, physical, labels);

    assertEquals(1, index.get("U:id").intValue());
    assertEquals(2, index.get("U:first_name").intValue());
    assertEquals(3, index.get("U:salaryalias").intValue());
  }
}
