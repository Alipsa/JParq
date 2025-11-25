package se.alipsa.jparq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.junit.jupiter.api.Test;

public class ColumnNameLookupTest {

  @Test
  public void testCanonicalName() {
    List<String> canonicalColumnNames = List.of("col1", "col2", "col3");
    List<String> physicalColumnOrder = List.of("phys1", "phys2", "phys3");
    List<String> columnOrder = List.of("label1", "label2", "label3");

    assertEquals("col2", ColumnNameLookup.canonicalName(canonicalColumnNames, physicalColumnOrder, columnOrder, 2));
  }

  @Test
  public void testCanonicalNameWithNulls() {
    List<String> canonicalColumnNames = java.util.Arrays.asList("col1", null, "col3");
    List<String> physicalColumnOrder = List.of("phys1", "phys2", "phys3");
    List<String> columnOrder = List.of("label1", "label2", "label3");

    assertEquals("phys2", ColumnNameLookup.canonicalName(canonicalColumnNames, physicalColumnOrder, columnOrder, 2));
  }

  @Test
  public void testCanonicalNameWithEmptyLists() {
    List<String> canonicalColumnNames = List.of();
    List<String> physicalColumnOrder = List.of();
    List<String> columnOrder = List.of();

    assertNull(ColumnNameLookup.canonicalName(canonicalColumnNames, physicalColumnOrder, columnOrder, 1));
  }

  @Test
  public void testCanonicalNameWithIndexOutOfBounds() {
    List<String> canonicalColumnNames = List.of("col1");
    List<String> physicalColumnOrder = List.of("phys1");
    List<String> columnOrder = List.of("label1");

    assertNull(ColumnNameLookup.canonicalName(canonicalColumnNames, physicalColumnOrder, columnOrder, 2));
  }
}
