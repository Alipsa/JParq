package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.Set;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.PredicateColumnRenamer;

/** Verifies rewriting of filter predicate column references. */
class PredicateColumnRenamerTest {

  /**
   * Keeps null predicates and unmapped column names untouched.
   */
  @Test
  void leavesUnmappedReferencesUnchanged() {
    FilterPredicate predicate = FilterApi.eq(FilterApi.intColumn("id"), 1);
    assertEquals("eq(id, 1)", PredicateColumnRenamer.rename(predicate, Map.of("a", "b")).toString());
    assertNull(PredicateColumnRenamer.rename(null, Map.of("a", "b")));
  }

  /**
   * Renames columns through nested logical operators.
   */
  @Test
  void renamesNestedPredicates() {
    FilterPredicate predicate = FilterApi.and(
        FilterApi.or(FilterApi.eq(FilterApi.intColumn("a"), 1), FilterApi.notEq(FilterApi.intColumn("a"), 2)),
        FilterApi.not(FilterApi.gt(FilterApi.intColumn("b"), 3)));
    FilterPredicate renamed = PredicateColumnRenamer.rename(predicate, Map.of("a", "id", "b", "name"));
    String text = renamed.toString();
    assertTrue(text.contains("eq(id, 1)"), text);
    assertTrue(text.contains("noteq(id, 2)"), text);
    assertTrue(text.contains("gt(name, 3)"), text);
  }

  /**
   * Renames the column of an IN predicate.
   */
  @Test
  void renamesInPredicates() {
    FilterPredicate in = new Operators.In<>(FilterApi.intColumn("a"), Set.of(1, 2));
    String text = PredicateColumnRenamer.rename(in, Map.of("a", "id")).toString();
    assertTrue(text.startsWith("in(id, "), text);
  }

  /**
   * Rejects user-defined predicates, which cannot be rewritten safely.
   */
  @Test
  void rejectsUserDefinedPredicates() {
    FilterPredicate userDefined = FilterApi.userDefined(FilterApi.binaryColumn("a"), DummyPredicate.class);
    assertThrows(IllegalArgumentException.class, () -> PredicateColumnRenamer.rename(userDefined, Map.of("a", "id")));
  }

  /** Test predicate used only to exercise the rejection path. */
  public static final class DummyPredicate
      extends
        org.apache.parquet.filter2.predicate.UserDefinedPredicate<org.apache.parquet.io.api.Binary> {

    @Override
    public boolean keep(org.apache.parquet.io.api.Binary value) {
      return true;
    }

    @Override
    public boolean canDrop(
        org.apache.parquet.filter2.predicate.Statistics<org.apache.parquet.io.api.Binary> statistics) {
      return false;
    }

    @Override
    public boolean inverseCanDrop(
        org.apache.parquet.filter2.predicate.Statistics<org.apache.parquet.io.api.Binary> statistics) {
      return false;
    }
  }
}
