package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
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
   * Renames the column of class-based, instance-based, and negated user-defined
   * predicates, such as the decimal predicates produced for BYTES/FIXED decimals.
   */
  @Test
  void renamesUserDefinedPredicates() {
    FilterPredicate byClass = FilterApi.userDefined(FilterApi.binaryColumn("a"), DummyPredicate.class);
    assertEquals(List.of("id"), columnsOf(PredicateColumnRenamer.rename(byClass, Map.of("a", "id"))));

    FilterPredicate instance = FilterApi.not(FilterApi.userDefined(FilterApi.binaryColumn("a"), new DummyPredicate()));
    assertEquals(List.of("id"), columnsOf(PredicateColumnRenamer.rename(instance, Map.of("a", "id"))));
  }

  private static List<String> columnsOf(FilterPredicate predicate) {
    List<String> columns = new ArrayList<>();
    predicate.accept(new ColumnCollector(columns));
    return columns;
  }

  /** Collects the dot-notation column paths of every visited predicate node. */
  private static final class ColumnCollector implements FilterPredicate.Visitor<Void> {

    private final List<String> columns;

    private ColumnCollector(List<String> columns) {
      this.columns = columns;
    }

    private void collect(Operators.Column<?> column) {
      columns.add(column.getColumnPath().toDotString());
    }

    @Override
    public <T extends Comparable<T>> Void visit(Operators.Eq<T> operator) {
      collect(operator.getColumn());
      return null;
    }

    @Override
    public <T extends Comparable<T>> Void visit(Operators.NotEq<T> operator) {
      collect(operator.getColumn());
      return null;
    }

    @Override
    public <T extends Comparable<T>> Void visit(Operators.Lt<T> operator) {
      collect(operator.getColumn());
      return null;
    }

    @Override
    public <T extends Comparable<T>> Void visit(Operators.LtEq<T> operator) {
      collect(operator.getColumn());
      return null;
    }

    @Override
    public <T extends Comparable<T>> Void visit(Operators.Gt<T> operator) {
      collect(operator.getColumn());
      return null;
    }

    @Override
    public <T extends Comparable<T>> Void visit(Operators.GtEq<T> operator) {
      collect(operator.getColumn());
      return null;
    }

    @Override
    public Void visit(Operators.And operator) {
      operator.getLeft().accept(this);
      operator.getRight().accept(this);
      return null;
    }

    @Override
    public Void visit(Operators.Or operator) {
      operator.getLeft().accept(this);
      operator.getRight().accept(this);
      return null;
    }

    @Override
    public Void visit(Operators.Not operator) {
      operator.getPredicate().accept(this);
      return null;
    }

    @Override
    public <T extends Comparable<T>, U extends org.apache.parquet.filter2.predicate.UserDefinedPredicate<T>> Void visit(
        Operators.UserDefined<T, U> operator) {
      collect(operator.getColumn());
      return null;
    }

    @Override
    public <T extends Comparable<T>, U extends org.apache.parquet.filter2.predicate.UserDefinedPredicate<T>> Void visit(
        Operators.LogicalNotUserDefined<T, U> operator) {
      return visit(operator.getUserDefined());
    }
  }

  /** Test predicate used only to exercise the user-defined renaming paths. */
  public static final class DummyPredicate extends org.apache.parquet.filter2.predicate.UserDefinedPredicate<Binary>
      implements
        Serializable {

    @Override
    public boolean keep(Binary value) {
      return true;
    }

    @Override
    public boolean canDrop(org.apache.parquet.filter2.predicate.Statistics<Binary> statistics) {
      return false;
    }

    @Override
    public boolean inverseCanDrop(org.apache.parquet.filter2.predicate.Statistics<Binary> statistics) {
      return false;
    }
  }
}
