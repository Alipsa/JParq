package se.alipsa.jparq.engine;

import java.util.Map;
import java.util.Objects;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/**
 * Rewrites the column references of a {@link FilterPredicate} so a predicate
 * planned against one schema (for example positional column aliases) can be
 * applied to a physically different but positionally equivalent schema.
 */
public final class PredicateColumnRenamer {

  private PredicateColumnRenamer() {
  }

  /**
   * Renames every column reference found in the predicate.
   *
   * @param predicate
   *          predicate to rewrite, or null
   * @param renames
   *          exact column names to replace, mapped to their replacements
   * @return the rewritten predicate; columns without a mapping keep their name,
   *         and null inputs pass through
   */
  public static FilterPredicate rename(FilterPredicate predicate, Map<String, String> renames) {
    if (predicate == null || renames.isEmpty()) {
      return predicate;
    }
    return predicate.accept(new RenamingVisitor(renames));
  }

  /**
   * Column replacement carrying the comparison capability markers required by the
   * {@link FilterApi} factories.
   *
   * @param <T>
   *          column value type
   */
  private abstract static class RenamedColumn<T extends Comparable<T>> extends Operators.Column<T>
      implements
        Operators.SupportsLtGt {

    RenamedColumn(ColumnPath path, Class<T> type) {
      super(path, type);
    }
  }

  private static final class RenamingVisitor implements FilterPredicate.Visitor<FilterPredicate> {

    private final Map<String, String> renames;

    private RenamingVisitor(Map<String, String> renames) {
      this.renames = Objects.requireNonNull(renames, "renames");
    }

    @Override
    public <T extends Comparable<T>> FilterPredicate visit(Operators.Eq<T> operator) {
      return FilterApi.eq(renameColumn(operator.getColumn()), operator.getValue());
    }

    @Override
    public <T extends Comparable<T>> FilterPredicate visit(Operators.NotEq<T> operator) {
      return FilterApi.notEq(renameColumn(operator.getColumn()), operator.getValue());
    }

    @Override
    public <T extends Comparable<T>> FilterPredicate visit(Operators.Lt<T> operator) {
      return FilterApi.lt(renameColumn(operator.getColumn()), operator.getValue());
    }

    @Override
    public <T extends Comparable<T>> FilterPredicate visit(Operators.LtEq<T> operator) {
      return FilterApi.ltEq(renameColumn(operator.getColumn()), operator.getValue());
    }

    @Override
    public <T extends Comparable<T>> FilterPredicate visit(Operators.Gt<T> operator) {
      return FilterApi.gt(renameColumn(operator.getColumn()), operator.getValue());
    }

    @Override
    public <T extends Comparable<T>> FilterPredicate visit(Operators.GtEq<T> operator) {
      return FilterApi.gtEq(renameColumn(operator.getColumn()), operator.getValue());
    }

    @Override
    public <T extends Comparable<T>> FilterPredicate visit(Operators.In<T> operator) {
      return FilterApi.in(renameColumn(operator.getColumn()), operator.getValues());
    }

    @Override
    public <T extends Comparable<T>> FilterPredicate visit(Operators.NotIn<T> operator) {
      return FilterApi.notIn(renameColumn(operator.getColumn()), operator.getValues());
    }

    @Override
    public FilterPredicate visit(Operators.And operator) {
      return FilterApi.and(rename(operator.getLeft()), rename(operator.getRight()));
    }

    @Override
    public FilterPredicate visit(Operators.Or operator) {
      return FilterApi.or(rename(operator.getLeft()), rename(operator.getRight()));
    }

    @Override
    public FilterPredicate visit(Operators.Not operator) {
      return FilterApi.not(rename(operator.getPredicate()));
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> FilterPredicate visit(
        Operators.UserDefined<T, U> operator) {
      throw new IllegalArgumentException("User-defined filter predicates cannot be column-renamed");
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> FilterPredicate visit(
        Operators.LogicalNotUserDefined<T, U> operator) {
      throw new IllegalArgumentException("User-defined filter predicates cannot be column-renamed");
    }

    private FilterPredicate rename(FilterPredicate predicate) {
      return PredicateColumnRenamer.rename(predicate, renames);
    }

    private <T extends Comparable<T>> RenamedColumn<T> renameColumn(Operators.Column<T> column) {
      String replacement = renames.get(column.getColumnPath().toDotString());
      if (replacement == null) {
        replacement = column.getColumnPath().toDotString();
      }
      String name = replacement;
      return new RenamedColumn<>(ColumnPath.get(name), column.getColumnType()) {
      };
    }
  }
}
