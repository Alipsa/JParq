package se.alipsa.jparq.engine;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Extracts projection fields from a SQL SELECT statement. */
public final class ProjectionFields {
  private ProjectionFields() {
  }

  /**
   * Extracts the set of selected column names from a SQL SELECT statement.
   *
   * @param sel
   *          the SELECT statement
   * @return null if SELECT * (unknown), else set of selected column names
   */
  public static Set<String> fromSelect(SqlParser.Select sel) {
    List<String> cols = sel.columns(); // never null; returns ["*"] for SELECT *
    if (cols.size() == 1 && "*".equals(cols.getFirst())) {
      // SELECT * -> reader should project all columns
      return null;
    }
    return new LinkedHashSet<>(cols);
  }
}
