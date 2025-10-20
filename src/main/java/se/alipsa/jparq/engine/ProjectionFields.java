package se.alipsa.jparq.engine;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class ProjectionFields {
  private ProjectionFields() {
  }

  /**
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
