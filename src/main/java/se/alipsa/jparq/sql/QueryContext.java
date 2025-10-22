package se.alipsa.jparq.sql;

import java.util.*;
import net.sf.jsqlparser.expression.Expression;

public final class QueryContext {
  public final Map<String, String> tableAliasToTable = new HashMap<>();
  public final Map<String, Expression> selectAliasToExpr = new LinkedHashMap<>();
  public final List<String> projectionLabels = new ArrayList<>();
  public final List<String> projectionColumnNames = new ArrayList<>(); // null for computed exprs

  public Optional<String> resolveTable(String maybeAliasOrName) {
    if (maybeAliasOrName == null) {
      return Optional.empty();
    }
    String real = tableAliasToTable.getOrDefault(maybeAliasOrName, maybeAliasOrName);
    return Optional.of(real);
  }

  public void addProjection(String label, String underlyingColumnName, Expression expr, String alias) {
    projectionLabels.add(label);
    projectionColumnNames.add(underlyingColumnName);
    if (alias != null) {
      selectAliasToExpr.put(alias, expr);
    }
  }
}
