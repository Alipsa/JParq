package se.alipsa.jparq.engine.function;

import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import se.alipsa.jparq.engine.ColumnMappingUtil;
import se.alipsa.jparq.engine.CorrelatedSubqueryRewriter.QualifiedColumn;
import se.alipsa.jparq.engine.CorrelationMappings;
import se.alipsa.jparq.engine.Identifier;
import se.alipsa.jparq.engine.SqlExceptions;
import se.alipsa.jparq.engine.SubqueryExecutor;

/** Binds scalar subquery correlations exclusively to grouped column values. */
final class GroupedScalarSubquery {
  private final Select select;
  private final SubqueryExecutor executor;
  private final CorrelationMappings mappings;
  private final Set<String> candidates;
  private final Map<QualifiedColumn, Integer> groupIndexes = new HashMap<>();

  /**
   * Validates outer references before any groups are read, including empty
   * inputs.
   *
   * @param select
   *          scalar subquery
   * @param groups
   *          outer grouping expressions
   * @param executor
   *          schema-aware subquery executor
   * @param mappings
   *          outer source column mappings
   */
  GroupedScalarSubquery(Select select, List<AggregateFunctions.GroupExpression> groups, SubqueryExecutor executor,
      CorrelationMappings mappings) {
    this.select = select;
    this.executor = executor;
    this.mappings = mappings;
    this.candidates = columnNames(select);
    Map<String, Integer> groupedColumns = new HashMap<>();
    for (int index = 0; index < groups.size(); index++) {
      if (AggregateFunctions.unwrap(groups.get(index).expression()) instanceof Column column) {
        String qualifier = column.getTable() == null ? null : column.getTable().getName();
        groupedColumns.put(canonical(qualifier, column.getColumnName()), index);
      }
    }
    executor.rewrite(select, mappings.outerQualifiers(), candidates, mappings.qualifierColumnMapping(),
        (qualifier, column) -> {
          Integer index = groupedColumns.get(canonical(qualifier, column));
          if (index == null) {
            throw SqlExceptions.syntaxFailure("Scalar subquery references ungrouped outer column '" + column
                + "'; outer columns must appear in the GROUP BY clause");
          }
          groupIndexes.put(new QualifiedColumn(qualifier, column), index);
          return null;
        });
  }

  /**
   * Resolves an SQL column to its canonical source identity, retaining join
   * qualifiers.
   *
   * @param qualifier
   *          optional table qualifier
   * @param column
   *          SQL column name
   * @return normalized canonical field identity
   */
  private String canonical(String qualifier, String column) {
    if (mappings.qualifierColumnMapping().isEmpty()) {
      return Identifier
          .lookupKey(mappings.unqualifiedColumnMapping().getOrDefault(Identifier.lookupKey(column), column));
    }
    return Identifier.lookupKey(ColumnMappingUtil.canonicalFieldName(qualifier, column,
        mappings.qualifierColumnMapping(), mappings.unqualifiedColumnMapping(), Map.of()));
  }

  /**
   * Evaluates a scalar against current values, including NULLs suppressed by
   * grouping sets.
   *
   * @param groupValues
   *          current group's values
   * @return scalar value, or null for an empty scalar result
   */
  Object evaluate(List<Object> groupValues) {
    var result = executor.executeCorrelated(select, mappings.outerQualifiers(), candidates,
        mappings.qualifierColumnMapping(),
        (qualifier, column) -> groupValues.get(groupIndexes.get(new QualifiedColumn(qualifier, column))));
    return result.scalarValue();
  }

  /**
   * Infers the fixed COUNT type without executing or materializing skipped
   * queries. Prepared-statement result metadata is unavailable before execution,
   * so other scalar types continue to depend on values from surviving groups.
   *
   * @return BIGINT for a COUNT projection, otherwise OTHER
   */
  int declaredSqlType() {
    Select query = select;
    while (query instanceof ParenthesedSelect parenthesed) {
      query = parenthesed.getSelect();
    }
    if (query instanceof PlainSelect plain && plain.getSelectItems().size() == 1
        && AggregateFunctions.unwrap(plain.getSelectItems().getFirst().getExpression()) instanceof Function function
        && "COUNT".equalsIgnoreCase(function.getName())) {
      return Types.BIGINT;
    }
    return Types.OTHER;
  }

  /**
   * Collects candidate names; the executor then excludes names bound in any inner
   * scope.
   *
   * @param select
   *          subquery, including nested query scopes
   * @return potential unqualified outer references
   */
  private static Set<String> columnNames(Select select) {
    Set<String> names = new LinkedHashSet<>();
    StringBuilder buffer = new StringBuilder();
    ExpressionDeParser expressions = new ExpressionDeParser() {
      @Override
      public <S> StringBuilder visit(Column column, S context) {
        names.add(Identifier.lookupKey(column.getColumnName()));
        return super.visit(column, context);
      }
    };
    SelectDeParser queries = new SelectDeParser(expressions, buffer);
    expressions.setSelectVisitor(queries);
    expressions.setBuilder(buffer);
    select.accept((SelectVisitor<StringBuilder>) queries, null);
    return Set.copyOf(names);
  }
}
