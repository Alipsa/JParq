package se.alipsa.jparq.engine;

import java.util.List;
import java.util.Optional;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;

/**
 * Plans independent WHERE predicates for join inputs without modifying the full
 * WHERE.
 */
public final class JoinPredicatePlanner {
  private JoinPredicatePlanner() {
  }

  /**
   * Plans safe single-table conjuncts for one join participant.
   * 
   * @param where
   *          the full WHERE expression
   * @param references
   *          the join participants
   * @param schemas
   *          participant schemas, with null for unresolved in-memory tables
   * @param index
   *          the participant to filter
   * @return an optional predicate; the full WHERE must still run after the join
   */
  public static Optional<FilterPredicate> plan(Expression where, List<SqlParser.TableReference> references,
      List<Schema> schemas, int index) {
    if (where == null || schemas.get(index) == null || references.get(index).tableSample() != null) {
      return Optional.empty();
    }
    Expression expression = ExpressionEvaluator.unwrapParenthesis(where);
    if (expression instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      return plan(list.getFirst(), references, schemas, index);
    }
    if (expression instanceof AndExpression and) {
      var left = plan(and.getLeftExpression(), references, schemas, index);
      var right = plan(and.getRightExpression(), references, schemas, index);
      return left.isEmpty() ? right : right.isEmpty() ? left : Optional.of(FilterApi.and(left.get(), right.get()));
    }
    Expression copy;
    try {
      copy = CCJSqlParserUtil.parseCondExpression(expression.toString());
    } catch (JSQLParserException exception) {
      return Optional.empty();
    }
    boolean[] safe = {
        true
    };
    boolean[] hasColumn = {
        false
    };
    copy.accept(new ExpressionVisitorAdapter<Void>() {
      @Override
      public <S> Void visit(Column column, S context) {
        hasColumn[0] = true;
        String qualifier = column.getTable() == null ? null : column.getTable().getName();
        int owner = -1;
        for (int i = 0; i < references.size(); i++) {
          var reference = references.get(i);
          if (qualifier != null && !qualifier.isBlank() && !same(qualifier, reference.tableAlias())
              && !same(qualifier, reference.tableName())) {
            continue;
          }
          Schema schema = schemas.get(i);
          if (schema == null) {
            // An unresolved participant might also own an unqualified name.
            safe[0] = false;
            continue;
          }
          boolean found = schema.getFields().stream().anyMatch(field -> same(field.name(), column.getColumnName()));
          if (found) {
            if (owner >= 0) {
              safe[0] = false;
            }
            owner = i;
          }
        }
        if (owner != index) {
          safe[0] = false;
        }
        if (qualifier == null || qualifier.isBlank()) {
          for (var reference : references) {
            if (reference.joinType() == SqlParser.JoinType.FULL_OUTER
                && reference.usingColumns().stream().anyMatch(name -> same(name, column.getColumnName()))) {
              safe[0] = false;
            }
          }
        }
        column.setTable(null);
        return null;
      }
    }, null);
    if (!safe[0] || !hasColumn[0]) {
      return Optional.empty();
    }
    Optional<FilterPredicate> predicate = ParquetFilterBuilder.split(schemas.get(index), copy).predicate();
    if (predicate.isEmpty()) {
      return predicate;
    }
    if (nullableSide(references, index)) {
      // Supported predicates are deterministic; evaluating an all-NULL input proves
      // null rejection.
      GenericData.Record nullRow = new GenericData.Record(schemas.get(index));
      if (new ExpressionEvaluator(schemas.get(index)).eval(copy, nullRow)) {
        return Optional.empty();
      }
    }
    return predicate;
  }

  /** Determines whether a join can synthesize NULLs for this participant. */
  private static boolean nullableSide(List<SqlParser.TableReference> references, int index) {
    var type = references.get(index).joinType();
    if (type == SqlParser.JoinType.LEFT_OUTER || type == SqlParser.JoinType.FULL_OUTER) {
      return true;
    }
    for (int i = index + 1; i < references.size(); i++) {
      type = references.get(i).joinType();
      if (type == SqlParser.JoinType.RIGHT_OUTER || type == SqlParser.JoinType.FULL_OUTER) {
        return true;
      }
    }
    return false;
  }

  /** Compares identifiers using the engine's identifier normalization. */
  private static boolean same(String left, String right) {
    return left != null && right != null && Identifier.lookupKey(left).equals(Identifier.lookupKey(right));
  }
}
