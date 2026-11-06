package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.TranscodingFunction;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.ParenthesedFromItem;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.Values;
import net.sf.jsqlparser.statement.select.WithItem;
import org.apache.avro.Schema;
import se.alipsa.jparq.helper.JdbcTypeMapper;

/**
 * Validates ARRAY declarations and binary CAST operands throughout queries
 * without evaluating rows.
 */
public final class ArrayQueryValidation {
  /** Prevents utility class construction. */
  private ArrayQueryValidation() {
  }

  /**
   * Checks every clause and nested scope, including scopes execution can skip.
   *
   * @param query
   *          parsed SQL query
   * @param tables
   *          source schema resolver, returning null for unknown sources
   * @throws IllegalArgumentException
   *           carrying a JDBC cause for invalid declarations
   */
  public static void validate(Select query, Function<Table, Schema> tables) {
    visitSelect(query, new Scope(column -> null, tables, Map.of(), new HashSet<>(), false));
  }

  /**
   * Checks an expression using bound local columns and independently scoped
   * nested queries.
   *
   * @param expression
   *          SQL expression, possibly null
   * @param columns
   *          bound column schema resolver
   * @param tables
   *          nested input schema resolver
   * @throws IllegalArgumentException
   *           carrying a JDBC cause for invalid declarations
   */
  public static void validate(Expression expression, Function<Column, Schema> columns, Function<Table, Schema> tables) {
    visitExpression(expression, new Scope(columns, tables, Map.of(), new HashSet<>(), true));
  }

  /**
   * Resolves a scalar projection without inheriting another query's inputs.
   *
   * @param query
   *          scalar subquery
   * @return declared projection schema, or null when unknown
   */
  public static Schema projectionSchema(Select query) {
    return projectionSchema(query, table -> null);
  }

  /**
   * Resolves a scalar projection using the engine's source schema declarations.
   *
   * @param query
   *          scalar query
   * @param tables
   *          input schema resolver
   * @return known projection schema, or null when uncertain
   */
  public static Schema projectionSchema(Select query, Function<Table, Schema> tables) {
    return projectedValue(query, new Scope(column -> null, tables, Map.of(), new HashSet<>(), false));
  }

  /**
   * Resolves an expression and its scalar subqueries in their lexical scopes.
   *
   * @param expression
   *          expression to resolve
   * @param scope
   *          local scope
   * @return known schema, or null
   */
  private static Schema resolve(Expression expression, Scope scope) {
    return ArrayExpressionSchemas.resolve(expression, scope.columns(), nested -> projectedValue(nested, scope));
  }

  /**
   * Resolves a single projected value with a recursion guard.
   *
   * @param query
   *          scalar query
   * @param outer
   *          enclosing scope
   * @return projected schema, or null when unknown
   */
  private static Schema projectedValue(Select query, Scope outer) {
    if (!outer.resolving().add(query)) {
      return null;
    }
    try {
      Scope inherited = withDeclarations(query, outer);
      if (query instanceof ParenthesedSelect nested) {
        return projectedValue(nested.getSelect(), inherited);
      }
      if (query instanceof PlainSelect plain && plain.getSelectItems().size() == 1) {
        return resolve(plain.getSelectItems().getFirst().getExpression(), localScope(plain, inherited));
      }
      return null;
    } finally {
      outer.resolving().remove(query);
    }
  }

  /**
   * Visits expressions while replacing the column scope at each nested SELECT.
   *
   * @param expression
   *          expression to inspect, possibly null
   * @param scope
   *          current lexical scope
   */
  private static void visitExpression(Expression expression, Scope scope) {
    if (expression == null) {
      return;
    }
    expression.accept(new JSqlParserSupport.NamedArgumentVisitor() {
      @Override
      public <S> Void visit(ArrayConstructor array, S context) {
        resolve(array, scope);
        return super.visit(array, context);
      }

      @Override
      public <S> Void visit(net.sf.jsqlparser.expression.Function function, S context) {
        if ("ARRAY".equalsIgnoreCase(function.getName())) {
          resolve(function, scope);
        }
        if ("NULLIF".equalsIgnoreCase(function.getName()) && function.getParameters() != null
            && function.getParameters().size() == 2) {
          ScalarExpressionSchemas.validateBinaryComparison(function.getParameters().get(0),
              function.getParameters().get(1), operand -> scalarOperand(operand, scope));
        }
        ScalarExpressionSchemas.validateCharacterFunction(function.getName(), function.getParameters(),
            operand -> scalarOperand(operand, scope));
        return super.visit(function, context);
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(CastExpression cast, S context) {
        ScalarExpressionSchemas.validateBinaryCast(cast, operand -> scalarOperand(operand, scope));
        return super.visit(cast, context);
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(TranscodingFunction convert, S context) {
        if (!convert.isTranscodeStyle()) {
          CastExpression cast = scope.bound()
              ? CastExpressions.requireCast(convert, name -> scope.columns().apply(new Column(name)) != null)
              : CastExpressions.asCast(convert, name -> scope.columns().apply(new Column(name)) != null);
          if (cast != null) {
            DecimalCasts.validateBounds(cast);
            ScalarExpressionSchemas.validateBinaryCast(cast, operand -> scalarOperand(operand, scope));
          }
        }
        return super.visit(convert, context);
      }

      /** {@inheritDoc} */
      @Override
      protected <S> Void visitBinaryExpression(BinaryExpression binary, S context) {
        if (binary instanceof ComparisonOperator || binary instanceof IsDistinctExpression) {
          ScalarExpressionSchemas.validateBinaryComparison(binary.getLeftExpression(), binary.getRightExpression(),
              operand -> scalarOperand(operand, scope));
        }
        return super.visitBinaryExpression(binary, context);
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(InExpression in, S context) {
        Schema left = scalarOperand(in.getLeftExpression(), scope);
        Expression candidates = in.getRightExpression();
        if (candidates instanceof ExpressionList<?> list) {
          for (Expression candidate : list) {
            ScalarExpressionSchemas.validateBinaryComparison(left, scalarOperand(candidate, scope));
          }
        } else {
          ScalarExpressionSchemas.validateBinaryComparison(left, scalarOperand(candidates, scope));
        }
        return super.visit(in, context);
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(Between between, S context) {
        ScalarExpressionSchemas.validateBinaryComparison(between.getLeftExpression(),
            between.getBetweenExpressionStart(), operand -> scalarOperand(operand, scope));
        ScalarExpressionSchemas.validateBinaryComparison(between.getLeftExpression(), between.getBetweenExpressionEnd(),
            operand -> scalarOperand(operand, scope));
        return super.visit(between, context);
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(LikeExpression like, S context) {
        ScalarExpressionSchemas.validateBinaryComparison(like.getLeftExpression(), like.getRightExpression(),
            operand -> scalarOperand(operand, scope));
        if (like.getEscape() != null) {
          ScalarExpressionSchemas.validateBinaryComparison(like.getLeftExpression(), like.getEscape(),
              operand -> scalarOperand(operand, scope));
        }
        return super.visit(like, context);
      }

      /** {@inheritDoc} */
      @Override
      public <S> Void visit(CaseExpression choice, S context) {
        if (choice.getSwitchExpression() != null && choice.getWhenClauses() != null) {
          choice.getWhenClauses()
              .forEach(branch -> ScalarExpressionSchemas.validateBinaryComparison(choice.getSwitchExpression(),
                  branch.getWhenExpression(), operand -> scalarOperand(operand, scope)));
        }
        return super.visit(choice, context);
      }

      @Override
      public <S> Void visit(Select nested, S context) {
        visitSelect(nested, scope);
        return null;
      }

      @Override
      public <S> Void visit(ParenthesedSelect nested, S context) {
        return visit((Select) nested, context);
      }

      @Override
      public <S> Void visit(AnyComparisonExpression comparison, S context) {
        visitSelect(comparison.getSelect(), scope);
        return null;
      }
    });
  }

  /**
   * Resolves known scalar and interval operands without evaluating their values.
   *
   * @param expression
   *          scoped operand
   * @param scope
   *          current lexical scope
   * @return known scalar schema, or null when unresolved
   */
  private static Schema scalarOperand(Expression expression, Scope scope) {
    Expression operand = ExpressionEvaluator.unwrapParenthesis(expression);
    if (operand instanceof AnyComparisonExpression quantified) {
      return projectedValue(quantified.getSelect(), scope);
    }
    if (operand instanceof net.sf.jsqlparser.expression.Function quantified
        && JSqlParserSupport.isQuantifiedOperand(quantified) && quantified.getParameters() != null
        && quantified.getParameters().size() == 1) {
      Schema array = JdbcTypeMapper.nonNullSchema(scalarOperand(quantified.getParameters().getFirst(), scope));
      return array != null && array.getType() == Schema.Type.ARRAY
          ? JdbcTypeMapper.nonNullSchema(array.getElementType())
          : null;
    }
    Schema scalar = ScalarExpressionSchemas.resolve(operand, name -> scope.columns().apply(new Column(name)) != null);
    if (scalar != null) {
      return scalar;
    }
    if (operand instanceof HexValue) {
      return JdbcTypeMapper.declaredSchema(java.sql.JDBCType.VARBINARY);
    }
    CastExpression cast = CastExpressions.asCast(operand, name -> scope.columns().apply(new Column(name)) != null);
    if (cast != null && cast.getColDataType() != null) {
      java.sql.JDBCType type = ScalarExpressionSchemas.declaredJdbcType(cast.getColDataType().getDataType());
      if (type == java.sql.JDBCType.CLOB || type == java.sql.JDBCType.NCLOB) {
        return Schema.create(Schema.Type.STRING);
      }
    }
    Schema interval = IntervalExpressionTypes.resolve(operand, scope.columns(),
        nested -> projectedValue(nested, scope));
    return interval != null ? interval : resolve(operand, scope);
  }

  /**
   * Validates a query body and its WITH declarations.
   *
   * @param query
   *          query body
   * @param outer
   *          enclosing scope
   */
  private static void visitSelect(Select query, Scope outer) {
    Scope inherited = withDeclarations(query, outer);
    if (query.getWithItemsList() != null) {
      query.getWithItemsList().forEach(cte -> visitSelect(cte.getSelect(), inherited));
    }
    if (query instanceof ParenthesedSelect nested) {
      visitSelect(nested.getSelect(), inherited);
    } else if (query instanceof SetOperationList set) {
      set.getSelects().forEach(branch -> visitSelect(branch, inherited));
    } else if (query instanceof Values values) {
      visitExpression(values.getExpressions(), inherited);
    } else if (query instanceof PlainSelect plain) {
      Scope local = localScope(plain, inherited);
      visitInput(plain.getFromItem(), local);
      if (plain.getJoins() != null) {
        plain.getJoins().forEach(join -> {
          visitInput(join.getRightItem(), local);
          join.getOnExpressions().forEach(on -> visitExpression(on, local));
        });
      }
      plain.getSelectItems().forEach(item -> visitExpression(item.getExpression(), local));
      visitExpression(plain.getWhere(), local);
      visitExpression(plain.getHaving(), local);
      visitExpression(plain.getQualify(), local);
      if (plain.getGroupBy() != null) {
        visitExpression(plain.getGroupBy().getGroupByExpressionList(), local);
        if (plain.getGroupBy().getGroupingSets() != null) {
          plain.getGroupBy().getGroupingSets().forEach(group -> visitExpression(group, local));
        }
      }
      if (plain.getDistinct() != null && plain.getDistinct().getOnSelectItems() != null) {
        plain.getDistinct().getOnSelectItems().forEach(item -> visitExpression(item.getExpression(), local));
      }
      if (plain.getWindowDefinitions() != null) {
        plain.getWindowDefinitions()
            .forEach(window -> window.getAllExpressions().forEach(expression -> visitExpression(expression, local)));
      }
      if (plain.getOrderByElements() != null) {
        plain.getOrderByElements().forEach(key -> visitExpression(key.getExpression(), local));
      }
    }
    if (!(query instanceof PlainSelect) && query.getOrderByElements() != null) {
      query.getOrderByElements().forEach(key -> visitExpression(key.getExpression(), inherited));
    }
  }

  /**
   * Validates nested inputs, grouped joins, and table-function arguments.
   *
   * @param input
   *          input to inspect, possibly null
   * @param scope
   *          visible declarations
   */
  private static void visitInput(FromItem input, Scope scope) {
    if (input instanceof Select select) {
      visitSelect(select, scope);
    } else if (input instanceof ParenthesedFromItem group) {
      visitInput(group.getFromItem(), scope);
      if (group.getJoins() != null) {
        group.getJoins().forEach(join -> {
          visitInput(join.getRightItem(), scope);
          join.getOnExpressions().forEach(on -> visitExpression(on, scope));
        });
      }
    } else if (input instanceof TableFunction function) {
      visitExpression(function.getFunction(), scope);
    }
  }

  /**
   * Applies local WITH shadowing without resolving recursive declarations yet.
   *
   * @param query
   *          declaring query
   * @param outer
   *          enclosing scope
   * @return scope with local declarations
   */
  private static Scope withDeclarations(Select query, Scope outer) {
    Map<Identifier, WithItem<?>> ctes = new HashMap<>(outer.ctes());
    if (query.getWithItemsList() != null) {
      query.getWithItemsList().forEach(cte -> ctes.put(Identifier.of(cte.getAliasName()), cte));
    }
    return new Scope(outer.columns(), outer.tables(), ctes, outer.resolving(), outer.bound());
  }

  /**
   * Resolves local columns before correlated outer references.
   *
   * @param query
   *          current SELECT
   * @param outer
   *          enclosing scope
   * @return local scope
   */
  private static Scope localScope(PlainSelect query, Scope outer) {
    // Result construction can repeat validation without nested table schemas.
    // An unknown local input might shadow a correlated column, so defer strict
    // CONVERT disambiguation until those schemas or correlation bindings exist.
    boolean bound = outer.bound() && inputs(query).stream().allMatch(input -> !(input instanceof Table table)
        || Identifier.of(table.getName()).resolve(outer.ctes()) != null || outer.tables().apply(table) != null);
    return new Scope(column -> {
      Binding match = localColumn(query, column, outer);
      if (match.present()) {
        return match.schema();
      }
      String qualifier = column.getTable() == null ? null : column.getTable().getName();
      boolean localQualifier = qualifier != null && inputs(query).stream()
          .anyMatch(input -> Identifier.of(qualifier).matches(Identifier.of(sourceName(input))));
      return localQualifier ? null : outer.columns().apply(column);
    }, outer.tables(), outer.ctes(), outer.resolving(), bound);
  }

  /**
   * Resolves only the current query's inputs, without correlation fallback.
   *
   * @param query
   *          declaring SELECT
   * @param column
   *          requested column
   * @param scope
   *          enclosing declarations
   * @return matching local binding, or an absent binding
   */
  private static Binding localColumn(PlainSelect query, Column column, Scope scope) {
    Binding match = new Binding(false, null);
    for (FromItem input : inputs(query)) {
      Binding binding = sourceColumn(input, column, scope);
      if (binding.present()) {
        if (match.present()) {
          return coalescedColumn(query, column, scope);
        }
        match = binding;
      }
    }
    return match;
  }

  /**
   * Reuses joined-schema binding for USING and NATURAL common columns. Other
   * duplicate or unresolved inputs retain an unknown local binding.
   *
   * @param query
   *          joined SELECT
   * @param column
   *          duplicate local reference
   * @param scope
   *          source schema context
   * @return coalesced declaration, or an unknown local binding
   */
  private static Binding coalescedColumn(PlainSelect query, Column column, Scope scope) {
    if (column.getTable() != null && column.getTable().getName() != null || query.getJoins() == null) {
      return new Binding(true, null);
    }
    boolean merged = query.getJoins().stream()
        .anyMatch(join -> join.isNatural() || join.getUsingColumns() != null && join.getUsingColumns().stream()
            .anyMatch(key -> Identifier.of(key.getColumnName()).matches(Identifier.of(column.getColumnName()))));
    if (!merged) {
      return new Binding(true, null);
    }
    List<JoinRecordReader.JoinTable> tables = new ArrayList<>();
    List<FromItem> sources = inputs(query);
    for (int i = 0; i < sources.size(); i++) {
      FromItem input = sources.get(i);
      if (!(input instanceof Table table) || Identifier.of(table.getName()).resolve(scope.ctes()) != null
          || input.getAlias() != null && input.getAlias().getAliasColumns() != null
              && !input.getAlias().getAliasColumns().isEmpty()) {
        return new Binding(true, null);
      }
      Schema schema = scope.tables().apply(table);
      if (schema == null) {
        return new Binding(true, null);
      }
      List<String> using = List.of();
      SqlParser.JoinType type = SqlParser.JoinType.BASE;
      if (i > 0) {
        var join = query.getJoins().get(i - 1);
        type = join.isFull()
            ? SqlParser.JoinType.FULL_OUTER
            : join.isRight()
                ? SqlParser.JoinType.RIGHT_OUTER
                : join.isLeft() ? SqlParser.JoinType.LEFT_OUTER : SqlParser.JoinType.INNER;
        using = join.isNatural()
            ? List.of(column.getColumnName())
            : join.getUsingColumns() == null
                ? List.of()
                : join.getUsingColumns().stream().map(Column::getColumnName).toList();
      }
      tables.add(new JoinRecordReader.JoinTable(table.getName(),
          input.getAlias() == null ? null : input.getAlias().getName(), schema, List.of(), type, null, using, null));
    }
    try {
      return new Binding(true, JoinRecordReader.columnSchema(tables, null, column.getColumnName()));
    } catch (IllegalArgumentException ambiguous) {
      return new Binding(true, null);
    }
  }

  /**
   * Lists inputs in join order.
   *
   * @param query
   *          SELECT body
   * @return input list, excluding null inputs
   */
  private static List<FromItem> inputs(PlainSelect query) {
    List<FromItem> result = new ArrayList<>();
    if (query.getFromItem() != null) {
      result.add(query.getFromItem());
    }
    if (query.getJoins() != null) {
      query.getJoins().forEach(join -> result.add(join.getRightItem()));
    }
    return result;
  }

  /**
   * Resolves one input without borrowing same-named outer columns.
   *
   * @param input
   *          local input
   * @param column
   *          requested column
   * @param scope
   *          enclosing declarations
   * @return whether the source owns the column and its known schema
   */
  private static Binding sourceColumn(FromItem input, Column column, Scope scope) {
    String qualifier = column.getTable() == null ? null : column.getTable().getName();
    String name = sourceName(input);
    if (qualifier != null && !Identifier.of(qualifier).matches(Identifier.of(name))) {
      return new Binding(false, null);
    }
    List<String> aliases = input.getAlias() != null && input.getAlias().getAliasColumns() != null
        ? input.getAlias().getAliasColumns().stream().map(alias -> alias.name).toList()
        : List.of();
    if (input instanceof Table table) {
      WithItem<?> cte = Identifier.of(table.getName()).resolve(scope.ctes());
      if (cte != null) {
        List<String> outputAliases = !aliases.isEmpty()
            ? aliases
            : cte.getWithItemList() == null ? List.of() : cte.getWithItemList().stream().map(Object::toString).toList();
        return projectedColumn(cte.getSelect(), column.getColumnName(), outputAliases, scope);
      }
      Schema schema = scope.tables().apply(table);
      if (schema == null) {
        return new Binding(true, null);
      }
      if (!aliases.isEmpty()) {
        for (int i = 0; i < aliases.size() && i < schema.getFields().size(); i++) {
          if (Identifier.of(column.getColumnName()).matches(Identifier.of(aliases.get(i)))) {
            return new Binding(true, schema.getFields().get(i).schema());
          }
        }
        return new Binding(false, null);
      }
      return schemaColumn(schema, column.getColumnName());
    }
    if (input instanceof Select select) {
      return projectedColumn(select, column.getColumnName(), aliases, scope);
    }
    if (input instanceof TableFunction function) {
      return unnestColumn(function, column, aliases, scope);
    }
    return new Binding(true, null);
  }

  /**
   * Returns the SQL-visible qualifier of a table or derived input.
   *
   * @param input
   *          local source
   * @return its alias or unaliased table name, possibly null
   */
  private static String sourceName(FromItem input) {
    return input.getAlias() != null
        ? input.getAlias().getName()
        : input instanceof Table table ? table.getName() : null;
  }

  /**
   * Reuses SQL label and ambiguity handling from the engine's column mappings.
   *
   * @param schema
   *          source record schema
   * @param name
   *          SQL column name
   * @return resolved field or an absent/uncertain binding
   */
  private static Binding schemaColumn(Schema schema, String name) {
    Map<String, String> names = ColumnMappingUtil.unqualifiedColumnMapping(schema);
    try {
      String canonical = ColumnMappingUtil.canonicalFieldName(null, name, Map.of(), names, names);
      canonical = MaterializedNames.sqlReference(schema, name, canonical);
      Schema.Field field = canonical == null ? null : schema.getField(canonical);
      return field == null ? new Binding(false, null) : new Binding(true, field.schema());
    } catch (IllegalArgumentException ambiguous) {
      return new Binding(true, null);
    }
  }

  /**
   * Reuses UNNEST alias and record expansion rules without evaluating elements.
   *
   * @param input
   *          table function
   * @param column
   *          requested output column
   * @param aliases
   *          explicit output aliases
   * @param scope
   *          available argument declarations
   * @return known element/ordinality declaration or an unknown local binding
   */
  private static Binding unnestColumn(TableFunction input, Column column, List<String> aliases, Scope scope) {
    var function = input.getFunction();
    if (function == null || !"UNNEST".equalsIgnoreCase(function.getName()) || function.getParameters() == null
        || function.getParameters().size() != 1) {
      return new Binding(true, null);
    }
    Expression argument = function.getParameters().getFirst();
    Schema array = JdbcTypeMapper.nonNullSchema(resolve(argument, scope));
    if (array == null || array.getType() != Schema.Type.ARRAY) {
      return new Binding(true, null);
    }
    Schema element = JdbcTypeMapper.nonNullSchema(array.getElementType());
    String alias = input.getAlias() == null ? "unnest" : input.getAlias().getName();
    var definition = new SqlParser.UnnestDefinition(argument, "ORDINALITY".equalsIgnoreCase(input.getWithClause()),
        aliases);
    var reference = new SqlParser.TableReference(null, null, alias, SqlParser.JoinType.BASE, null, null, null, null,
        List.of(), null, definition, false, null, List.of(), List.of());
    try {
      var plan = UnnestTableBuilder.AliasPlan.build(definition, element, alias);
      return schemaColumn(UnnestTableBuilder.buildSchema(reference, element, plan), column.getColumnName());
    } catch (java.sql.SQLException invalid) {
      throw new IllegalArgumentException(invalid.getMessage(), invalid);
    }
  }

  /**
   * Resolves a derived or CTE projection in its own input scope.
   *
   * @param query
   *          projected query
   * @param name
   *          output column name
   * @param aliases
   *          positional output aliases
   * @param outer
   *          enclosing declarations
   * @return matching declaration, or an unresolved binding
   */
  private static Binding projectedColumn(Select query, String name, List<String> aliases, Scope outer) {
    if (!outer.resolving().add(query)) {
      return new Binding(true, null);
    }
    try {
      Scope inherited = withDeclarations(query, outer);
      if (query instanceof ParenthesedSelect nested) {
        return projectedColumn(nested.getSelect(), name, aliases, inherited);
      }
      if (query instanceof PlainSelect plain) {
        if (!aliases.isEmpty()
            && plain.getSelectItems().stream().anyMatch(item -> item.getExpression() instanceof AllColumns)) {
          // Wildcards can expand to several positions. Let bound engine metadata
          // establish their types rather than assigning aliases by select-item index.
          boolean exported = aliases.stream().anyMatch(alias -> Identifier.of(name).matches(Identifier.of(alias)));
          return new Binding(exported, null);
        }
        Scope local = localScope(plain, inherited);
        Binding match = new Binding(false, null);
        for (int i = 0; i < plain.getSelectItems().size(); i++) {
          var item = plain.getSelectItems().get(i);
          Expression expression = item.getExpression();
          if (expression instanceof AllColumns) {
            Column reference = expression instanceof AllTableColumns all
                ? new Column(all.getTable(), name)
                : new Column(name);
            Binding binding = localColumn(plain, reference, inherited);
            if (binding.present()) {
              if (match.present()) {
                return new Binding(true, null);
              }
              match = binding;
            }
            continue;
          }
          String label = i < aliases.size()
              ? aliases.get(i)
              : item.getAlias() != null
                  ? item.getAlias().getName()
                  : expression instanceof Column column ? column.getColumnName() : expression.toString();
          if (Identifier.of(name).matches(Identifier.of(label))) {
            if (match.present()) {
              return new Binding(true, null);
            }
            match = new Binding(true, resolve(expression, local));
          }
        }
        return match;
      }
      return new Binding(true, null);
    } finally {
      outer.resolving().remove(query);
    }
  }

  /**
   * A lexical column resolver with local CTEs and a recursive-resolution guard.
   *
   * @param columns
   *          visible column declarations
   * @param tables
   *          physical or materialized source schema resolver
   * @param ctes
   *          visible WITH declarations
   * @param resolving
   *          projections currently being resolved
   * @param bound
   *          whether source schemas have been supplied for declaration validation
   */
  private record Scope(Function<Column, Schema> columns, Function<Table, Schema> tables,
      Map<Identifier, WithItem<?>> ctes, Set<Select> resolving, boolean bound) {
  }

  /**
   * Distinguishes an unknown local type from an absent local column.
   *
   * @param present
   *          whether the source owns or may own the column
   * @param schema
   *          known declaration, possibly null
   */
  private record Binding(boolean present, Schema schema) {
  }
}
