package jparq;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.Time;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.AggregateFunctions;

/**
 * Unit tests covering helper utilities inside {@link AggregateFunctions}.
 */
class AggregateFunctionsHelperTest {

  private static Method expressionsEquivalentMethod;
  private static Method functionsEquivalentMethod;
  private static Method columnsEquivalentMethod;
  private static Method tablesEquivalentMethod;
  private static Method aggregateSpecMethod;

  @BeforeAll
  static void setUpReflection() throws NoSuchMethodException {
    expressionsEquivalentMethod = AggregateFunctions.class.getDeclaredMethod("expressionsEquivalent", Expression.class,
        Expression.class);
    expressionsEquivalentMethod.setAccessible(true);

    functionsEquivalentMethod = AggregateFunctions.class.getDeclaredMethod("functionsEquivalent", Function.class,
        Function.class);
    functionsEquivalentMethod.setAccessible(true);

    columnsEquivalentMethod = AggregateFunctions.class.getDeclaredMethod("columnsEquivalent",
        net.sf.jsqlparser.schema.Column.class, net.sf.jsqlparser.schema.Column.class);
    columnsEquivalentMethod.setAccessible(true);

    tablesEquivalentMethod = AggregateFunctions.class.getDeclaredMethod("tablesEquivalent", Table.class, Table.class);
    tablesEquivalentMethod.setAccessible(true);

    aggregateSpecMethod = AggregateFunctions.class.getDeclaredMethod("aggregateSpec", Function.class, String.class);
    aggregateSpecMethod.setAccessible(true);
  }

  @Test
  void expressionsEquivalentCoversStructuralBranches() throws Exception {
    Expression between = CCJSqlParserUtil.parseExpression("hp BETWEEN 100 AND 200");
    Expression betweenNot = CCJSqlParserUtil.parseExpression("hp NOT BETWEEN 100 AND 200");
    assertTrue(exprEq(between, between));
    assertFalse(exprEq(between, betweenNot), "NOT flag should affect BETWEEN equivalence");

    Expression inExpression = CCJSqlParserUtil.parseExpression("cyl IN (4,6)");
    Expression notInExpression = CCJSqlParserUtil.parseExpression("cyl NOT IN (4,6)");
    assertFalse(exprEq(inExpression, notInExpression), "NOT flag should affect IN equivalence");

    Expression isNull = CCJSqlParserUtil.parseExpression("carb IS NULL");
    Expression isNotNull = CCJSqlParserUtil.parseExpression("carb IS NOT NULL");
    assertTrue(exprEq(isNull, isNull));
    assertFalse(exprEq(isNull, isNotNull));

    Expression caseA = CCJSqlParserUtil.parseExpression("CASE WHEN gear = 4 THEN 'four' ELSE 'other' END");
    Expression caseB = CCJSqlParserUtil.parseExpression("CASE WHEN gear = 4 THEN 'four' ELSE 'other' END");
    Expression caseDifferent = CCJSqlParserUtil.parseExpression("CASE WHEN gear = 5 THEN 'five' ELSE 'other' END");
    assertTrue(exprEq(caseA, caseB));
    assertFalse(exprEq(caseA, caseDifferent), "Different WHEN clause should break equivalence");

    Expression signed = CCJSqlParserUtil.parseExpression("-hp");
    Expression signedOther = CCJSqlParserUtil.parseExpression("+hp");
    assertTrue(exprEq(signed, signed));
    assertFalse(exprEq(signed, signedOther), "Sign should be part of expression equivalence");

    Expression castA = CCJSqlParserUtil.parseExpression("CAST(hp AS DECIMAL)");
    Expression castB = CCJSqlParserUtil.parseExpression("CAST(hp AS DECIMAL)");
    Expression castDifferentType = CCJSqlParserUtil.parseExpression("CAST(hp AS INT)");
    assertTrue(exprEq(castA, castB));
    assertFalse(exprEq(castA, castDifferentType));

    Expression notExpression = CCJSqlParserUtil.parseExpression("NOT (hp > 100)");
    assertTrue(exprEq(notExpression, notExpression));

    Expression parenthesis = CCJSqlParserUtil.parseExpression("(hp)");
    Expression plainColumn = CCJSqlParserUtil.parseExpression("hp");
    assertFalse(exprEq(parenthesis, plainColumn), "Different expression classes should not match");
  }

  @Test
  void expressionsEquivalentHandlesLiteralsAndParameters() throws Exception {
    assertTrue(exprEq(new NullValue(), new NullValue()));
    assertFalse(exprEq(new NullValue(), new StringValue("x")));

    assertTrue(exprEq(new StringValue("abc"), new StringValue("abc")));
    assertFalse(exprEq(new StringValue("abc"), new StringValue("def")));

    assertTrue(exprEq(new DateValue(Date.valueOf("2020-01-01")), new DateValue(Date.valueOf("2020-01-01"))));
    assertFalse(exprEq(new DateValue(Date.valueOf("2020-01-01")), new DateValue(Date.valueOf("2021-01-01"))));

    TimeValue tenElevenTwelve = new TimeValue().withValue(Time.valueOf("10:11:12"));
    TimeValue tenElevenThirteen = new TimeValue().withValue(Time.valueOf("10:11:13"));
    assertTrue(exprEq(tenElevenTwelve, tenElevenTwelve));
    assertFalse(exprEq(tenElevenTwelve, tenElevenThirteen));

    assertTrue(exprEq(new TimestampValue("2024-01-02 10:11:12"), new TimestampValue("2024-01-02 10:11:12")));

    JdbcNamedParameter named = new JdbcNamedParameter();
    named.setName("param");
    JdbcNamedParameter otherNamed = new JdbcNamedParameter();
    otherNamed.setName("other");
    assertTrue(exprEq(named, named));
    assertFalse(exprEq(named, otherNamed));

    JdbcParameter param = new JdbcParameter();
    param.setIndex(1);
    JdbcParameter otherParam = new JdbcParameter();
    otherParam.setIndex(2);
    assertTrue(exprEq(param, param));
    assertFalse(exprEq(param, otherParam));
  }

  @Test
  void columnsAndTablesEquivalentRespectQualifiers() throws Exception {
    Table leftTable = new Table("Cars");
    leftTable.setAlias(new Alias("c"));
    Table rightTable = new Table("cars");
    rightTable.setAlias(new Alias("C"));

    net.sf.jsqlparser.schema.Column left = new net.sf.jsqlparser.schema.Column(leftTable, "Model");
    net.sf.jsqlparser.schema.Column right = new net.sf.jsqlparser.schema.Column(rightTable, "model");
    assertTrue(colEq(left, right), "Table aliases and column names should be normalized");

    Table differentAlias = new Table("cars");
    differentAlias.setAlias(new Alias("other"));
    net.sf.jsqlparser.schema.Column differentColumn = new net.sf.jsqlparser.schema.Column(differentAlias, "model");
    assertFalse(colEq(left, differentColumn), "Different aliases should prevent equivalence");

    Table otherTable = new Table("trucks");
    net.sf.jsqlparser.schema.Column otherTableColumn = new net.sf.jsqlparser.schema.Column(otherTable, "model");
    assertFalse(tblEq(leftTable, otherTable), "Different table names should not match");
    assertTrue(tblEq(leftTable, rightTable), "Normalized table names and aliases should match");
    assertFalse(colEq(left, otherTableColumn), "Columns from different tables should differ");

    assertTrue(colEq(null, null), "Null columns should be considered equivalent");
  }

  @Test
  void functionsEquivalentValidatesNameFlagsAndArguments() throws Exception {
    Function lowerSum = (Function) CCJSqlParserUtil.parseExpression("sum(hp)");
    Function upperSum = (Function) CCJSqlParserUtil.parseExpression("SUM(hp)");
    assertTrue(funcEq(lowerSum, upperSum), "Function names should be compared case-insensitively");

    Function distinctSum = (Function) CCJSqlParserUtil.parseExpression("SUM(DISTINCT hp)");
    assertFalse(funcEq(lowerSum, distinctSum), "Distinct flag should affect equivalence");

    Function countStar = new Function();
    countStar.setName("COUNT");
    countStar.setParameters(new ExpressionList<>(new AllColumns()));
    countStar.setAllColumns(true);
    Function countColumn = (Function) CCJSqlParserUtil.parseExpression("COUNT(hp)");
    assertFalse(funcEq(countStar, countColumn), "COUNT(*) should not equal COUNT(column)");

    Function sumWithTwoArgs = (Function) CCJSqlParserUtil.parseExpression("SUM(hp, mpg)");
    assertFalse(funcEq(lowerSum, sumWithTwoArgs), "Different argument counts should fail equivalence");
  }

  @Test
  void aggregateSpecValidationEnforcesRules() throws Exception {
    Function validStringAgg = (Function) CCJSqlParserUtil.parseExpression("STRING_AGG(model, ',')");
    Object spec = aggSpec(validStringAgg, "agg_label");
    assertNotNull(spec, "Valid STRING_AGG should produce an AggregateSpec");

    Function missingSeparator = (Function) CCJSqlParserUtil.parseExpression("STRING_AGG(model)");
    assertThrows(IllegalArgumentException.class, () -> aggSpec(missingSeparator, "missing_sep"),
        "STRING_AGG requires two arguments");

    Function sumDistinct = (Function) CCJSqlParserUtil.parseExpression("SUM(DISTINCT hp)");
    assertThrows(IllegalArgumentException.class, () -> aggSpec(sumDistinct, "distinct_disallowed"),
        "Distinct aggregates should be rejected");

    Function emptySum = new Function();
    emptySum.setName("SUM");
    emptySum.setParameters(new ExpressionList<Expression>());
    assertThrows(IllegalArgumentException.class, () -> aggSpec(emptySum, "empty_sum"),
        "Aggregates require at least one argument");

    Function countStarWithArgs = new Function();
    countStarWithArgs.setName("COUNT");
    countStarWithArgs.setAllColumns(true);
    ExpressionList<Expression> countArgs = new ExpressionList<>();
    countArgs.addExpressions(CCJSqlParserUtil.parseExpression("1"));
    countStarWithArgs.setParameters(countArgs);
    AggregateFunctions.AggregateSpec countSpec = (AggregateFunctions.AggregateSpec) aggSpec(countStarWithArgs,
        "count_star_args");
    assertTrue(countSpec.countStar(), "COUNT(*) should be detected even if parameters are present");
    assertTrue(countSpec.arguments().isEmpty(), "COUNT(*) should not retain arguments");
  }

  private boolean exprEq(Expression first, Expression second) {
    try {
      return (boolean) expressionsEquivalentMethod.invoke(null, first, second);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new IllegalStateException(cause);
    }
  }

  private boolean funcEq(Function first, Function second) {
    try {
      return (boolean) functionsEquivalentMethod.invoke(null, first, second);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new IllegalStateException(cause);
    }
  }

  private boolean colEq(net.sf.jsqlparser.schema.Column left, net.sf.jsqlparser.schema.Column right) {
    try {
      return (boolean) columnsEquivalentMethod.invoke(null, left, right);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new IllegalStateException(cause);
    }
  }

  private boolean tblEq(Table left, Table right) {
    try {
      return (boolean) tablesEquivalentMethod.invoke(null, left, right);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new IllegalStateException(cause);
    }
  }

  private Object aggSpec(Function function, String label) {
    try {
      return aggregateSpecMethod.invoke(null, function, label);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new IllegalStateException(cause);
    }
  }
}
