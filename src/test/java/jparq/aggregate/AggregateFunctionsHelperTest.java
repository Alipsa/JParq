package jparq.aggregate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static se.alipsa.jparq.engine.function.AggregateFunctions.aggregateSpec;
import static se.alipsa.jparq.engine.function.AggregateFunctions.columnsEquivalent;
import static se.alipsa.jparq.engine.function.AggregateFunctions.expressionsEquivalent;
import static se.alipsa.jparq.engine.function.AggregateFunctions.functionsEquivalent;
import static se.alipsa.jparq.engine.function.AggregateFunctions.tablesEquivalent;

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
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.function.AggregateFunctions;

/**
 * Unit tests covering helper utilities inside {@link AggregateFunctions}.
 */
class AggregateFunctionsHelperTest {

  @Test
  void expressionsEquivalentCoversStructuralBranches() throws Exception {
    Expression between = CCJSqlParserUtil.parseExpression("hp BETWEEN 100 AND 200");
    Expression betweenNot = CCJSqlParserUtil.parseExpression("hp NOT BETWEEN 100 AND 200");
    assertTrue(expressionsEquivalent(between, between));
    assertFalse(expressionsEquivalent(between, betweenNot), "NOT flag should affect BETWEEN equivalence");

    Expression inExpression = CCJSqlParserUtil.parseExpression("cyl IN (4,6)");
    Expression notInExpression = CCJSqlParserUtil.parseExpression("cyl NOT IN (4,6)");
    assertFalse(expressionsEquivalent(inExpression, notInExpression), "NOT flag should affect IN equivalence");

    Expression isNull = CCJSqlParserUtil.parseExpression("carb IS NULL");
    Expression isNotNull = CCJSqlParserUtil.parseExpression("carb IS NOT NULL");
    assertTrue(expressionsEquivalent(isNull, isNull));
    assertFalse(expressionsEquivalent(isNull, isNotNull));

    Expression caseA = CCJSqlParserUtil.parseExpression("CASE WHEN gear = 4 THEN 'four' ELSE 'other' END");
    Expression caseB = CCJSqlParserUtil.parseExpression("CASE WHEN gear = 4 THEN 'four' ELSE 'other' END");
    Expression caseDifferent = CCJSqlParserUtil.parseExpression("CASE WHEN gear = 5 THEN 'five' ELSE 'other' END");
    assertTrue(expressionsEquivalent(caseA, caseB));
    assertFalse(expressionsEquivalent(caseA, caseDifferent), "Different WHEN clause should break equivalence");

    Expression signed = CCJSqlParserUtil.parseExpression("-hp");
    Expression signedOther = CCJSqlParserUtil.parseExpression("+hp");
    assertTrue(expressionsEquivalent(signed, signed));
    assertFalse(expressionsEquivalent(signed, signedOther), "Sign should be part of expression equivalence");

    Expression castA = CCJSqlParserUtil.parseExpression("CAST(hp AS DECIMAL)");
    Expression castB = CCJSqlParserUtil.parseExpression("CAST(hp AS DECIMAL)");
    Expression castDifferentType = CCJSqlParserUtil.parseExpression("CAST(hp AS INT)");
    assertTrue(expressionsEquivalent(castA, castB));
    assertFalse(expressionsEquivalent(castA, castDifferentType));

    Expression notExpression = CCJSqlParserUtil.parseExpression("NOT (hp > 100)");
    assertTrue(expressionsEquivalent(notExpression, notExpression));

    Expression parenthesis = CCJSqlParserUtil.parseExpression("(hp)");
    Expression plainColumn = CCJSqlParserUtil.parseExpression("hp");
    assertFalse(expressionsEquivalent(parenthesis, plainColumn), "Different expression classes should not match");
  }

  @Test
  void expressionsEquivalentHandlesLiteralsAndParameters() throws Exception {
    assertTrue(expressionsEquivalent(new NullValue(), new NullValue()));
    assertFalse(expressionsEquivalent(new NullValue(), new StringValue("x")));

    assertTrue(expressionsEquivalent(new StringValue("abc"), new StringValue("abc")));
    assertFalse(expressionsEquivalent(new StringValue("abc"), new StringValue("def")));

    assertTrue(
        expressionsEquivalent(new DateValue(Date.valueOf("2020-01-01")), new DateValue(Date.valueOf("2020-01-01"))));
    assertFalse(
        expressionsEquivalent(new DateValue(Date.valueOf("2020-01-01")), new DateValue(Date.valueOf("2021-01-01"))));

    TimeValue tenElevenTwelve = new TimeValue().withValue(Time.valueOf("10:11:12"));
    TimeValue tenElevenThirteen = new TimeValue().withValue(Time.valueOf("10:11:13"));
    assertTrue(expressionsEquivalent(tenElevenTwelve, tenElevenTwelve));
    assertFalse(expressionsEquivalent(tenElevenTwelve, tenElevenThirteen));

    assertTrue(
        expressionsEquivalent(new TimestampValue("2024-01-02 10:11:12"), new TimestampValue("2024-01-02 10:11:12")));

    JdbcNamedParameter named = new JdbcNamedParameter();
    named.setName("param");
    JdbcNamedParameter otherNamed = new JdbcNamedParameter();
    otherNamed.setName("other");
    assertTrue(expressionsEquivalent(named, named));
    assertFalse(expressionsEquivalent(named, otherNamed));

    JdbcParameter param = new JdbcParameter();
    param.setIndex(1);
    JdbcParameter otherParam = new JdbcParameter();
    otherParam.setIndex(2);
    assertTrue(expressionsEquivalent(param, param));
    assertFalse(expressionsEquivalent(param, otherParam));
  }

  @Test
  void columnsAndTablesEquivalentRespectQualifiers() throws Exception {
    Table leftTable = new Table("Cars");
    leftTable.setAlias(new Alias("c"));
    Table rightTable = new Table("cars");
    rightTable.setAlias(new Alias("C"));

    net.sf.jsqlparser.schema.Column left = new net.sf.jsqlparser.schema.Column(leftTable, "Model");
    net.sf.jsqlparser.schema.Column right = new net.sf.jsqlparser.schema.Column(rightTable, "model");
    assertTrue(columnsEquivalent(left, right), "Table aliases and column names should be normalized");

    Table differentAlias = new Table("cars");
    differentAlias.setAlias(new Alias("other"));
    net.sf.jsqlparser.schema.Column differentColumn = new net.sf.jsqlparser.schema.Column(differentAlias, "model");
    assertFalse(columnsEquivalent(left, differentColumn), "Different aliases should prevent equivalence");

    Table otherTable = new Table("trucks");
    net.sf.jsqlparser.schema.Column otherTableColumn = new net.sf.jsqlparser.schema.Column(otherTable, "model");
    assertFalse(tablesEquivalent(leftTable, otherTable), "Different table names should not match");
    assertTrue(tablesEquivalent(leftTable, rightTable), "Normalized table names and aliases should match");
    assertFalse(columnsEquivalent(left, otherTableColumn), "Columns from different tables should differ");

    assertTrue(columnsEquivalent(null, null), "Null columns should be considered equivalent");
  }

  @Test
  void functionsEquivalentValidatesNameFlagsAndArguments() throws Exception {
    Function lowerSum = (Function) CCJSqlParserUtil.parseExpression("sum(hp)");
    Function upperSum = (Function) CCJSqlParserUtil.parseExpression("SUM(hp)");
    assertTrue(functionsEquivalent(lowerSum, upperSum), "Function names should be compared case-insensitively");

    Function distinctSum = (Function) CCJSqlParserUtil.parseExpression("SUM(DISTINCT hp)");
    assertFalse(functionsEquivalent(lowerSum, distinctSum), "Distinct flag should affect equivalence");

    Function countStar = new Function();
    countStar.setName("COUNT");
    countStar.setParameters(new ExpressionList<>(new AllColumns()));
    countStar.setAllColumns(true);
    Function countColumn = (Function) CCJSqlParserUtil.parseExpression("COUNT(hp)");
    assertFalse(functionsEquivalent(countStar, countColumn), "COUNT(*) should not equal COUNT(column)");

    Function sumWithTwoArgs = (Function) CCJSqlParserUtil.parseExpression("SUM(hp, mpg)");
    assertFalse(functionsEquivalent(lowerSum, sumWithTwoArgs), "Different argument counts should fail equivalence");
  }

  @Test
  void aggregateSpecValidationEnforcesRules() throws Exception {
    Function validStringAgg = (Function) CCJSqlParserUtil.parseExpression("STRING_AGG(model, ',')");
    Object spec = aggregateSpec(validStringAgg, "agg_label");
    assertNotNull(spec, "Valid STRING_AGG should produce an AggregateSpec");

    Function missingSeparator = (Function) CCJSqlParserUtil.parseExpression("STRING_AGG(model)");
    assertThrows(IllegalArgumentException.class, () -> aggregateSpec(missingSeparator, "missing_sep"),
        "STRING_AGG requires two arguments");

    Function sumDistinct = (Function) CCJSqlParserUtil.parseExpression("SUM(DISTINCT hp)");
    assertThrows(IllegalArgumentException.class, () -> aggregateSpec(sumDistinct, "distinct_disallowed"),
        "Distinct aggregates should be rejected");

    Function emptySum = new Function();
    emptySum.setName("SUM");
    emptySum.setParameters(new ExpressionList<Expression>());
    assertThrows(IllegalArgumentException.class, () -> aggregateSpec(emptySum, "empty_sum"),
        "Aggregates require at least one argument");

    Function countStarWithArgs = new Function();
    countStarWithArgs.setName("COUNT");
    countStarWithArgs.setAllColumns(true);
    ExpressionList<Expression> countArgs = new ExpressionList<>();
    countArgs.addExpressions(CCJSqlParserUtil.parseExpression("1"));
    countStarWithArgs.setParameters(countArgs);
    AggregateFunctions.AggregateSpec countSpec = aggregateSpec(countStarWithArgs, "count_star_args");
    assertNotNull(countSpec);
    assertTrue(countSpec.countStar(), "COUNT(*) should be detected even if parameters are present");
    assertTrue(countSpec.arguments().isEmpty(), "COUNT(*) should not retain arguments");
  }
}
