package jparq.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import se.alipsa.jparq.engine.NumericCastTarget;
import se.alipsa.jparq.engine.NumericExpressionTypes;
import se.alipsa.jparq.engine.function.DateTimeFunctions;

/** Verifies numeric aliases keep evaluation and type inference aligned. */
class NumericCastTargetTest {
  /**
   * Resolves aliases to the expected JDBC type and Java number class.
   *
   * @param name
   *          the SQL target declaration
   * @param jdbcType
   *          the expected JDBC type code
   * @param javaClass
   *          the expected Java class name
   * @throws Exception
   *           if parsing fails
   */
  @ParameterizedTest
  @CsvSource({
      "TINYINT,-6,Byte", "SMALLINT,5,Short", "INT2,5,Short", "INT,4,Integer", "INTEGER,4,Integer", "SIGNED,4,Integer",
      "INT4,4,Integer", "BIGINT,-5,Long", "INT8,-5,Long", "FLOAT,7,Float", "REAL,7,Float", "FLOAT4,7,Float",
      "DOUBLE,8,Double", "DOUBLE PRECISION,8,Double", "FLOAT8,8,Double", "FLOAT(53),7,Float", "INT(11),4,Integer"
  })
  void aliasesShareEvaluationAndMetadata(String name, int jdbcType, String javaClass) throws Exception {
    var cast = (CastExpression) CCJSqlParserUtil.parseExpression("CAST(1 AS " + name + ")");
    var target = NumericCastTarget.fromName(cast.getColDataType().getDataType());
    assertNotNull(target);
    assertEquals(jdbcType, target.numericType().jdbcType());
    assertEquals(target.numericType(), NumericExpressionTypes.resolve(cast, column -> null));
    assertEquals(javaClass, DateTimeFunctions.castLiteral(cast, 1).getClass().getSimpleName());
  }

  /** Prevents strict lookups from stripping characters from column names. */
  @Test
  void normalizedLookupRequiresAnExactAlias() {
    assertEquals(NumericCastTarget.BIGINT, NumericCastTarget.fromNormalizedName("bigint"));
    assertNull(NumericCastTarget.fromNormalizedName("big_int"));
    assertNull(NumericCastTarget.fromNormalizedName("double precision"));
    assertNull(NumericCastTarget.fromNormalizedName("FLOAT"));
    assertEquals(NumericCastTarget.DOUBLE, NumericCastTarget.fromName("DOUBLE PRECISION"));
  }

  /** Leaves nonnumeric and unsupported names outside the shared mapping. */
  @Test
  void unrelatedNamesRemainUnrecognized() {
    assertNull(NumericCastTarget.fromName(null));
    assertNull(NumericCastTarget.fromName("VARCHAR"));
    assertNull(NumericCastTarget.fromName("DECIMAL(5,0)"));
    assertNull(NumericCastTarget.fromName("NOT_A_TYPE"));
  }
}
