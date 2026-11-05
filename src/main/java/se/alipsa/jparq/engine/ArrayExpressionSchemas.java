package se.alipsa.jparq.engine;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.List;
import net.sf.jsqlparser.expression.ArrayConstructor;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import se.alipsa.jparq.NumericType;
import se.alipsa.jparq.engine.function.DateTimeFunctions;
import se.alipsa.jparq.engine.function.StructArrayTypes;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.TemporalValueType;

/** Resolves array declarations without using the current outer row's values. */
public final class ArrayExpressionSchemas {
  /** Prevents utility class construction. */
  private ArrayExpressionSchemas() {
  }

  /**
   * Validates ARRAY declarations in a projection without evaluating its values.
   * Nested query scopes validate their own projections during planning.
   *
   * @param expression
   *          projected expression
   * @param columns
   *          source column schema resolver
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 for incompatible element declarations
   */
  public static void validate(Expression expression, java.util.function.Function<Column, Schema> columns) {
    expression.accept(new ExpressionVisitorAdapter<Void>() {
      @Override
      public <S> Void visit(ArrayConstructor array, S context) {
        resolve(array, columns);
        return super.visit(array, context);
      }

      @Override
      public <S> Void visit(Function function, S context) {
        if ("ARRAY".equalsIgnoreCase(function.getName())) {
          resolve(function, columns);
        }
        return super.visit(function, context);
      }

      @Override
      public <S> Void visit(net.sf.jsqlparser.statement.select.Select select, S context) {
        return null;
      }

      @Override
      public <S> Void visit(net.sf.jsqlparser.statement.select.ParenthesedSelect select, S context) {
        return null;
      }
    }, null);
  }

  /**
   * Resolves typed bindings, array constructors, and common expression branches.
   * Unknown expressions retain the caller's value-based inference.
   *
   * @param expression
   *          expression whose declaration is needed
   * @return declared schema, or null when unknown
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 if known array element types are
   *           incompatible
   */
  public static Schema resolve(Expression expression) {
    return resolve(expression, column -> null);
  }

  /**
   * Resolves declarations using the input columns visible to the expression.
   *
   * @param expression
   *          expression whose declaration is needed
   * @param columns
   *          source column schema resolver
   * @return declared schema, or null when unknown
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 if known array element types are
   *           incompatible
   */
  public static Schema resolve(Expression expression, java.util.function.Function<Column, Schema> columns) {
    if (expression instanceof Column column) {
      return JdbcTypeMapper.nonNullSchema(columns.apply(column));
    }
    if (expression instanceof Function function && CorrelatedBindings.matches(function)) {
      return JdbcTypeMapper.nonNullSchema(CorrelatedBindings.schema(function));
    }
    if (expression instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      return resolve(list.getFirst(), columns);
    }
    if (expression instanceof net.sf.jsqlparser.statement.select.ParenthesedSelect select) {
      return resolve(select.getSelect(), columns);
    }
    if (expression instanceof net.sf.jsqlparser.statement.select.PlainSelect select
        && select.getSelectItems().size() == 1) {
      return resolve(select.getSelectItems().getFirst().getExpression(), columns);
    }
    if (expression instanceof ArrayConstructor array) {
      Schema element = common(array.getExpressions(), columns);
      return element == null ? null : Schema.createArray(element);
    }
    if (expression instanceof Function function && function.getParameters() != null) {
      if ("ARRAY".equalsIgnoreCase(function.getName())) {
        Schema element = common(function.getParameters(), columns);
        return element == null ? null : Schema.createArray(element);
      }
      if ("NULLIF".equalsIgnoreCase(function.getName()) && !function.getParameters().isEmpty()) {
        return resolve(function.getParameters().getFirst(), columns);
      }
      if ("COALESCE".equalsIgnoreCase(function.getName())) {
        return common(function.getParameters(), columns);
      }
    }
    if (expression instanceof CaseExpression choice) {
      return common(ExpressionBranches.caseResults(choice), columns);
    }
    if (expression instanceof BinaryExpression binary && NumericExpressionTypes.operation(expression) != null) {
      NumericType left = numeric(resolve(binary.getLeftExpression(), columns));
      NumericType right = numeric(resolve(binary.getRightExpression(), columns));
      return left == null || right == null
          ? null
          : left.arithmetic(right, NumericExpressionTypes.operation(expression)).schema();
    }
    if (expression instanceof SignedExpression signed) {
      return resolve(signed.getExpression(), columns);
    }
    if (expression instanceof CastExpression cast && cast.getColDataType() != null) {
      Schema declared = nonnumericSchema(cast.getColDataType().getDataType());
      if (declared != null) {
        return declared;
      }
    }
    if (expression instanceof DateTimeLiteralExpression literal) {
      return nonnumericSchema(literal.getType().name());
    }
    if (expression instanceof TimeKeyExpression time) {
      return switch (time.getStringValue().toUpperCase(java.util.Locale.ROOT)) {
        case "CURRENT_DATE" -> nonnumericSchema("DATE");
        case "LOCALTIME" -> nonnumericSchema("TIME");
        case "LOCALTIMESTAMP" -> nonnumericSchema("TIMESTAMP");
        case "CURRENT_TIME" -> TemporalValueType.OFFSET_TIME.schema();
        case "CURRENT_TIMESTAMP" -> TemporalValueType.OFFSET_TIMESTAMP.schema();
        default -> null;
      };
    }
    NumericType numeric = NumericExpressionTypes.resolve(expression, column -> numeric(columns.apply(column)));
    if (numeric != null) {
      return numeric.schema();
    }
    return expression instanceof StringValue ? Schema.create(Schema.Type.STRING) : null;
  }

  /**
   * Maps nonnumeric SQL declarations to their decoded value schemas.
   *
   * @param typeName
   *          SQL type name, including optional precision and time-zone clauses
   * @return declared schema, or null for numeric or unrecognized declarations
   */
  private static Schema nonnumericSchema(String typeName) {
    String normalized = DateTimeFunctions.normalizeTypeName(typeName.replaceAll("\\([^)]*\\)", ""));
    return switch (normalized) {
      case "date" -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
      case "time", "timewithouttimezone" -> LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
      case "timestamp", "timestampwithouttimezone", "datetime" -> TemporalValueType.UTC_TIMESTAMP.schema();
      case "timewithtimezone" -> TemporalValueType.OFFSET_TIME.schema();
      case "timestampwithtimezone" -> TemporalValueType.OFFSET_TIMESTAMP.schema();
      case "boolean", "bool" -> Schema.create(Schema.Type.BOOLEAN);
      case "char", "character" -> JdbcTypeMapper.declaredSchema(JDBCType.CHAR);
      case "nchar", "nationalcharacter", "nationalchar" -> JdbcTypeMapper.declaredSchema(JDBCType.NCHAR);
      case "nvarchar", "nationalcharactervarying" -> JdbcTypeMapper.declaredSchema(JDBCType.NVARCHAR);
      case "varchar", "charactervarying", "string", "text", "clob" -> Schema.create(Schema.Type.STRING);
      case "binary", "blob" -> JdbcTypeMapper.declaredSchema(JDBCType.BINARY);
      case "varbinary", "binaryvarying" -> JdbcTypeMapper.declaredSchema(JDBCType.VARBINARY);
      default -> null;
    };
  }

  /**
   * Describes a known numeric declaration without consulting projection metadata.
   *
   * @param schema
   *          declared operand schema, or null
   * @return its numeric descriptor, or null for unknown or nonnumeric operands
   */
  private static NumericType numeric(Schema schema) {
    return schema == null ? null : NumericType.of(JdbcTypeMapper.mapSchemaToJdbcType(schema), schema);
  }

  /**
   * Combines known branch declarations, retaining array and logical types.
   *
   * @param expressions
   *          candidate values or branches
   * @param columns
   *          source column schema resolver
   * @return common known schema, or null when no declaration is available
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 if known array element types are
   *           incompatible
   */
  private static Schema common(List<? extends Expression> expressions,
      java.util.function.Function<Column, Schema> columns) {
    Schema result = null;
    for (Expression expression : expressions) {
      result = common(result, resolve(expression, columns));
    }
    return result;
  }

  /**
   * Identifies character declarations that can share an ARRAY element type.
   *
   * @param type
   *          JDBC type
   * @return whether it is fixed or varying character data
   */
  private static boolean isCharacter(int type) {
    return type == Types.CHAR || type == Types.VARCHAR || type == Types.NCHAR || type == Types.NVARCHAR;
  }

  /**
   * Promotes numeric elements and recursively combines ARRAY and STRUCT
   * declarations.
   *
   * @param left
   *          first declaration, possibly null
   * @param right
   *          second declaration, possibly null
   * @return the common declaration
   * @throws IllegalArgumentException
   *           carrying SQLState 42000 if known array element types are
   *           incompatible
   */
  private static Schema common(Schema left, Schema right) {
    if (left == null || right == null || left.equals(right)) {
      return left == null ? right : left;
    }
    if (left.getType() == Schema.Type.RECORD && right.getType() == Schema.Type.RECORD) {
      return StructArrayTypes.common(left, right);
    }
    if (left.getType() == Schema.Type.ARRAY && right.getType() == Schema.Type.ARRAY) {
      return Schema.createArray(common(left.getElementType(), right.getElementType()));
    }
    if (left.getType() == Schema.Type.MAP && right.getType() == Schema.Type.MAP) {
      return Schema.createMap(common(left.getValueType(), right.getValueType()));
    }
    int leftType = JdbcTypeMapper.mapSchemaToJdbcType(left);
    int rightType = JdbcTypeMapper.mapSchemaToJdbcType(right);
    if (isCharacter(leftType) && isCharacter(rightType)) {
      boolean national = leftType == Types.NCHAR || leftType == Types.NVARCHAR || rightType == Types.NCHAR
          || rightType == Types.NVARCHAR;
      boolean varying = leftType == Types.VARCHAR || leftType == Types.NVARCHAR || rightType == Types.VARCHAR
          || rightType == Types.NVARCHAR;
      return JdbcTypeMapper.declaredSchema(
          national ? varying ? JDBCType.NVARCHAR : JDBCType.NCHAR : varying ? JDBCType.VARCHAR : JDBCType.CHAR);
    }
    if ((leftType == Types.BINARY || leftType == Types.VARBINARY)
        && (rightType == Types.BINARY || rightType == Types.VARBINARY)) {
      return JdbcTypeMapper.declaredSchema(
          leftType == Types.VARBINARY || rightType == Types.VARBINARY ? JDBCType.VARBINARY : JDBCType.BINARY);
    }
    if ((leftType == Types.TIME || leftType == Types.TIME_WITH_TIMEZONE)
        && (rightType == Types.TIME || rightType == Types.TIME_WITH_TIMEZONE) && leftType != rightType) {
      return TemporalValueType.OFFSET_TIME.schema();
    }
    if ((leftType == Types.TIMESTAMP || leftType == Types.TIMESTAMP_WITH_TIMEZONE)
        && (rightType == Types.TIMESTAMP || rightType == Types.TIMESTAMP_WITH_TIMEZONE) && leftType != rightType) {
      return TemporalValueType.OFFSET_TIMESTAMP.schema();
    }
    NumericType first = NumericType.of(leftType, left);
    NumericType second = NumericType.of(rightType, right);
    if (first != null && second != null) {
      return first.common(second).schema();
    }
    if (leftType == rightType && leftType != Types.OTHER && leftType != Types.STRUCT) {
      return StructArrayTypes.common(left, right);
    }
    throw SqlExceptions.incompatibleArrayElements(JDBCType.valueOf(JdbcTypeMapper.mapSchemaToJdbcType(left)).getName(),
        JDBCType.valueOf(JdbcTypeMapper.mapSchemaToJdbcType(right)).getName());
  }
}
