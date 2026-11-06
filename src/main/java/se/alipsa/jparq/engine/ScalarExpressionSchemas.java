package se.alipsa.jparq.engine;

import java.sql.JDBCType;
import java.sql.Types;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import se.alipsa.jparq.engine.function.DateTimeFunctions;
import se.alipsa.jparq.helper.IntervalType;
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.TemporalValueType;

/**
 * Resolves scalar declarations whose evaluated values have the declared
 * representation.
 */
public final class ScalarExpressionSchemas {

  private static final Set<String> CHARACTER_ONLY_FUNCTIONS = Set.of("LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH",
      "UPPER", "UCASE", "LOWER", "LCASE", "CONCAT");

  /** Prevents utility class construction. */
  private ScalarExpressionSchemas() {
  }

  /**
   * Resolves nonnumeric literals and CAST declarations without evaluating rows.
   * Unsupported scalar declarations remain unknown rather than applying ARRAY
   * element validation or advertising an unimplemented value representation.
   *
   * @param expression
   *          scalar projection
   * @return declared schema, or null when unresolved
   */
  public static Schema resolve(Expression expression) {
    return resolve(expression, name -> false);
  }

  /**
   * Resolves scalar declarations with source names for CONVERT argument ordering.
   *
   * @param expression
   *          projected expression
   * @param columns
   *          identifies visible source column names
   * @return declared schema, or null when unresolved
   */
  public static Schema resolve(Expression expression, Predicate<String> columns) {
    CastExpression cast = CastExpressions.asCast(expression, columns);
    if (cast != null && cast.getColDataType() != null) {
      return declaredSchema(cast.getColDataType().getDataType());
    }
    return switch (ExpressionEvaluator.unwrapParenthesis(expression)) {
      case NullValue value -> Schema.create(Schema.Type.NULL);
      case DateValue value -> declaredSchema("date");
      case TimeValue value -> declaredSchema("time");
      case TimestampValue value -> declaredSchema("timestamp");
      case StringValue value when "N".equalsIgnoreCase(value.getPrefix()) ->
        JdbcTypeMapper.declaredSchema(JDBCType.NVARCHAR);
      case StringValue value when value.getPrefix() == null -> Schema.create(Schema.Type.STRING);
      case DateTimeLiteralExpression literal -> declaredSchema(literal.getType().name());
      case null, default -> null;
    };
  }

  /**
   * Validates a binary length independently of NULL values and empty inputs. LOB
   * lengths support K/M/G/T/P multipliers, each a power of 1024.
   *
   * @param cast
   *          SQL CAST declaration
   * @return explicit binary length, or null when absent or not binary
   * @throws IllegalArgumentException
   *           if a length is invalid, carrying SQLState 42000
   */
  public static Long binaryLength(CastExpression cast) {
    if (cast == null || cast.getColDataType() == null) {
      return null;
    }
    JDBCType target = declaredJdbcType(cast.getColDataType().getDataType());
    if (target == null || !JdbcTypeMapper.isBinaryType(target.getVendorTypeNumber())) {
      return null;
    }
    List<String> arguments = cast.getColDataType().getArgumentsStringList();
    if (arguments == null || arguments.isEmpty()) {
      String name = cast.getColDataType().getDataType();
      int start = name.indexOf('(');
      int end = name.indexOf(')', start + 1);
      if (start < 0 || end < 0) {
        return null;
      }
      arguments = List.of(name.substring(start + 1, end).split(",", -1));
    }
    try {
      String argument = arguments.getFirst().trim().toUpperCase(Locale.ROOT);
      int exponent = target == JDBCType.BLOB && !argument.isEmpty()
          ? "KMGTP".indexOf(argument.charAt(argument.length() - 1)) + 1
          : 0;
      String digits = exponent == 0 ? argument : argument.substring(0, argument.length() - 1);
      long length = Math.multiplyExact(Long.parseLong(digits), 1L << (exponent * 10));
      if (arguments.size() == 1 && length > 0 && (target == JDBCType.BLOB || length <= Integer.MAX_VALUE)) {
        return length;
      }
    } catch (ArithmeticException | NumberFormatException ignored) {
      // Report malformed or overflowing lengths consistently as SQL syntax errors.
    }
    throw SqlExceptions.syntaxFailure("Invalid binary CAST length: " + cast.getColDataType());
  }

  /**
   * Rejects known noncharacter, nonbinary operands before evaluating rows.
   * Character-to-binary conversion is supported using UTF-8.
   *
   * @param cast
   *          SQL CAST declaration
   * @param sources
   *          scoped operand schema resolver
   * @throws IllegalArgumentException
   *           if the source or length is invalid, carrying SQLState 42000
   */
  public static void validateBinaryCast(CastExpression cast, Function<Expression, Schema> sources) {
    binaryLength(cast);
    if (cast.getColDataType() == null) {
      return;
    }
    JDBCType target = declaredJdbcType(cast.getColDataType().getDataType());
    if (target == null || !JdbcTypeMapper.isBinaryType(target.getVendorTypeNumber())) {
      return;
    }
    Expression operand = ExpressionEvaluator.unwrapParenthesis(cast.getLeftExpression());
    CastExpression inner = CastExpressions.asCast(operand);
    if (inner != null && inner.getColDataType() != null) {
      JDBCType source = declaredJdbcType(inner.getColDataType().getDataType());
      if (source == JDBCType.CLOB || source == JDBCType.NCLOB) {
        return;
      }
    }
    Schema source = JdbcTypeMapper.nonNullSchema(sources.apply(operand));
    int type = JdbcTypeMapper.mapSchemaToJdbcType(source);
    if (IntervalType.matches(source) || type != Types.NULL && type != Types.OTHER
        && !JdbcTypeMapper.isCharacterType(type) && !JdbcTypeMapper.isBinaryType(type)) {
      throw SqlExceptions.syntaxFailure("Binary CAST requires a character or binary operand");
    }
  }

  /**
   * Rejects binary operands of functions whose signatures require characters.
   *
   * @param name
   *          function name
   * @param arguments
   *          function arguments, possibly null
   * @param sources
   *          scoped argument schema resolver
   * @throws IllegalArgumentException
   *           if an argument is binary, carrying SQLState 42000
   */
  public static void validateCharacterFunction(String name, List<? extends Expression> arguments,
      Function<Expression, Schema> sources) {
    if (name == null || arguments == null || !CHARACTER_ONLY_FUNCTIONS.contains(name.toUpperCase(Locale.ROOT))) {
      return;
    }
    for (Expression argument : arguments) {
      Schema schema = sources.apply(argument);
      if (JdbcTypeMapper.isBinaryType(JdbcTypeMapper.mapSchemaToJdbcType(schema))) {
        throw SqlExceptions.syntaxFailure(name + " requires character arguments, not binary values");
      }
    }
  }

  /**
   * Rejects a known binary/nonbinary comparison independently of returned rows.
   *
   * @param left
   *          left operand
   * @param right
   *          right operand
   * @param sources
   *          scoped operand schema resolver
   * @throws IllegalArgumentException
   *           if known operand types are incompatible, carrying SQLState 42000
   */
  public static void validateBinaryComparison(Expression left, Expression right, Function<Expression, Schema> sources) {
    validateBinaryComparison(sources.apply(left), sources.apply(right));
  }

  /**
   * Compares declarations already resolved in their lexical scopes.
   *
   * @param first
   *          left operand schema, possibly unknown
   * @param second
   *          right operand schema, possibly unknown
   * @throws IllegalArgumentException
   *           if known binary and nonbinary operands are mixed, carrying SQLState
   *           42000
   */
  public static void validateBinaryComparison(Schema first, Schema second) {
    int leftType = JdbcTypeMapper.mapSchemaToJdbcType(first);
    int rightType = JdbcTypeMapper.mapSchemaToJdbcType(second);
    boolean leftBinary = JdbcTypeMapper.isBinaryType(leftType);
    boolean rightBinary = JdbcTypeMapper.isBinaryType(rightType);
    boolean leftKnown = leftType != Types.NULL && (leftType != Types.OTHER || IntervalType.matches(first));
    boolean rightKnown = rightType != Types.NULL && (rightType != Types.OTHER || IntervalType.matches(second));
    if (leftBinary && !rightBinary && rightKnown || rightBinary && !leftBinary && leftKnown) {
      throw SqlExceptions.syntaxFailure("Cannot compare binary and nonbinary operands");
    }
  }

  /**
   * Maps implemented nonnumeric scalar type declarations to value schemas.
   * Numeric declarations use the numeric expression resolver. Character LOB and
   * time-zone CAST declarations remain unresolved until their scalar evaluation
   * supplies the corresponding value representations.
   *
   * @param typeName
   *          SQL type name, including optional precision
   * @return value schema, or null for unresolved declarations
   */
  public static Schema declaredSchema(String typeName) {
    if (typeName == null) {
      return null;
    }
    String normalized = normalizeTypeName(typeName);
    return switch (normalized) {
      case "date" -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
      case "time", "timewithouttimezone" -> LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
      case "timestamp", "timestampwithouttimezone", "datetime" -> TemporalValueType.UTC_TIMESTAMP.schema();
      case "boolean", "bool" -> Schema.create(Schema.Type.BOOLEAN);
      default -> {
        JDBCType type = declaredJdbcType(typeName);
        yield type == null || type == JDBCType.CLOB || type == JDBCType.NCLOB
            ? null
            : JdbcTypeMapper.declaredSchema(type);
      }
    };
  }

  /**
   * Normalizes SQL type names while retaining clauses following precision.
   *
   * @param typeName
   *          SQL type name, possibly including precision
   * @return normalized name, or an empty string for null
   */
  public static String normalizeTypeName(String typeName) {
    return typeName == null ? "" : DateTimeFunctions.normalizeTypeName(typeName.replaceAll("\\([^)]*\\)", ""));
  }

  /**
   * Identifies character, binary, and LOB declarations for shared validation.
   *
   * @param typeName
   *          SQL type name, possibly including precision
   * @return declared JDBC type, or null for other declarations
   */
  public static JDBCType declaredJdbcType(String typeName) {
    return switch (normalizeTypeName(typeName)) {
      case "char", "character" -> JDBCType.CHAR;
      case "nchar", "nationalcharacter", "nationalchar" -> JDBCType.NCHAR;
      case "nvarchar", "nationalcharactervarying" -> JDBCType.NVARCHAR;
      case "varchar", "charactervarying", "string", "text" -> JDBCType.VARCHAR;
      case "clob", "characterlargeobject" -> JDBCType.CLOB;
      case "nclob", "nationalcharacterlargeobject" -> JDBCType.NCLOB;
      case "blob", "binarylargeobject" -> JDBCType.BLOB;
      case "binary" -> JDBCType.BINARY;
      case "varbinary", "binaryvarying" -> JDBCType.VARBINARY;
      default -> null;
    };
  }

}
