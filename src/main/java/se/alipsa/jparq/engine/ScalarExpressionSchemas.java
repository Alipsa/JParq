package se.alipsa.jparq.engine;

import java.sql.JDBCType;
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
import se.alipsa.jparq.helper.JdbcTypeMapper;
import se.alipsa.jparq.helper.TemporalValueType;

/**
 * Resolves scalar declarations whose evaluated values have the declared
 * representation.
 */
public final class ScalarExpressionSchemas {

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
    return switch (ExpressionEvaluator.unwrapParenthesis(expression)) {
      case NullValue value -> Schema.create(Schema.Type.NULL);
      case DateValue value -> declaredSchema("date");
      case TimeValue value -> declaredSchema("time");
      case TimestampValue value -> declaredSchema("timestamp");
      case StringValue value when "N".equalsIgnoreCase(value.getPrefix()) ->
        JdbcTypeMapper.declaredSchema(JDBCType.NVARCHAR);
      case StringValue value when value.getPrefix() == null -> Schema.create(Schema.Type.STRING);
      case CastExpression cast when cast.getColDataType() != null ->
        declaredSchema(cast.getColDataType().getDataType());
      case DateTimeLiteralExpression literal -> declaredSchema(literal.getType().name());
      case null, default -> null;
    };
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
