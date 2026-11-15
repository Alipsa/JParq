package jparq.engine;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.alipsa.jparq.JParqPreparedStatement;
import se.alipsa.jparq.NumericType;
import se.alipsa.jparq.engine.NumericExpressionTypes;

/** Verifies numeric schema conversion and branch type combination. */
class NumericExpressionTypesTest {
  /**
   * Known numeric set branches do not depend on optional value-schema lookups.
   *
   * @param directory
   *          empty database directory
   * @throws Exception
   *           if statement preparation or declaration lookup fails
   */
  @Test
  void numericSetBranchesSkipValueSchemas(@TempDir Path directory) throws Exception {
    var numeric = new NumericType(Types.DECIMAL, 8, 3);
    try (var connection = DriverManager.getConnection("jdbc:jparq:" + directory);
        var statement = (JParqPreparedStatement) connection.prepareStatement("SELECT 1 UNION ALL SELECT 1.0")) {
      try (var branches = mockConstruction(JParqPreparedStatement.class, (branch, context) -> {
        when(branch.firstProjectionNumericType()).thenReturn(numeric);
        when(branch.firstProjectionSchema()).thenThrow(new SQLException("value schema is unavailable"));
      })) {
        assertEquals(numeric, statement.firstProjectionNumericType());
        assertEquals(2, branches.constructed().size());
        for (var branch : branches.constructed()) {
          verify(branch, never()).firstProjectionSchema();
          verify(branch, never()).executeQuery();
        }
      }
    }
  }

  /** Recognizes nullable numeric schemas and retains decimal bounds. */
  @Test
  void nullableNumericSchemas() {
    Schema decimal = LogicalTypes.decimal(8, 3).addToSchema(Schema.create(Schema.Type.BYTES));
    assertEquals(new NumericType(Types.DECIMAL, 8, 3), NumericExpressionTypes.numeric(decimal));
    Schema nullable = Schema.createUnion(Schema.create(Schema.Type.NULL), decimal);
    assertEquals(new NumericType(Types.DECIMAL, 8, 3), NumericExpressionTypes.numeric(nullable));
    assertEquals(NumericType.of(Types.INTEGER, 0, 0), NumericExpressionTypes.numeric(Schema.create(Schema.Type.INT)));
    assertNull(NumericExpressionTypes.numeric(null));
    assertNull(NumericExpressionTypes.numeric(Schema.create(Schema.Type.NULL)));
    assertNull(NumericExpressionTypes.numeric(Schema.create(Schema.Type.STRING)));
  }

  /**
   * Retains unbounded decimals in either order while honoring approximate
   * precedence.
   */
  @Test
  void unboundedCommonBranches() {
    var unbounded = new NumericType(Types.DECIMAL, 0, 0);
    var bounded = new NumericType(Types.DECIMAL, 8, 3);
    var integer = NumericType.of(Types.INTEGER, 0, 0);
    var approximate = NumericType.of(Types.DOUBLE, 0, 0);
    for (NumericType type : List.of(unbounded, bounded, integer)) {
      assertEquals(unbounded, NumericExpressionTypes.common(unbounded, type));
      assertEquals(unbounded, NumericExpressionTypes.common(type, unbounded));
    }
    assertEquals(approximate, NumericExpressionTypes.common(unbounded, approximate));
    assertEquals(approximate, NumericExpressionTypes.common(approximate, unbounded));
    assertEquals(new NumericType(Types.DECIMAL, 13, 3), NumericExpressionTypes.common(integer, bounded));
    assertEquals(bounded, NumericExpressionTypes.common(null, bounded));
    assertEquals(bounded, NumericExpressionTypes.common(bounded, null));
    assertNull(NumericExpressionTypes.common(null, null));
  }
}
