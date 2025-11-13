package se.alipsa.jparq.engine;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ParquetSchemas} utility methods that inspect Parquet logical
 * types.
 */
class ParquetSchemasTest {

  /**
   * Ensures that binary primitives annotated with the Parquet string logical
   * type are recognised as textual fields.
   */
  @Test
  void stringLogicalTypeAnnotationMarksBinaryFieldsAsText() {
    Type binaryString = Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
        .named("text");
    assertTrue(ParquetSchemas.isStringAnnotatedBinary(binaryString),
        "String logical type annotation should mark the field as text");
  }

  /**
   * Verifies that binary primitives without logical annotations are not treated
   * as textual data.
   */
  @Test
  void binaryFieldWithoutStringAnnotationIsNotReportedAsText() {
    Type binary = Types.required(PrimitiveTypeName.BINARY).named("payload");
    assertFalse(ParquetSchemas.isStringAnnotatedBinary(binary),
        "Binary fields without logical type annotations should remain binary");
  }

  /**
   * Confirms that non-binary primitives are never reported as string annotated
   * fields.
   */
  @Test
  void nonBinaryPrimitiveIsNotReportedAsText() {
    Type integer = Types.required(PrimitiveTypeName.INT32).named("id");
    assertFalse(ParquetSchemas.isStringAnnotatedBinary(integer),
        "Only binary primitives should be considered for string logical types");
  }
}
