package jparq.qualified;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.QualifiedWildcard;
import se.alipsa.jparq.engine.SourcePosition;

/**
 * Unit tests for {@link QualifiedWildcard}.
 */
class QualifiedWildcardTest {

  @Test
  void constructorValidatesQualifier() {
    assertThrows(IllegalArgumentException.class, () -> new QualifiedWildcard(null),
        "Null qualifier should not be accepted");
    assertThrows(IllegalArgumentException.class, () -> new QualifiedWildcard("   "),
        "Blank qualifier should not be accepted");
  }

  @Test
  void trimsQualifierAndExposesFields() {
    SourcePosition position = new SourcePosition(2, 5);
    QualifiedWildcard wildcard = new QualifiedWildcard("  cars  ", position, 3);

    assertEquals("cars", wildcard.qualifier(), "Qualifier should be trimmed");
    assertEquals(Optional.of(position), wildcard.sourcePosition(), "Source position should be preserved");
    assertEquals(3, wildcard.selectListIndex(), "Select list index should be retained");
    assertEquals("cars.*", wildcard.toString(), "toString should render qualifier followed by .*");
  }

  @Test
  void defaultConstructorsPopulateOptionalFields() {
    QualifiedWildcard noPosition = new QualifiedWildcard("models");
    assertTrue(noPosition.sourcePosition().isEmpty(), "No source position should be present");
    assertEquals(-1, noPosition.selectListIndex(), "Unknown index should be represented as -1");

    SourcePosition position = new SourcePosition(1, 1);
    QualifiedWildcard withPosition = new QualifiedWildcard("models", position);
    assertEquals(Optional.of(position), withPosition.sourcePosition(),
        "Position-aware constructor should populate Optional");
    assertEquals(-1, withPosition.selectListIndex(), "Index defaults to -1 when omitted");
  }

  @Test
  void equalityConsidersQualifierPositionAndIndex() {
    SourcePosition posA = new SourcePosition(1, 2);

    QualifiedWildcard first = new QualifiedWildcard("t", posA, 0);

    QualifiedWildcard identical = new QualifiedWildcard("t", posA, 0);
    assertEquals(first, identical, "Identical qualifiers, positions, and indexes should be equal");
    assertEquals(first.hashCode(), identical.hashCode(), "Equal objects must share hash codes");

    QualifiedWildcard differentIndex = new QualifiedWildcard("t", posA, 1);
    assertNotEquals(first, differentIndex, "Different select list index should break equality");

    SourcePosition posB = new SourcePosition(2, 3);
    QualifiedWildcard differentPosition = new QualifiedWildcard("t", posB, 0);
    assertNotEquals(first, differentPosition, "Different source position should break equality");

    QualifiedWildcard differentQualifier = new QualifiedWildcard("other", posA, 0);
    assertNotEquals(first, differentQualifier, "Different qualifier should break equality");
    assertNotEquals(null, first, "QualifiedWildcard should not equal null");
  }
}
