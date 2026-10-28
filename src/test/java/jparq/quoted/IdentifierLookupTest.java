package jparq.quoted;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.Identifier;

/** Verifies exact-name precedence and lowercase fallback for CTE lookup. */
class IdentifierLookupTest {

  /** Checks fallback in both directions without merging exact identifier keys. */
  @Test
  void resolvesLowercaseEquivalentsAfterExactNames() {
    var plain = Identifier.of("c");
    var quoted = Identifier.of("\"c\"");
    assertEquals("plain", quoted.resolve(Map.of(plain, "plain")));
    assertEquals("quoted", plain.resolve(Map.of(quoted, "quoted")));
    assertEquals("quoted", Identifier.of("C").resolve(Map.of(quoted, "quoted")));
    var both = Map.of(plain, "plain", quoted, "quoted");
    assertEquals("plain", plain.resolve(both));
    assertEquals("quoted", quoted.resolve(both));
    assertNull(Identifier.of("\"C\"").resolve(both));
    assertNull(plain.resolve(Map.of(Identifier.of("\"C\""), "upper")));
    assertNull(plain.resolve(Map.of()));
  }
}
