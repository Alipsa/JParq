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

  /** Rebuilds identifiers from lookup keys, restoring quoting semantics. */
  @Test
  void rebuildsIdentifiersFromLookupKeys() {
    assertEquals(Identifier.of("name"), Identifier.fromLookupKey("U:name"));
    assertEquals(Identifier.of("Name"), Identifier.fromLookupKey("U:Name"));
    assertEquals(Identifier.of("\"Name\""), Identifier.fromLookupKey("Q:Name"));
    assertEquals(Identifier.of("\"a\"\"b\""), Identifier.fromLookupKey("Q:a\"b"));
    assertNull(Identifier.fromLookupKey(null));
    assertNull(Identifier.fromLookupKey("U:"));
    assertNull(Identifier.fromLookupKey("name"));
  }

  /** Lookup keys rebuilt from their own form round-trip unchanged. */
  @Test
  void lookupKeyRoundTripIsStable() {
    for (Identifier identifier : new Identifier[]{
        Identifier.of("t2"), Identifier.of("\"T2\""), Identifier.of("\"a\"\"b\"")
    }) {
      assertEquals(identifier, Identifier.fromLookupKey(identifier.lookupKey()));
      assertEquals(identifier.lookupKey(), Identifier.fromLookupKey(identifier.lookupKey()).lookupKey());
    }
  }
}
