package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.parser.Node;
import net.sf.jsqlparser.parser.Token;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/** Preserves SQL identifier boundaries when resolving relation qualifiers. */
public final class RelationQualifiers {
  /** Prevents utility class instantiation. */
  private RelationQualifiers() {
  }

  /**
   * Restores qualifier components from tokens before the parser's automatic
   * splitting of dots inside delimited identifiers can affect name resolution.
   *
   * @param column
   *          parsed column whose qualifier is normalized
   */
  public static void restore(Column column) {
    Node node = column.getASTNode();
    if (column.getTable() == null || node == null) {
      return;
    }
    List<String> names = new ArrayList<>();
    boolean identifier = true;
    for (Token token = node.jjtGetFirstToken(); token != null; token = token.next) {
      if (identifier) {
        names.add(token.image);
      } else if (!".".equals(token.image)) {
        return;
      }
      identifier = !identifier;
      if (token == node.jjtGetLastToken()) {
        if (!identifier && !names.isEmpty()) {
          column.setName(names.removeLast(), false);
          if (names.isEmpty()) {
            column.setTable(null);
          } else {
            Collections.reverse(names);
            Table table = new Table();
            table.setNameParts(names);
            column.setTable(table);
          }
        }
        return;
      }
    }
  }

  /**
   * Gets the entire qualifier, including any schema prefix.
   *
   * @param column
   *          column reference
   * @return qualifier with identifier quotes preserved, or null
   */
  public static String qualifier(Column column) {
    return column.getTable() == null ? null : column.getTable().getFullyQualifiedName();
  }

  /**
   * Compares each component using SQL identifier rules. A dot inside a quoted
   * identifier does not create a schema prefix.
   *
   * @param reference
   *          qualifier used by the query
   * @param declaration
   *          visible relation name
   * @return whether all components denote the declared qualifier
   */
  public static boolean matches(String reference, String declaration) {
    List<String> actual = parts(reference);
    List<String> expected = parts(declaration);
    if (actual.isEmpty() || actual.size() != expected.size()) {
      return false;
    }
    for (int i = 0; i < actual.size(); i++) {
      Identifier name = Identifier.of(actual.get(i));
      Identifier declared = Identifier.of(expected.get(i));
      if (name == null || declared == null || name.resolve(Map.of(declared, true)) == null) {
        return false;
      }
    }
    return true;
  }

  /**
   * Splits a qualifier only at dots outside delimited identifiers.
   *
   * @param name
   *          optional qualified name
   * @return ordered identifier components
   */
  private static List<String> parts(String name) {
    if (name == null || name.isBlank()) {
      return List.of();
    }
    List<String> result = new ArrayList<>();
    int start = 0;
    char closing = '\0';
    for (int i = 0; i < name.length(); i++) {
      char ch = name.charAt(i);
      if (closing != '\0') {
        if (ch == closing) {
          if (i + 1 < name.length() && name.charAt(i + 1) == closing) {
            i++;
          } else {
            closing = '\0';
          }
        }
      } else if (ch == '"' || ch == '`' || ch == '[') {
        closing = ch == '[' ? ']' : ch;
      } else if (ch == '.') {
        result.add(name.substring(start, i).trim());
        start = i + 1;
      }
    }
    result.add(name.substring(start).trim());
    return List.copyOf(result);
  }
}
