package jparq.alias;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;
import se.alipsa.jparq.engine.SqlParser;

/** Tests ensuring projection aliases remain accessible for derived tables. */
class SqlParserProjectionAliasTest {

  @Test
  void aggregatedExpressionAliasBecomesPhysicalColumn() {
    SqlParser.Select select = SqlParser.parseSelect("""
        SELECT e.first_name, e.last_name, s.salary
        FROM employees e
        JOIN (
          SELECT employee, MAX(change_date) AS max_cd
          FROM salary
          GROUP BY employee
        ) m ON m.employee = e.id
        JOIN salary s
          ON s.employee = m.employee
         AND s.change_date = m.max_cd
        """);

    SqlParser.TableReference derived = select.tableReferences().stream().filter(ref -> "m".equals(ref.tableAlias()))
        .findFirst().orElseThrow(() -> new IllegalStateException("Derived table alias m should exist"));

    List<String> physical = derived.subquery().columnNames();

    assertNotNull(physical, "Derived table should expose physical column names");
    assertEquals(2, physical.size(), "Derived table should project two columns");
    assertFalse(physical.stream().anyMatch(Objects::isNull), "Physical column names must not contain null");
    assertEquals("max_cd", physical.get(1), "Aggregate alias should be preserved as physical column name");
  }
}
