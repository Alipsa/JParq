package se.alipsa.jparq.model;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link MetaDataResultSet}. */
public class MetaDataResultSetTest {

  /**
   * Verifies that null rows are handled gracefully when accessed by index or
   * column label.
   */
  @Test
  public void testNullRowAccessIsSafe() {
    String[] headers = {
        "A", "B"
    };
    List<Object[]> rows = new ArrayList<>();
    rows.add(null);
    rows.add(new Object[]{
        "value", "other"
    });

    MetaDataResultSet resultSet = new MetaDataResultSet(headers, rows);

    Assertions.assertTrue(resultSet.next());
    Assertions.assertNull(resultSet.getString(1));
    Assertions.assertTrue(resultSet.wasNull());
    Assertions.assertNull(resultSet.getString("A"));
    Assertions.assertTrue(resultSet.wasNull());

    Assertions.assertTrue(resultSet.next());
    Assertions.assertEquals("value", resultSet.getString("A"));
    Assertions.assertFalse(resultSet.wasNull());
    Assertions.assertEquals("other", resultSet.getString(2));
    Assertions.assertFalse(resultSet.wasNull());
  }
}
