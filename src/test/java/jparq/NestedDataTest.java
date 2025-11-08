package jparq;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.alipsa.jparq.JParqSql;

public class NestedDataTest {
  private static final Logger LOG = LoggerFactory.getLogger(NestedDataTest.class);
  private static JParqSql jparqSql;

  @BeforeAll
  static void setup() throws URISyntaxException {
    URL acmesUrl = NestedDataTest.class.getResource("/acme");
    Assertions.assertNotNull(acmesUrl, "acme must be on the test classpath (src/test/resources)");
    Path acmePath = Paths.get(acmesUrl.toURI());
    jparqSql = new JParqSql("jdbc:jparq:" + acmePath.toAbsolutePath());
  }

  /**
   * Ensure explicit {@code CROSS JOIN} syntax produces the Cartesian product of
   * the joined tables.
   */
  @Test
  void testSimpleArrayFlattening() {
    String sql = """
        SELECT
            p.name,
            t.tag_name
        FROM
            products_nested p,
            UNNEST(p.tags) AS t(tag_name)
        ORDER BY
            p.id, t.tag_name
        """;

    // Expect that each product name will be repeated for every tag it has.
    LOG.info("NOT IMPLEMENTED: testSimpleArrayFlattening");
  }

  @Test
  void testSingleRecord() {
    String sql = """
        SELECT
            name,
            details.manufacturer,
            details.warranty_years
        FROM
            products_nested
        WHERE
            details.warranty_years >= 2;
        """;

    // Expect Laptop Pro and Monitor Ultra rows.
    LOG.info("NOT IMPLEMENTED: testSingleRecord");
  }

  @Test
  void testFlatteningArrayOfRecords() {
    String sql = """
        SELECT
            p.name,
            r.rating,
            r.user
        FROM
            products_nested p,
            UNNEST(p.reviews) AS r(rating, user) -- UNNEST flattens the ROW ARRAY
        WHERE
            r.rating = 5
        ORDER BY
            p.id;
        """;

    // Expected Output: Only rows for reviews with a 5-star rating, showing the
    // product name and the user who left the review.
    LOG.info("NOT IMPLEMENTED: testFlatteningArrayOfRecords");
  }
}
