package jparq.functions.strfunc;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import se.alipsa.jparq.JParqSql;

/** Utility factory for creating {@link JParqSql} instances in tests. */
final class StringFunctionTestSupport {

  private StringFunctionTestSupport() {
  }

  static JParqSql createSql() {
    try {
      URL mtcarsUrl = StringFunctionTestSupport.class.getResource("/mtcars.parquet");
      if (mtcarsUrl == null) {
        throw new IllegalStateException("mtcars.parquet must be on the test classpath");
      }
      Path mtcarsPath = Paths.get(mtcarsUrl.toURI());
      Path dir = mtcarsPath.getParent();
      return new JParqSql("jdbc:jparq:" + dir.toAbsolutePath());
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Failed to locate mtcars dataset", e);
    }
  }
}
