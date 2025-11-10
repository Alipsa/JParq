package jparq.group;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import se.alipsa.jparq.JParqSql;

/**
 * Utility for computing horsepower aggregates from the mtcars dataset used in grouping tests.
 */
final class MtcarsHpSums {

  private MtcarsHpSums() {
  }

  /**
   * Calculate horsepower sums grouped by cylinder and gear.
   *
   * @param jparqSql
   *          SQL helper connected to the directory containing mtcars.parquet
   * @return aggregated horsepower values structured for assertions
   */
  static Aggregates compute(JParqSql jparqSql) {
    Map<Integer, Map<Integer, Double>> detail = new HashMap<>();
    Map<Integer, Double> cylTotals = new HashMap<>();
    double[] total = new double[]{
        0.0
    };
    jparqSql.query("SELECT cyl, gear, hp FROM mtcars", rs -> {
      try {
        while (rs.next()) {
          Integer cyl = (Integer) rs.getObject("cyl");
          Integer gear = (Integer) rs.getObject("gear");
          double hpValue = rs.getDouble("hp");
          if (rs.wasNull() || cyl == null || gear == null) {
            continue;
          }
          detail.computeIfAbsent(cyl, key -> new HashMap<>()).merge(gear, hpValue, Double::sum);
          cylTotals.merge(cyl, hpValue, Double::sum);
          total[0] += hpValue;
        }
      } catch (SQLException e) {
        fail(e);
      }
    });
    return new Aggregates(detail, cylTotals, total[0]);
  }

  /**
   * Aggregated horsepower information sourced from the mtcars dataset.
   *
   * @param detailSums
   *          horsepower totals for each cylinder and gear combination
   * @param cylinderTotals
   *          horsepower totals for each cylinder
   * @param grandTotal
   *          horsepower total across the entire dataset
   */
  record Aggregates(Map<Integer, Map<Integer, Double>> detailSums, Map<Integer, Double> cylinderTotals,
      double grandTotal) {
  }
}
