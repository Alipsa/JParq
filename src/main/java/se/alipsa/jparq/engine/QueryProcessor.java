package se.alipsa.jparq.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import net.sf.jsqlparser.expression.Expression;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;

public final class QueryProcessor implements AutoCloseable {

  private final ParquetReader<GenericRecord> reader;
  private final List<String> projection;
  private final Expression where;
  private final int limit;
  private final ExpressionEvaluator evaluator;

  private int emitted;

  public QueryProcessor(
      ParquetReader<GenericRecord> reader,
      List<String> projection,
      Expression where,
      int limit,
      Schema schema,
      int initialEmitted) { // NEW
    this.reader = Objects.requireNonNull(reader);
    this.projection = List.copyOf(projection);
    this.where = where;
    this.limit = limit;
    this.evaluator = new ExpressionEvaluator(schema);
    this.emitted = Math.max(0, initialEmitted); // NEW
  }

  public GenericRecord nextMatching() throws Exception {
    if (limit >= 0 && emitted >= limit) return null;
    GenericRecord rec = reader.read();
    while (rec != null) {
      if (where == null || evaluator.eval(where, rec)) {
        emitted++;
        return rec;
      }
      rec = reader.read();
    }
    return null;
  }

  List<String> projection() {
    return projection;
  }

  @Override
  public void close() throws Exception {
    reader.close();
  }

  /** Expand '*' against the first record’s schema if needed. */
  public static List<String> computeProjection(List<String> requested, Schema schema) {
    if (requested.isEmpty() || requested.contains("*")) {
      List<String> cols = new ArrayList<>();
      for (Schema.Field f : schema.getFields()) cols.add(f.name());
      return cols;
    }
    return requested;
  }
}
