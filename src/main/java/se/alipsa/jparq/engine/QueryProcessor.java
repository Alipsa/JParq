package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.Expression;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;

public final class QueryProcessor implements AutoCloseable {

  private final ParquetReader<GenericRecord> reader;

  // Read from record using source names; expose labels to JDBC
  private List<String> sourceProjection; // may temporarily contain a single "\*" sentinel
  private List<String> outputLabels;

  private final Expression where;
  private final int limit;
  private ExpressionEvaluator evaluator; // built lazily if schema was absent
  private Schema schema; // may be null initially

  private final List<SqlParser.OrderKey> orderBy; // empty => no sort
  private final boolean distinct;

  // Buffered path (ORDER BY or DISTINCT)
  private List<Object[]> bufferedRows;
  private int rowIdx = 0;

  // Streaming path
  private int emitted;
  private Object[] firstRowStreaming;

  /** Streaming constructor (no ORDER BY, no DISTINCT). */
  public QueryProcessor(ParquetReader<GenericRecord> reader, List<String> sourceProjection, List<String> outputLabels,
      Expression where, int limit, Schema schema, int initialEmitted) {
    this(reader, sourceProjection, outputLabels, where, limit, schema, initialEmitted, List.of(), false, null);
  }

  /** General constructor. */
  public QueryProcessor(ParquetReader<GenericRecord> reader, List<String> sourceProjection, List<String> outputLabels,
      Expression where, int limit, Schema schema, int initialEmitted, List<SqlParser.OrderKey> orderBy,
      boolean distinct, GenericRecord firstAlreadyRead) {
    this.reader = Objects.requireNonNull(reader, "reader");
    this.where = where;
    this.limit = limit;
    this.emitted = Math.max(0, initialEmitted);
    this.orderBy = (orderBy == null) ? List.of() : List.copyOf(orderBy);
    this.distinct = distinct;

    // Adopt schema if provided or from the first record
    this.schema = (schema != null) ? schema : (firstAlreadyRead != null ? firstAlreadyRead.getSchema() : null);
    this.evaluator = (this.schema != null) ? new ExpressionEvaluator(this.schema) : null;

    // Initialize projections; keep "\*" as sentinel if we cannot expand yet
    this.sourceProjection = new ArrayList<>(computeProjection(sourceProjection, this.schema));
    if (outputLabels == null || outputLabels.isEmpty()) {
      this.outputLabels = new ArrayList<>(this.sourceProjection);
    } else {
      this.outputLabels = new ArrayList<>(outputLabels);
    }

    // If we now have a schema, expand any "\*" in both lists coherently
    expandSelectAllIfPossible();

    // Decide path
    if (this.distinct || !this.orderBy.isEmpty()) {
      bufferMaterialize(firstAlreadyRead);
    } else if (firstAlreadyRead != null) {
      if (matches(firstAlreadyRead)) {
        this.firstRowStreaming = project(firstAlreadyRead);
        this.emitted = 0;
      } else {
        this.firstRowStreaming = null;
        this.emitted = 0;
      }
    }
  }

  /** Returns the effective output labels (column names visible to JDBC). */
  public List<String> projection() {
    return outputLabels;
  }

  public Object[] nextRow() throws IOException {
    // Buffered path
    if (bufferedRows != null) {
      if (rowIdx >= bufferedRows.size()) {
        return null;
      }
      return bufferedRows.get(rowIdx++);
    }

    // Streaming path
    if (limit >= 0 && emitted >= limit) {
      return null;
    }

    if (firstRowStreaming != null) {
      Object[] row = firstRowStreaming;
      firstRowStreaming = null;
      emitted++;
      return row;
    }

    GenericRecord rec = reader.read();
    while (rec != null) {
      if (matches(rec)) {
        if (limit >= 0 && emitted >= limit) {
          return null;
        }
        emitted++;
        return project(rec);
      }
      rec = reader.read();
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  /**
   * Expand '\*' against the schema if needed; if schema is null, keep a single
   * '\*' sentinel.
   */
  public static List<String> computeProjection(List<String> requested, Schema schema) {
    if (requested == null || requested.isEmpty()) {
      return List.of("*");
    }
    if (requested.size() == 1 && "*".equals(requested.get(0))) {
      if (schema == null) {
        return List.of("*");
      }
      List<String> cols = new ArrayList<>();
      for (Schema.Field f : schema.getFields()) {
        cols.add(f.name());
      }
      return cols;
    }
    return requested;
  }

  // --- Internal helpers ---

  private static boolean isSelectAll(List<String> cols) {
    return cols.size() == 1 && "*".equals(cols.get(0));
  }

  // Expand source and labels independently when schema exists
  private void expandSelectAllIfPossible() {
    if (this.schema == null) {
      return;
    }
    if (isSelectAll(this.sourceProjection)) {
      List<String> expanded = this.schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
      this.sourceProjection = new ArrayList<>(expanded);
    }
    if (isSelectAll(this.outputLabels)) {
      List<String> expandedLabels = this.schema.getFields().stream().map(Schema.Field::name)
          .collect(Collectors.toList());
      this.outputLabels = new ArrayList<>(expandedLabels);
    }
  }

  private void ensureEvaluator(GenericRecord rec) {
    if (evaluator == null && where != null) {
      Schema rs = rec.getSchema();
      if (rs != null) {
        this.schema = rs;
        this.evaluator = new ExpressionEvaluator(rs);
        // Schema just arrived: try to expand '\*'
        expandSelectAllIfPossible();
      }
    }
  }

  private boolean matches(GenericRecord rec) {
    if (where == null) {
      return true;
    }
    ensureEvaluator(rec);
    return evaluator != null && evaluator.eval(where, rec);
  }

  private Object[] project(GenericRecord rec) {
    // If we finally have a schema, ensure '\*' is expanded before projecting
    if (this.schema == null) {
      this.schema = rec.getSchema();
      expandSelectAllIfPossible();
    }
    Schema recSchema = (this.schema != null) ? this.schema : rec.getSchema();
    Object[] row = new Object[sourceProjection.size()];
    for (int i = 0; i < sourceProjection.size(); i++) {
      String sourceCol = sourceProjection.get(i);
      Schema.Field f = (recSchema != null) ? recSchema.getField(sourceCol) : null;
      Object v = rec.get(sourceCol);
      row[i] = (f == null) ? v : AvroCoercions.unwrap(v, f.schema());
    }
    return row;
  }

  private void bufferMaterialize(GenericRecord first) {
    try {
      List<Object[]> rows = new ArrayList<>();

      if (first != null && matches(first)) {
        rows.add(project(first));
      }

      GenericRecord rec = reader.read();
      while (rec != null) {
        if (matches(rec)) {
          rows.add(project(rec));
        }
        rec = reader.read();
      }

      // DISTINCT before ORDER BY and LIMIT
      if (distinct) {
        rows = dedupeRows(rows); // returns mutable ArrayList
      }

      if (!orderBy.isEmpty() && !rows.isEmpty()) {
        rows.sort(buildRowComparator());
      }

      if (limit >= 0 && rows.size() > limit) {
        rows = new ArrayList<>(rows.subList(0, limit));
      }

      this.bufferedRows = rows;
      this.rowIdx = 0;
    } catch (Exception e) {
      throw new RuntimeException("Materialization failed", e);
    }
  }

  private List<Object[]> dedupeRows(List<Object[]> rows) {
    Map<List<Object>, Object[]> seen = new LinkedHashMap<>();
    for (Object[] r : rows) {
      List<Object> key = Arrays.stream(r).map(QueryProcessor::normalizeForDistinct).collect(Collectors.toList());
      seen.putIfAbsent(key, r);
    }
    return new ArrayList<>(seen.values());
  }

  private static Object normalizeForDistinct(Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof byte[] ba) {
      return java.nio.ByteBuffer.wrap(Arrays.copyOf(ba, ba.length)).asReadOnlyBuffer();
    }
    if (v instanceof java.nio.ByteBuffer bb) {
      java.nio.ByteBuffer ro = bb.asReadOnlyBuffer();
      ro.rewind();
      byte[] copy = new byte[ro.remaining()];
      ro.get(copy);
      return java.nio.ByteBuffer.wrap(copy).asReadOnlyBuffer();
    }
    if (v instanceof java.math.BigDecimal bd) {
      return bd.stripTrailingZeros();
    }
    if (v instanceof Double d) {
      if (Double.isNaN(d)) {
        return Double.NaN;
      }
      if (d.doubleValue() == 0d) {
        return 0d;
      }
      return d;
    }
    if (v instanceof Float f) {
      if (Float.isNaN(f)) {
        return Float.NaN;
      }
      if (f.floatValue() == 0f) {
        return 0f;
      }
      return f;
    }
    return v;
  }

  private Comparator<Object[]> buildRowComparator() {
    // Map ORDER BY column to its index, prefer label match, then source match
    int[] idx = new int[orderBy.size()];
    boolean[] asc = new boolean[orderBy.size()];
    for (int i = 0; i < orderBy.size(); i++) {
      SqlParser.OrderKey k = orderBy.get(i);
      String col = k.column();
      int p = outputLabels.indexOf(col);
      if (p < 0) {
        p = sourceProjection.indexOf(col);
      }
      if (p < 0) {
        throw new IllegalArgumentException("ORDER BY column not found in output or source projection: " + col);
      }
      idx[i] = p;
      asc[i] = k.asc();
    }

    return (a, b) -> {
      for (int i = 0; i < idx.length; i++) {
        Object va = a[idx[i]];
        Object vb = b[idx[i]];

        int nc = nullCompare(va, vb, asc[i]);
        if (nc != 0) {
          return nc;
        }
        if (va != null && vb != null) {
          int c = ExpressionEvaluator.typedCompare(va, vb);
          if (c != 0) {
            return asc[i] ? c : -c;
          }
        }
      }
      return 0;
    };
  }

  private static int nullCompare(Object a, Object b, boolean asc) {
    if (a == null && b == null) {
      return 0;
    }
    if (a == null) {
      return asc ? 1 : -1; // NULLS LAST for ASC, FIRST for DESC
    }
    if (b == null) {
      return asc ? -1 : 1;
    }
    return 0;
  }
}
