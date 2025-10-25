package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import net.sf.jsqlparser.expression.Expression;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetReader;

/**
 * Processes a ParquetReader with projection, WHERE clause, LIMIT, and ORDER BY.
 */
public final class QueryProcessor implements AutoCloseable {

  private final ParquetReader<GenericRecord> reader;
  private final List<String> projection;
  private final Expression where;
  private final int limit;
  private final ExpressionEvaluator evaluator;

  // ORDER BY support
  private final List<SqlParser.OrderKey> orderBy; // empty => streaming path
  private List<GenericRecord> sorted; // buffer when ORDER BY is used
  private int idx = 0;

  private int emitted;

  /**
   * Streaming constructor (no ORDER BY).
   *
   * @param reader
   *          the Parquet reader
   * @param projection
   *          list of columns to project
   * @param where
   *          the WHERE expression (may be null)
   * @param limit
   *          the LIMIT (-1 = no limit)
   * @param schema
   *          the Avro schema (needed for expression evaluation)
   * @param initialEmitted
   *          number of rows already emitted (affects LIMIT)
   */
  public QueryProcessor(ParquetReader<GenericRecord> reader, List<String> projection, Expression where, int limit,
      Schema schema, int initialEmitted) {
    this(reader, projection, where, limit, schema, initialEmitted, List.of(), null);
  }

  /**
   * Generic constructor that can handle ORDER BY (buffer+sort) or streaming when
   * {@code orderBy} is empty.
   *
   * @param reader
   *          the Parquet reader
   * @param projection
   *          list of columns to project
   * @param where
   *          the WHERE expression (may be null)
   * @param limit
   *          the LIMIT (-1 = no limit)
   * @param schema
   *          the Avro schema (needed for expression evaluation and ORDER BY)
   * @param initialEmitted
   *          number of rows already emitted (affects LIMIT)
   * @param orderBy
   *          list of ORDER BY keys (empty = streaming path)
   * @param firstAlreadyRead
   *          a record already pulled by caller (may be null). It will be
   *          considered for buffering.
   */
  public QueryProcessor(ParquetReader<GenericRecord> reader, List<String> projection, Expression where, int limit,
      Schema schema, int initialEmitted, List<SqlParser.OrderKey> orderBy, GenericRecord firstAlreadyRead) {
    this.reader = Objects.requireNonNull(reader);
    this.projection = List.copyOf(projection);
    this.where = where;
    this.limit = limit;
    this.evaluator = (schema != null) ? new ExpressionEvaluator(schema) : null;
    this.emitted = Math.max(0, initialEmitted);
    this.orderBy = (orderBy == null) ? List.of() : List.copyOf(orderBy);

    if (!this.orderBy.isEmpty()) {
      bufferAndSort(firstAlreadyRead, schema);
    }
  }

  private void bufferAndSort(GenericRecord first, Schema schema) {
    try {
      List<GenericRecord> buf = new ArrayList<>();

      // include firstAlreadyRead if it matches WHERE
      if (first != null && matches(first)) {
        buf.add(first);
      }

      // read and filter all remaining
      GenericRecord rec = reader.read();
      while (rec != null) {
        if (matches(rec)) {
          buf.add(rec);
        }
        rec = reader.read();
      }

      // sort by ORDER BY keys (ASC default), simple NULL policy: ASC -> NULLS LAST,
      // DESC -> NULLS FIRST
      buf.sort(buildComparator(schema, orderBy));

      this.sorted = buf;
      this.idx = 0;
    } catch (Exception e) {
      throw new RuntimeException("ORDER BY buffer/sort failed", e);
    }
  }

  private static Comparator<GenericRecord> buildComparator(Schema schema, List<SqlParser.OrderKey> keys) {
    return (a, b) -> {
      for (SqlParser.OrderKey k : keys) {
        Schema.Field f = schema.getField(k.column());
        if (f == null) {
          throw new IllegalArgumentException("Unknown ORDER BY column: " + k.column());
        }

        Object va = AvroCoercions.unwrap(a.get(k.column()), f.schema());
        Object vb = AvroCoercions.unwrap(b.get(k.column()), f.schema());

        // NULLS LAST for ASC, NULLS FIRST for DESC
        if (va == null || vb == null) {
          int nullCmp = (va == null ? 1 : 0) - (vb == null ? 1 : 0); // null > non-null
          if (!k.asc()) {
            nullCmp = -nullCmp;
          }
          if (nullCmp != 0) {
            return nullCmp;
          }
          continue;
        }

        int c = ExpressionEvaluator.typedCompare(va, vb);
        if (c != 0) {
          return k.asc() ? c : -c;
        }
      }
      return 0;
    };
  }

  private boolean matches(GenericRecord rec) {
    return where == null || (evaluator != null && evaluator.eval(where, rec));
  }

  /**
   * Get the next record honoring WHERE/LIMIT (and ORDER BY if present).
   *
   * @return next matching record, or null if exhausted
   * @throws IOException
   *           on read error
   */
  public GenericRecord nextMatching() throws IOException {
    if (limit >= 0 && emitted >= limit) {
      return null;
    }

    // ORDER BY path: consume from sorted buffer
    if (!orderBy.isEmpty()) {
      if (sorted == null || idx >= sorted.size()) {
        return null;
      }
      GenericRecord r = sorted.get(idx++);
      emitted++;
      return r;
    }

    // Streaming path
    GenericRecord rec = reader.read();
    while (rec != null) {
      if (matches(rec)) {
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
  public void close() throws IOException {
    reader.close();
  }

  /**
   * Expand '*' against the first record’s schema if needed.
   *
   * @param requested
   *          requested columns (may include '*')
   * @param schema
   *          The Avro schema
   *
   * @return list of columns to project
   */
  public static List<String> computeProjection(List<String> requested, Schema schema) {
    if (requested.isEmpty() || requested.contains("*")) {
      List<String> cols = new ArrayList<>();
      for (Schema.Field f : schema.getFields()) {
        cols.add(f.name());
      }
      return cols;
    }
    return requested;
  }
}
