package se.alipsa.jparq.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import net.sf.jsqlparser.expression.Expression;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Processes a {@link RecordReader} with projection, WHERE clause, LIMIT, and
 * ORDER BY.
 */
public final class QueryProcessor implements AutoCloseable {

  private final RecordReader reader;
  private final List<String> projection;
  private final Expression where;
  private final int limit;
  private final boolean distinct;
  private final boolean distinctBeforePreLimit;
  private final SubqueryExecutor subqueryExecutor;
  private final int preLimit;
  private final int offset;
  private final int preOffset;
  private final List<SqlParser.OrderKey> preOrderBy;
  private final boolean hasPreStage;
  private final List<String> distinctColumns;
  private final List<String> preStageDistinctColumns;
  private final List<String> outerQualifiers;
  private final Map<String, Map<String, String>> qualifierColumnMapping;
  private final Map<String, String> unqualifiedColumnMapping;

  // Evaluator is lazily created if schema was not provided
  private ExpressionEvaluator evaluator;

  // ORDER BY support
  private final List<SqlParser.OrderKey> orderBy; // empty => streaming path
  private List<GenericRecord> sorted; // buffer when ORDER BY is used
  private int idx = 0;

  private int emitted;
  private int skipped;
  private int preOffsetApplied;
  private GenericRecord prefetched;
  private final Set<List<Object>> distinctSeen;

  /**
   * Builder-style options for configuring {@link QueryProcessor} instances.
   */
  public static final class Options {

    private Schema schema;
    private int initialEmitted;
    private boolean distinct;
    private boolean distinctBeforePreLimit;
    private List<SqlParser.OrderKey> orderBy = List.of();
    private GenericRecord firstAlreadyRead;
    private SubqueryExecutor subqueryExecutor;
    private int preLimit = -1;
    private int offset;
    private int preOffset;
    private List<SqlParser.OrderKey> preOrderBy = List.of();
    private List<String> distinctColumns;
    private List<String> preStageDistinctColumns = List.of();
    private List<String> outerQualifiers = List.of();
    private Map<String, Map<String, String>> qualifierColumnMapping = Map.of();
    private Map<String, String> unqualifiedColumnMapping = Map.of();

    private Options() {
      // use factory
    }

    /**
     * Create a new mutable options instance.
     *
     * @return a fresh {@link Options} instance
     */
    public static Options builder() {
      return new Options();
    }

    /**
     * Set the Avro schema used when evaluating expressions.
     *
     * @param schema
     *          schema for evaluation (may be {@code null})
     * @return {@code this} for chaining
     */
    public Options schema(Schema schema) {
      this.schema = schema;
      return this;
    }

    /**
     * Record how many rows were already emitted before the processor was created.
     *
     * @param initialEmitted
     *          number of rows already emitted
     * @return {@code this} for chaining
     */
    public Options initialEmitted(int initialEmitted) {
      this.initialEmitted = initialEmitted;
      return this;
    }

    /**
     * Enable or disable DISTINCT processing.
     *
     * @param distinct
     *          whether DISTINCT should be applied
     * @return {@code this} for chaining
     */
    public Options distinct(boolean distinct) {
      this.distinct = distinct;
      return this;
    }

    /**
     * Control whether DISTINCT must be applied before pre-stage LIMIT handling.
     *
     * @param distinctBeforePreLimit
     *          {@code true} if DISTINCT originates from an inner SELECT that should
     *          be enforced prior to any pre-stage LIMIT
     * @return {@code this} for chaining
     */
    public Options distinctBeforePreLimit(boolean distinctBeforePreLimit) {
      this.distinctBeforePreLimit = distinctBeforePreLimit;
      return this;
    }

    /**
     * Provide ORDER BY keys for the outer query.
     *
     * @param orderBy
     *          ORDER BY keys; {@code null} means none
     * @return {@code this} for chaining
     */
    public Options orderBy(List<SqlParser.OrderKey> orderBy) {
      this.orderBy = (orderBy == null || orderBy.isEmpty()) ? List.of() : List.copyOf(orderBy);
      return this;
    }

    /**
     * Provide a record that has already been read prior to constructing the
     * processor.
     *
     * @param firstAlreadyRead
     *          the pre-fetched record (may be {@code null})
     * @return {@code this} for chaining
     */
    public Options firstAlreadyRead(GenericRecord firstAlreadyRead) {
      this.firstAlreadyRead = firstAlreadyRead;
      return this;
    }

    /**
     * Set the executor used for correlated subqueries.
     *
     * @param subqueryExecutor
     *          executor implementation (may be {@code null})
     * @return {@code this} for chaining
     */
    public Options subqueryExecutor(SubqueryExecutor subqueryExecutor) {
      this.subqueryExecutor = subqueryExecutor;
      return this;
    }

    /**
     * Apply a LIMIT that should be enforced before the outer projection.
     *
     * @param preLimit
     *          limit applied during pre-stage execution (-1 for none)
     * @return {@code this} for chaining
     */
    public Options preLimit(int preLimit) {
      this.preLimit = preLimit;
      return this;
    }

    /**
     * Apply an OFFSET that should be honored after filtering.
     *
     * @param offset
     *          number of rows to skip; negative values are treated as zero
     * @return {@code this} for chaining
     */
    public Options offset(int offset) {
      this.offset = Math.max(0, offset);
      return this;
    }

    /**
     * Apply an OFFSET that must be enforced prior to any pre-stage processing.
     *
     * @param preOffset
     *          number of rows to skip before applying pre-stage ORDER BY or LIMIT
     *          logic
     * @return {@code this} for chaining
     */
    public Options preOffset(int preOffset) {
      this.preOffset = Math.max(0, preOffset);
      return this;
    }

    /**
     * Apply ORDER BY keys that must run before the outer projection.
     *
     * @param preOrderBy
     *          ORDER BY keys for the pre-stage; {@code null} means none
     * @return {@code this} for chaining
     */
    public Options preOrderBy(List<SqlParser.OrderKey> preOrderBy) {
      this.preOrderBy = (preOrderBy == null || preOrderBy.isEmpty()) ? List.of() : List.copyOf(preOrderBy);
      return this;
    }

    /**
     * Specify the columns that should be used to compute DISTINCT keys.
     *
     * @param distinctColumns
     *          ordered list of column names that define the DISTINCT key
     * @return {@code this} for chaining
     */
    public Options distinctColumns(List<String> distinctColumns) {
      boolean hasDistinctColumns = distinctColumns != null && !distinctColumns.isEmpty();
      this.distinctColumns = hasDistinctColumns ? List.copyOf(distinctColumns) : null;
      return this;
    }

    /**
     * Specify the columns that define DISTINCT semantics prior to any pre-stage
     * LIMIT.
     *
     * @param preStageDistinctColumns
     *          ordered list of column names that must be considered when DISTINCT
     *          originates from an inner SELECT
     * @return {@code this} for chaining
     */
    public Options preStageDistinctColumns(List<String> preStageDistinctColumns) {
      boolean hasColumns = preStageDistinctColumns != null && !preStageDistinctColumns.isEmpty();
      this.preStageDistinctColumns = hasColumns ? List.copyOf(preStageDistinctColumns) : List.of();
      return this;
    }

    /**
     * Provide table names or aliases that should be visible to correlated sub
     * queries evaluated by the processor.
     *
     * @param qualifiers
     *          table names or aliases from the outer query scope
     * @return {@code this} for chaining
     */
    public Options outerQualifiers(List<String> qualifiers) {
      this.outerQualifiers = (qualifiers == null || qualifiers.isEmpty()) ? List.of() : List.copyOf(qualifiers);
      return this;
    }

    /**
     * Provide a mapping between table qualifiers and canonical column names.
     *
     * @param mapping
     *          qualifier to column mapping (may be {@code null}); entries are
     *          normalized using {@link ColumnMappingUtil#normaliseQualifierMapping(Map)}
     * @return {@code this} for chaining
     */
    public Options qualifierColumnMapping(Map<String, Map<String, String>> mapping) {
      this.qualifierColumnMapping = ColumnMappingUtil.normaliseQualifierMapping(mapping);
      return this;
    }

    /**
     * Provide canonical names for unqualified columns that remain unique after a
     * join.
     *
     * @param mapping
     *          mapping from column name to canonical field name (may be
     *          {@code null}); entries are normalized using
     *          {@link ColumnMappingUtil#normaliseUnqualifiedMapping(Map)}
     * @return {@code this} for chaining
     */
    public Options unqualifiedColumnMapping(Map<String, String> mapping) {
      this.unqualifiedColumnMapping = ColumnMappingUtil.normaliseUnqualifiedMapping(mapping);
      return this;
    }
  }

  /**
   * Construct a processor that can stream results or buffer+sort depending on the
   * supplied {@link Options}.
   *
   * @param reader
   *          the reader providing {@link GenericRecord} instances
   * @param projection
   *          list of columns to project
   * @param where
   *          the WHERE expression (may be null)
   * @param limit
   *          the LIMIT (-1 = no limit)
   * @param options
   *          configuration for DISTINCT, ORDER BY and other behaviour
   */
  public QueryProcessor(RecordReader reader, List<String> projection, Expression where, int limit, Options options) {
    this.reader = Objects.requireNonNull(reader);
    this.projection = Collections.unmodifiableList(new ArrayList<>(projection));
    this.where = where;
    this.limit = limit;
    Options opts = Objects.requireNonNull(options, "options");
    this.distinct = opts.distinct;
    this.distinctBeforePreLimit = opts.distinctBeforePreLimit;
    this.subqueryExecutor = opts.subqueryExecutor;
    this.preLimit = opts.preLimit;
    this.offset = Math.max(0, opts.offset);
    this.preOffset = Math.max(0, opts.preOffset);
    this.outerQualifiers = opts.outerQualifiers;
    this.qualifierColumnMapping = opts.qualifierColumnMapping;
    this.unqualifiedColumnMapping = opts.unqualifiedColumnMapping;
    this.preOrderBy = canonicalizeOrderKeys(opts.preOrderBy);
    this.hasPreStage = (this.preLimit >= 0) || !this.preOrderBy.isEmpty() || (this.preOffset > 0);
    List<String> distinctCols = opts.distinctColumns == null ? this.projection : opts.distinctColumns;
    this.distinctColumns = distinctCols;
    this.preStageDistinctColumns = (opts.preStageDistinctColumns == null || opts.preStageDistinctColumns.isEmpty())
        ? distinctCols
        : opts.preStageDistinctColumns;
    this.orderBy = canonicalizeOrderKeys(opts.orderBy);
    this.evaluator = (opts.schema != null)
        ? new ExpressionEvaluator(opts.schema, subqueryExecutor, outerQualifiers, qualifierColumnMapping,
            unqualifiedColumnMapping)
        : null;
    this.emitted = Math.max(0, opts.initialEmitted);
    this.skipped = Math.min(this.offset, Math.max(0, opts.initialEmitted));
    this.prefetched = (opts.initialEmitted > 0 && this.offset == 0) ? null : opts.firstAlreadyRead;
    this.preOffsetApplied = 0;
    this.distinctSeen = distinct ? new LinkedHashSet<>() : null;

    GenericRecord firstAlreadyRead = opts.firstAlreadyRead;

    if (distinct && firstAlreadyRead != null && opts.initialEmitted > 0 && this.orderBy.isEmpty() && !hasPreStage) {
      registerDistinct(firstAlreadyRead);
    }

    if (!this.orderBy.isEmpty() || hasPreStage) {
      bufferAndSort(firstAlreadyRead, opts.schema);
      this.prefetched = null;
    }
  }

  private void bufferAndSort(GenericRecord first, Schema schemaHint) {
    try {
      List<GenericRecord> buf = new ArrayList<>();
      Set<List<Object>> preStageDistinct = distinctBeforePreLimit ? new LinkedHashSet<>() : null;
      Schema sortSchema = schemaHint;

      // include firstAlreadyRead if it matches WHERE
      if (first != null) {
        if (sortSchema == null) {
          sortSchema = first.getSchema();
        }
        if (matches(first)) {
          buf.add(first);
        }
      }

      // read remaining, detect schema if still unknown
      GenericRecord rec = reader.read();
      if (sortSchema == null && rec != null) {
        sortSchema = rec.getSchema();
      }
      while (rec != null) {
        if (matches(rec)) {
          if (preStageDistinct != null) {
            List<Object> key = distinctKey(rec, preStageDistinctColumns);
            if (preStageDistinct.add(key)) {
              buf.add(rec);
            }
            if (preLimit >= 0 && preOrderBy.isEmpty() && preStageDistinct.size() >= preLimit) {
              break;
            }
          } else {
            buf.add(rec);
            if (preLimit >= 0 && preOrderBy.isEmpty() && buf.size() >= preLimit) {
              break;
            }
          }
        }
        rec = reader.read();
      }

      // ensure evaluator exists if WHERE is used
      ensureEvaluator(sortSchema);

      boolean distinctApplied = false;
      if (hasPreStage) {
        if (distinctBeforePreLimit && distinct && !buf.isEmpty()) {
          buf = applyDistinct(buf);
          distinctApplied = true;
        }
        if (!preOrderBy.isEmpty() && sortSchema != null && buf.size() > 1) {
          buf.sort(buildComparator(sortSchema, preOrderBy));
        }
        if (preOffset > 0 && !buf.isEmpty()) {
          int before = buf.size();
          if (preOffset >= before) {
            buf = new ArrayList<>();
          } else {
            buf = new ArrayList<>(buf.subList(preOffset, before));
          }
          preOffsetApplied = Math.min(preOffset, before);
        } else {
          preOffsetApplied = 0;
        }
        if (preLimit >= 0 && buf.size() > preLimit) {
          buf = new ArrayList<>(buf.subList(0, preLimit));
        }
      }

      if (distinct && !buf.isEmpty() && !distinctApplied) {
        buf = applyDistinct(buf);
      }

      // sort by ORDER BY keys if we have schema and >1 row
      if (!orderBy.isEmpty() && buf.size() > 1 && sortSchema != null) {
        buf.sort(buildComparator(sortSchema, orderBy));
      }

      this.sorted = buf;
      int effectiveOffset = Math.max(0, offset - preOffsetApplied);
      if (effectiveOffset >= sorted.size()) {
        this.idx = sorted.size();
        this.skipped = offset;
      } else {
        this.idx = effectiveOffset;
        this.skipped = offset;
      }
    } catch (IOException e) {
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

  private List<SqlParser.OrderKey> canonicalizeOrderKeys(List<SqlParser.OrderKey> keys) {
    if (keys == null || keys.isEmpty()) {
      return List.of();
    }
    List<SqlParser.OrderKey> canonical = new ArrayList<>(keys.size());
    for (SqlParser.OrderKey key : keys) {
      if (key == null) {
        continue;
      }
      String column = key.column();
      String resolved = ColumnMappingUtil.canonicalOrderColumn(column, key.qualifier(), qualifierColumnMapping,
          unqualifiedColumnMapping);
      canonical.add(new SqlParser.OrderKey(resolved, key.asc(), key.qualifier()));
    }
    return List.copyOf(canonical);
  }

  private void ensureEvaluator(GenericRecord rec) {
    if (where != null && evaluator == null && rec != null) {
      evaluator = new ExpressionEvaluator(rec.getSchema(), subqueryExecutor, outerQualifiers, qualifierColumnMapping,
          unqualifiedColumnMapping);
    }
  }

  private void ensureEvaluator(Schema schema) {
    if (where != null && evaluator == null && schema != null) {
      evaluator = new ExpressionEvaluator(schema, subqueryExecutor, outerQualifiers, qualifierColumnMapping,
          unqualifiedColumnMapping);
    }
    // If still null, it will be lazily created from the first record seen.
  }

  private boolean matches(GenericRecord rec) {
    if (where == null) {
      return true;
    }
    ensureEvaluator(rec);
    return evaluator != null && evaluator.eval(where, rec);
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
    if (!orderBy.isEmpty() || hasPreStage) {
      if (sorted == null || idx >= sorted.size()) {
        return null;
      }
      GenericRecord r = sorted.get(idx++);
      emitted++;
      return r;
    }

    // Streaming path
    GenericRecord rec = nextRawRecord();
    while (rec != null) {
      if (matches(rec)) {
        if (distinct && !registerDistinct(rec)) {
          rec = nextRawRecord();
          continue;
        }
        if (skipped < offset) {
          skipped++;
          rec = nextRawRecord();
          continue;
        }
        emitted++;
        return rec;
      }
      rec = nextRawRecord();
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

  private boolean registerDistinct(GenericRecord rec) {
    if (!distinct) {
      return true;
    }
    List<Object> key = distinctKey(rec, distinctColumns);
    return distinctSeen.add(key);
  }

  private GenericRecord nextRawRecord() throws IOException {
    if (prefetched != null) {
      GenericRecord next = prefetched;
      prefetched = null;
      return next;
    }
    return reader.read();
  }

  private List<GenericRecord> applyDistinct(List<GenericRecord> records) {
    if (!distinct || records.isEmpty()) {
      return records;
    }
    List<GenericRecord> unique = new ArrayList<>(records.size());
    Set<List<Object>> seen = new LinkedHashSet<>();
    for (GenericRecord rec : records) {
      List<Object> key = distinctKey(rec, distinctColumns);
      if (seen.add(key)) {
        unique.add(rec);
      }
    }
    if (distinctSeen != null) {
      distinctSeen.addAll(seen);
    }
    return unique;
  }

  private List<Object> distinctKey(GenericRecord rec, List<String> columns) {
    List<Object> key = new ArrayList<>(columns.size());
    for (String col : columns) {
      if (col == null) {
        throw new IllegalArgumentException("DISTINCT on expressions is not supported");
      }
      Schema.Field field = rec.getSchema().getField(col);
      if (field == null) {
        throw new IllegalArgumentException("Unknown column for DISTINCT: " + col);
      }
      Object value = AvroCoercions.unwrap(rec.get(col), field.schema());
      key.add(value);
    }
    return key;
  }
}
