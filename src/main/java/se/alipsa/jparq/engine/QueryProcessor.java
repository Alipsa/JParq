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
import java.util.function.Function;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import se.alipsa.jparq.engine.window.WindowFunctions;
import se.alipsa.jparq.engine.window.WindowPlan;
import se.alipsa.jparq.engine.window.WindowState;
import se.alipsa.jparq.helper.LiteralConverter;

/**
 * Processes a {@link RecordReader} with projection, WHERE clause, LIMIT, and
 * ORDER BY.
 */
public final class QueryProcessor implements AutoCloseable {

  private final RecordReader reader;
  // DISTINCT key fallback, used only when no schema is known
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
  private final List<Expression> distinctExpressions;
  private final boolean[] mixedDistinctTypes;
  private final List<String> preStageDistinctColumns;
  private final List<String> outerQualifiers;
  private final Map<String, Map<String, String>> qualifierColumnMapping;
  private final Map<String, String> unqualifiedColumnMapping;
  private final WindowPlan windowPlan;
  private WindowState windowState;
  private final boolean windowed;

  // Evaluator is lazily created if schema was not provided
  private ExpressionEvaluator evaluator;

  // ORDER BY support
  private final List<SqlParser.OrderKey> orderBy; // empty => streaming path
  private final Map<String, Expression> orderByExpressions;
  private List<GenericRecord> sorted; // buffer when ORDER BY is used
  private int idx = 0;

  private int emitted;
  private int skipped;
  private int preOffsetApplied;
  private GenericRecord prefetched;
  private final Set<List<Object>> distinctSeen;
  private ValueExpressionEvaluator valueEvaluator;

  /**
   * Builder-style options for configuring {@link QueryProcessor} instances.
   */
  public static final class Options {

    private Schema schema;
    private int initialEmitted;
    private boolean distinct;
    private boolean distinctBeforePreLimit;
    private List<SqlParser.OrderKey> orderBy = List.of();
    private Map<String, Expression> orderByExpressions = Map.of();
    private GenericRecord firstAlreadyRead;
    private SubqueryExecutor subqueryExecutor;
    private int preLimit = -1;
    private int offset;
    private int preOffset;
    private List<SqlParser.OrderKey> preOrderBy = List.of();
    private List<String> distinctColumns;
    private List<Expression> distinctExpressions = List.of();
    private List<String> preStageDistinctColumns = List.of();
    private List<String> outerQualifiers = List.of();
    private Map<String, Map<String, String>> qualifierColumnMapping = Map.of();
    private Map<String, String> unqualifiedColumnMapping = Map.of();
    private WindowPlan windowPlan;

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
     * Record how many rows were already emitted before streaming processing.
     * Buffering ignores this hint because it consumes the supplied first row.
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
     * Provide expressions corresponding to projection labels referenced from ORDER
     * BY clauses.
     *
     * @param orderByExpressions
     *          mapping of projection label to expression; {@code null} or empty
     *          indicates no computed expressions are required
     * @return {@code this} for chaining
     */
    public Options orderByExpressions(Map<String, Expression> orderByExpressions) {
      if (orderByExpressions == null || orderByExpressions.isEmpty()) {
        this.orderByExpressions = Map.of();
      } else {
        this.orderByExpressions = Map.copyOf(orderByExpressions);
      }
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
     *          normalized using
     *          {@link ColumnMappingUtil#normaliseQualifierMapping(Map)}
     * @return {@code this} for chaining
     */
    public Options qualifierColumnMapping(Map<String, Map<String, String>> mapping) {
      this.qualifierColumnMapping = ColumnMappingUtil.normaliseQualifierMapping(mapping);
      return this;
    }

    /**
     * Provides the SELECT expressions in projection order for DISTINCT keys. Empty
     * expressions retain physical-column key evaluation.
     *
     * @param expressions
     *          projection expressions, or {@code null}
     * @return this options instance
     */
    public Options distinctExpressions(List<Expression> expressions) {
      this.distinctExpressions = expressions == null ? List.of() : List.copyOf(expressions);
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

    /**
     * Provide a plan describing analytic window functions that require
     * materialization.
     *
     * @param windowPlan
     *          plan describing analytic window functions, {@code null} when none
     *          are present
     * @return {@code this} for chaining
     */
    public Options windowPlan(WindowPlan windowPlan) {
      this.windowPlan = windowPlan;
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
   *          columns used as the DISTINCT key fallback when no schema is known
   * @param where
   *          the WHERE expression (may be null)
   * @param limit
   *          the LIMIT (-1 = no limit)
   * @param options
   *          configuration for DISTINCT, ORDER BY and other behaviour
   */
  public QueryProcessor(RecordReader reader, List<String> projection, Expression where, int limit, Options options) {
    this.reader = Objects.requireNonNull(reader);
    this.projection = Collections.unmodifiableList(new ArrayList<>(projection == null ? List.of() : projection));
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
    this.windowPlan = opts.windowPlan;
    this.windowed = windowPlan != null && !windowPlan.isEmpty();
    this.windowState = WindowState.empty();
    this.preOrderBy = canonicalizeOrderKeys(opts.preOrderBy);
    this.hasPreStage = (this.preLimit >= 0) || !this.preOrderBy.isEmpty() || (this.preOffset > 0);
    List<String> distinctCols = opts.distinctColumns == null
        ? defaultDistinctColumns(opts.schema, this.projection)
        : opts.distinctColumns;
    this.distinctColumns = distinctCols;
    this.distinctExpressions = opts.distinctExpressions;
    this.mixedDistinctTypes = new boolean[distinctExpressions.size()];
    for (int column = 0; column < distinctExpressions.size(); column++) {
      mixedDistinctTypes[column] = mayMixWholeNumberTypes(distinctExpressions.get(column));
    }
    this.preStageDistinctColumns = (opts.preStageDistinctColumns == null || opts.preStageDistinctColumns.isEmpty())
        ? distinctCols
        : opts.preStageDistinctColumns;
    this.orderBy = canonicalizeOrderKeys(opts.orderBy);
    this.orderByExpressions = normalizeOrderByExpressions(opts.orderByExpressions);
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

    if (!this.orderBy.isEmpty() || hasPreStage || windowed) {
      // Buffered input includes firstAlreadyRead; it has not been emitted from
      // this processor, regardless of the caller's streaming-prefetch hint.
      this.emitted = 0;
      this.skipped = 0;
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

      // An empty input leaves no record to take the schema from. The reader
      // schema is the full input schema; ordinal bounds derived from it use
      // only the fields a wildcard projection exposes, so the columns a USING
      // join keeps hidden do not widen the bound. A narrower read schema would
      // validate positions against the wrong width.
      if (sortSchema == null) {
        sortSchema = reader.schema();
      }

      // ensure evaluator exists if WHERE is used
      ensureEvaluator(sortSchema);

      boolean distinctApplied = false;
      if (hasPreStage) {
        if (distinctBeforePreLimit && distinct && !buf.isEmpty()) {
          buf = applyDistinct(buf);
          distinctApplied = true;
        }
        if (!preOrderBy.isEmpty() && sortSchema != null) {
          // Building the comparator resolves each key once, so invalid
          // positional references fail even without a row to compare.
          Comparator<GenericRecord> comparator = buildComparator(sortSchema, preOrderBy);
          if (buf.size() > 1) {
            buf.sort(comparator);
          }
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

      if (windowed) {
        if (sortSchema == null && !buf.isEmpty()) {
          sortSchema = buf.getFirst().getSchema();
        }
        windowState = WindowFunctions.compute(windowPlan, buf, sortSchema, subqueryExecutor, outerQualifiers,
            qualifierColumnMapping, unqualifiedColumnMapping);
        valueEvaluator = null;
      }

      if (distinct && !buf.isEmpty() && !distinctApplied) {
        buf = applyDistinct(buf);
      }

      // Building the comparator resolves each key once, so invalid positional
      // references fail even when there is nothing to sort.
      if (!orderBy.isEmpty() && sortSchema != null) {
        Comparator<GenericRecord> comparator = buildComparator(sortSchema, orderBy);
        if (buf.size() > 1) {
          buf.sort(comparator);
        }
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

  private Comparator<GenericRecord> buildComparator(Schema schema, List<SqlParser.OrderKey> keys) {
    // Resolve every key up front so ordinal references are validated against
    // the schema once, instead of being re-parsed on each comparison.
    List<Function<GenericRecord, Object>> extractors = new ArrayList<>(keys.size());
    for (SqlParser.OrderKey key : keys) {
      extractors.add(buildOrderValueExtractor(schema, key));
    }
    return (a, b) -> {
      for (int i = 0; i < keys.size(); i++) {
        Object va = extractors.get(i).apply(a);
        Object vb = extractors.get(i).apply(b);
        SqlParser.OrderKey key = keys.get(i);

        boolean hasNull = va == null || vb == null;
        int nullCmp = OrderingUtil.compareNulls(va, vb, key.asc(), key.nullOrdering());
        if (nullCmp != 0) {
          return nullCmp;
        }
        if (hasNull) {
          continue;
        }

        int c = ExpressionEvaluator.typedCompare(va, vb);
        if (c != 0) {
          return key.asc() ? c : -c;
        }
      }
      return 0;
    };
  }

  /**
   * Builds the value extractor for one ordering key, resolving positional
   * references and column lookups against the schema once. Keys bound to an
   * expression keep the original per-record evaluation, so the decorative
   * positional literal of a bound key is not validated against the input schema.
   *
   * @param schema
   *          sort schema of the buffered rows
   * @param key
   *          the ordering key
   * @return the extractor producing the key value for a record
   * @throws IllegalArgumentException
   *           if a positional reference is outside the visible select-list width
   *           or a column reference resolves to no field, with SQLState 42000
   */
  private Function<GenericRecord, Object> buildOrderValueExtractor(Schema schema, SqlParser.OrderKey key) {
    if (key.expression() == null) {
      return buildColumnOrderValueExtractor(schema, key);
    }
    Expression expression = key.expression();
    return record -> {
      ensureValueEvaluator(schema);
      return valueEvaluator.eval(expression, record);
    };
  }

  /**
   * Builds the value extractor for the column reference of an ordering key.
   *
   * @param schema
   *          sort schema of the buffered rows
   * @param key
   *          the ordering key
   * @return the extractor producing the column value for a record
   * @throws IllegalArgumentException
   *           if a positional reference is outside the visible select-list width
   *           or the column reference resolves to no field, with SQLState 42000
   */
  private Function<GenericRecord, Object> buildColumnOrderValueExtractor(Schema schema, SqlParser.OrderKey key) {
    String column = key.column();
    if (OrderingUtil.isPosition(column)) {
      // The bound is the wildcard projection of the input, not the record
      // width: a USING join keeps its peer copies of the join columns in the
      // record but hides them from the select list. Positional references over
      // a projection wider than the input are bound to their select-list
      // expressions at preparation, so an unbound key always addresses the
      // wildcard width.
      List<String> visible = computeProjection(List.of("*"), schema);
      int ordinal = OrderingUtil.position(column, visible.size());
      Schema.Field field = schema.getField(visible.get(ordinal - 1));
      return record -> AvroCoercions.unwrap(record.get(field.name()), field.schema());
    }
    Schema.Field field = resolveOrderField(schema, column);
    if (field != null) {
      return record -> AvroCoercions.unwrap(record.get(field.name()), field.schema());
    }
    Expression expression = orderExpressionFor(column);
    if (expression != null) {
      return record -> {
        ensureValueEvaluator(schema);
        return valueEvaluator.eval(expression, record);
      };
    }
    // Resolving every key up front means an unresolvable reference fails here
    // instead of degrading to a no-op sort that compares every key equal.
    throw SqlExceptions.syntaxFailure("Unknown column for ORDER BY: " + column);
  }

  /**
   * Resolves an ordering column to a field of the sort schema. An exact
   * physical-name match wins, and a quoted identifier then falls back to its
   * unquoted text without case folding, while an unquoted identifier falls back
   * to a case-insensitive match.
   *
   * @param schema
   *          sort schema of the buffered rows
   * @param column
   *          the ordering column text, as written
   * @return the matching field, or {@code null} when the reference resolves to no
   *         field
   */
  private Schema.Field resolveOrderField(Schema schema, String column) {
    if (column == null) {
      return null;
    }
    Schema.Field field = schema.getField(column);
    if (field != null) {
      return field;
    }
    Identifier identifier = Identifier.of(column);
    if (identifier != null && identifier.quoted()) {
      // A quoted identifier keeps its declared case, so there is no
      // case-insensitive fallback.
      return schema.getField(identifier.text());
    }
    return findFieldIgnoreCase(schema, column);
  }

  private Map<String, Expression> normalizeOrderByExpressions(Map<String, Expression> expressions) {
    if (expressions == null || expressions.isEmpty()) {
      return Map.of();
    }
    Map<String, Expression> normalized = new java.util.HashMap<>();
    for (Map.Entry<String, Expression> entry : expressions.entrySet()) {
      String key = entry.getKey();
      Expression expression = entry.getValue();
      if (key == null || key.isBlank() || expression == null) {
        continue;
      }
      String normalizedKey = Identifier.lookupKey(key);
      if (normalizedKey != null) {
        normalized.putIfAbsent(normalizedKey, expression);
      }
      String canonical = ColumnMappingUtil.canonicalOrderColumn(key, null, qualifierColumnMapping,
          unqualifiedColumnMapping);
      if (canonical != null) {
        String canonicalKey = Identifier.lookupKey(canonical);
        if (canonicalKey != null) {
          normalized.putIfAbsent(canonicalKey, expression);
        }
      }
    }
    return Map.copyOf(normalized);
  }

  /**
   * Creates the shared projection-key and ordering evaluator when needed.
   *
   * @param schema
   *          source schema, or {@code null} when input is empty
   */
  private void ensureValueEvaluator(Schema schema) {
    if (valueEvaluator == null && schema != null) {
      CorrelationMappings mappings = new CorrelationMappings(outerQualifiers, qualifierColumnMapping,
          unqualifiedColumnMapping, qualifierColumnMapping);
      valueEvaluator = new ValueExpressionEvaluator(schema, subqueryExecutor, mappings, windowState());
    }
  }

  private Schema.Field findFieldIgnoreCase(Schema schema, String name) {
    if (schema == null || name == null) {
      return null;
    }
    for (Schema.Field f : schema.getFields()) {
      if (f != null && name.equalsIgnoreCase(f.name())) {
        return f;
      }
    }
    return null;
  }

  private Expression orderExpressionFor(String column) {
    if (column == null || orderByExpressions.isEmpty()) {
      return null;
    }
    String key = Identifier.lookupKey(column);
    if (key == null) {
      return null;
    }
    return orderByExpressions.get(key);
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
      canonical.add(new SqlParser.OrderKey(resolved, key.asc(), key.qualifier(), key.nullOrdering(), key.expression(),
          key.projectionIndex()));
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
    if (!orderBy.isEmpty() || hasPreStage || windowed) {
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

  /**
   * Retrieve the precomputed analytic window state associated with this
   * processor.
   *
   * @return the computed {@link WindowState}, or an empty state when no window
   *         functions are present
   */
  public WindowState windowState() {
    return windowState == null ? WindowState.empty() : windowState;
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
        if (MaterializedNames.isVisible(f)) {
          cols.add(f.name());
        }
      }
      return cols;
    }
    return requested;
  }

  private boolean registerDistinct(GenericRecord rec) {
    if (!distinct) {
      return true;
    }
    List<Object> key = projectedDistinctKey(rec);
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
      List<Object> key = projectedDistinctKey(rec);
      if (seen.add(key)) {
        unique.add(rec);
      }
    }
    if (distinctSeen != null) {
      distinctSeen.addAll(seen);
    }
    return unique;
  }

  /**
   * Evaluates the SELECT tuple in positional order, including duplicate labels.
   * The shared evaluator retains the window state keyed by source record
   * identity.
   *
   * @param rec
   *          source record
   * @return projected values with SQL NULLs preserved
   */
  private List<Object> projectedDistinctKey(GenericRecord rec) {
    if (distinctExpressions.isEmpty()) {
      return distinctKey(rec, distinctColumns);
    }
    ensureValueEvaluator(rec.getSchema());
    List<Object> key = new ArrayList<>(distinctExpressions.size());
    for (int column = 0; column < distinctExpressions.size(); column++) {
      Object value = valueEvaluator.eval(distinctExpressions.get(column), rec);
      key.add(RelationalKeys.value(value, mixedDistinctTypes[column]));
    }
    return key;
  }

  /**
   * Identifies projections whose scalar values may mix INTEGER and BIGINT. Source
   * columns and literals have stable Java types, including in parentheses.
   *
   * @param expression
   *          projection expression
   * @return whether whole-number scalar keys require normalization
   */
  private static boolean mayMixWholeNumberTypes(Expression expression) {
    if (expression instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      return mayMixWholeNumberTypes(list.getFirst());
    }
    return !(expression instanceof Column) && !LiteralConverter.isLiteral(expression);
  }

  /**
   * Builds a physical-column key for wildcard projections and legacy options.
   *
   * @param rec
   *          source record
   * @param columns
   *          physical fields
   * @return values with SQL NULLs preserved
   */
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
    return RelationalKeys.row(key, false);
  }

  /**
   * Derives the DISTINCT key list for a wildcard select. The keys are the input
   * fields, never the ORDER BY references: an ordering key is not a select-list
   * column, whatever its textual form (position, quoted, or case-variant), and
   * must not participate in deduplication (#611).
   *
   * @param schema
   *          input schema of the records, may be {@code null} when unknown
   * @param fallback
   *          the projected columns, used only when the schema is unknown
   * @return the physical fields a {@code SELECT DISTINCT *} deduplicates on
   */
  private static List<String> defaultDistinctColumns(Schema schema, List<String> fallback) {
    return schema == null ? fallback : List.copyOf(computeProjection(List.of("*"), schema));
  }
}
