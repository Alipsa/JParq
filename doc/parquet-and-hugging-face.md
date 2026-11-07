# Parquet files and Hugging Face datasets

This page describes how JParq reads Parquet files, which predicates it pushes down to the Parquet reader, and how
it maps Hugging Face dataset layouts to tables. For SQL semantics, see the [SQL reference](sql-reference.md).

## Contents

- [Supported Parquet features](#supported-parquet-features)
- [Predicate pushdown](#predicate-pushdown)
- [Hugging Face dataset layouts](#hugging-face-dataset-layouts)
- [Discovery and schema validation](#discovery-and-schema-validation)
- [Metadata and listings](#metadata-and-listings)
- [Table names containing dots or hyphens](#table-names-containing-dots-or-hyphens)
- [API note: `TableLocation`](#api-note-tablelocation)

## Supported Parquet features

JParq reads ordinary Parquet files and Hugging Face optimized Parquet files, including files written by PyArrow with
content-defined chunking (CDC). CDC changes page boundaries; it does not require a special reader. When a file
contains a page index, JParq uses it automatically with supported predicates to skip pages within row groups.

| Parquet type | How it is returned |
| --- | --- |
| UTC-adjusted timestamps | `java.sql.Timestamp` instants |
| Local timestamps | Stored wall-clock values in the JVM's default time zone |
| Nanosecond and microsecond timestamps | Full precision is preserved |
| float16 | Readable |
| Lists (arrays) | `Object[]` from `ResultSet.getObject`; both the legacy 2-level and the standard 3-level LIST layouts are supported, also with projection and predicate pushdown |
| Structs, including image structs | Avro `GenericRecord` from `getObject`; JSON text from `getString`. See [structs](sql-reference.md#structs) |

The CLI `/describe <table>` command reports row groups, row counts, and page-index availability. `/info` reports the
connection's pushdown settings.

## Predicate pushdown

Predicate pushdown is enabled by default and can be disabled with `predicatePushdown=false`.

### Supported predicates

- Column-to-literal comparisons
- `BETWEEN`
- `IN` with up to 20 literals
- `IS NULL` and `IS NOT NULL`
- Supported combinations of `AND`, `OR`, and `NOT`. Supported terms of an `AND` are pushed independently; an `OR`
  is pushed only if it is fully supported.
- Case-sensitive string `LIKE 'prefix%'`, using a UTF-8 prefix range. The exact pattern is still evaluated in Java.
- Date, timestamp, and exactly representable decimal literals

`DISTINCT` queries can use pushed-down filters too.

### Evaluated in Java instead

- `ILIKE`, escaped patterns, and prefixes containing `_`
- Incompatible or out-of-range literal bounds
- On `FLOAT` and `DOUBLE` columns: `>`, `>=`, `<>`, `NOT IN`, `NOT BETWEEN`, and comparisons with zero. PyArrow
  leaves `NaN` out of Parquet statistics, so only `<`, `<=`, `=`, `IN`, and `BETWEEN` against a non-zero literal
  are pushed down. With Parquet 1.18, floating-point page indexes without NaN counts keep all pages for range
  predicates; equality and `IN` can still prune pages, as can eligible predicates on other columns.
- Comparisons with `TIME` literals (see [#359](https://github.com/Alipsa/JParq/issues/359)). Results are the same
  with pushdown on or off.

Floating-point comparisons give the same results with and without pushdown; see
[floating-point ordering](sql-reference.md#floating-point-values).

### Joins, sampling, and projections

- Single-table conditions in a join's `WHERE` can filter individual inputs. Unqualified columns must resolve to one
  input. Outer-join filtering preserves NULL-extended rows, and merged `FULL JOIN ... USING` columns remain in the
  final filter. The complete `WHERE` is always evaluated after the join.
- Pushdown is disabled for sampled inputs. `TABLESAMPLE` chooses from the unfiltered input before `WHERE`, so
  `REPEATABLE` samples do not depend on the pushdown setting.
- Parenthesized join groups currently materialize their inputs eagerly. Outer `WHERE` predicates are evaluated after
  the grouped join and are not pushed into its physical tables, so grouping can increase reads and memory use for
  large inputs. Pushdown into nested groups is not yet implemented
  ([#478](https://github.com/Alipsa/JParq/issues/478)).
- `ORDER BY` projections keep the columns needed for sorting and correlated subqueries, while excluding unrelated
  columns such as embedded media.

## Hugging Face dataset layouts

The `datasetLayout` option accepts `none` and `hf`, without regard to case (`HF` and `None` work).
Empty, whitespace-padded, and unknown values fail connection creation with a message listing the allowed values.

By default (`datasetLayout=none`), each shard is a separate table. With `datasetLayout=hf`, point the URL at the
dataset root to combine shards into logical tables:

| Relative files | Logical table |
| --- | --- |
| `data/train-0-of-2.parquet`, `data/train-1-of-2.parquet` | `data.train` |
| `default/train/0000.parquet`, `default/train/0001.parquet` | `default.train` |
| `train-0-of-1-a1b2.parquet` | `PUBLIC.train` |

The detailed requirements and verification record are in
[hf-dataset-layout.md](../requirements/done/hf-dataset-layout.md).

### Schemas and tables

- Files directly in the base directory belong to `PUBLIC`.
- An immediate child directory containing only numeric Parquet files and no visible child directories becomes a
  `PUBLIC` table rather than a schema.
- Any other child directory is a schema. Its direct files, and its eligible immediate numeric directories, become
  tables.
- README files do not disqualify a directory from being a table; a visible child directory (such as
  `partial-train/`) or a non-numeric Parquet file does.
- Empty directories are schemas, not tables.
- All dot-prefixed entries are ignored, including `.cache/` and `.ipynb_checkpoints/`. The base directory you choose
  is honored even if its own name starts with a dot.
- A base directory that is itself a table is assigned only the table role; its name does not also create a schema.
- There are no nested schemas or deeper traversal.

### Choosing the base directory

Using the example above:

| URL points at | Result |
| --- | --- |
| dataset root | `data.train`, `default.train`, `PUBLIC.train` |
| `default/` | `PUBLIC.train`, without a `train` schema |
| `data/` | named splits as `PUBLIC` tables |
| `default/train/` | the individual files `PUBLIC.0000` and `PUBLIC.0001` |

A dataset-root file such as `data/config/train-*.parquet` is too deep and is not discovered; point at `data/` to
expose it as `config.train`.

### Named shards

Named shards must match this regex (the hexadecimal hash suffix is optional):

```regex
^(?<split>[A-Za-z0-9_][A-Za-z0-9_.-]*)-(?<index>[0-9]+)-of-(?<total>[0-9]+)(?:-(?<hash>[0-9a-fA-F]+))?\.(?i:parquet)$
```

- A group requires a positive common total and exactly one file for each index from zero through total minus one.
- Missing, duplicate (including padded or hash variants), out-of-range, and disagreeing-total shards fail discovery.
- Filenames that do not match remain ordinary single-file tables.

### Numeric directory tables

- Files in a directory table must match `^[0-9]+\.(?i:parquet)$`.
- Gaps in the numbering are allowed, but duplicate numeric indices are errors.

### Reading logical tables

- Both layouts scan files in numeric order (`9` before `10`). SQL row order still requires `ORDER BY`.
- Row bounds, `DISTINCT`, aggregation, window functions, joins, and repeatable sampling operate on the whole logical
  table.
- Each shard keeps its own predicate and projection pushdown, physical read schema, and enriched logical types.
- At most one shard reader is open per logical input. The reading thread closes its input before opening the next
  shard.

### Symbolic links

- File and directory symlinks are followed, including visible `.parquet` links to extensionless cache blobs outside
  the base directory. Names and discovery depth come from the visible paths.
- Dot-prefixed links are ignored. Other dangling non-Parquet links, such as README, are ignored unless they resolve
  to directories.
- Visible invalid `.parquet` links fail discovery. Directories that cannot be listed also fail discovery.

### Name collisions

- HF mode rejects table source collisions (single file, group, or directory) and normalized schema collisions, even
  for empty or README-only schema directories.
- Case variants collide with `caseSensitive=false`; they remain distinct with `caseSensitive=true` on file systems
  that support it.

## Discovery and schema validation

**Discovery is catalog-wide** and reads no footers. One incomplete group, naming collision, or broken Parquet link
fails every query, even one on an unrelated table, and every filtered catalog listing. Repairing the problem lets the
next discovery succeed.

**Schema validation is lazy.** It compares field names and order, types, nullability, containers, enum symbols,
fixed sizes, and logical-type parameters recursively. Union branch order, record names and namespaces, docs,
defaults, and non-semantic writer metadata may differ. There is no schema widening or evolution.

- Each query input is fully validated before rows are returned.
- A prepared statement reuses its validated schemas across executions; validation is not shared between prepared
  statements.
- An incompatible group does not prevent queries on healthy physical tables.

## Metadata and listings

| Operation | Validation scope |
| --- | --- |
| `getTables`, CLI `/list`, `information_schema.tables` | Read remarks from the first file only, without full shard validation. First-file footer I/O errors can still fail a listing. |
| `getColumns` | Validates every shard of every matching table. A call restricted to a healthy table is independent of other tables. Wildcard `getColumns(null, null, "%", "%")` covers the whole catalog. |
| `information_schema.columns` | Materializes all catalog columns before SQL `WHERE`, so even a filter naming a healthy table fails if another table is incompatible. |
| CLI `/describe` and queries | Validate their selected inputs. |

- Column requests cost one footer read per included shard, repeated for new statements; there is no shared
  validation cache.
- Table and column remarks come from the first numeric file.
- `/describe` sums files, row groups, and unfiltered row counts. It reports page indexes as `present` (all chunks
  have both indexes), `absent` (none have either, including no chunks), or `partial` (anything else).

## Table names containing dots or hyphens

Dots and hyphens can occur in table names.

**In SQL:**

- Use schema-qualified names for dotted tables: `data."train.v2"` or `PUBLIC."train.v2"`. Unqualified
  `"train.v2"` does not resolve, even with an alias.
- Unqualified hyphenated names work: `"train-extra"` and `"train-extra" t`.
- With the default `caseSensitive=false`, lowercase both schema and table names before quoting names taken from
  listings: listed `Train-X` becomes `data."train-x"`. Leave `PUBLIC` unquoted or write `"public"`; quoted
  `"PUBLIC"` does not resolve in this mode. Quoting original mixed-case listed names therefore does not work with
  the default setting.
- With `caseSensitive=true`, use the exact listed spelling, such as `data."Train-X"` or `"PUBLIC"."train.v2"`.

**In the CLI `/describe` command:**

- Arguments are literal names without SQL quotes. JDBC metadata wildcards (`_`, `%`, and the escape character) are
  escaped before columns are requested.
- It describes one exact table, using `PUBLIC` when no schema is given.
- The argument is split at the first dot: `/describe data.train.v2` describes table `train.v2` in schema `data`,
  and `/describe train.v2` means schema `train`, table `v2`. For a dotted `PUBLIC` table, use
  `/describe PUBLIC.train.v2`.
- `/describe train-extra` needs no schema for a `PUBLIC` table.

## API note: `TableLocation`

`TableLocation` has an immutable, non-empty `List<File> files` record component. Its original single-file
constructor and `file()` accessor remain available, but `file()` returns only the first file. Code inspecting record
components must account for this, and logical-table consumers should use `files()`.
