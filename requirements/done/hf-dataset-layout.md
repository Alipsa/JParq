# Hugging Face Dataset Layout Support

Follow-up to [optimized-parquet.md](optimized-parquet.md). Implemented and verified on Java 21.
`datasetLayout=none` remains the default; `hf` opts into the behavior specified below.

## Background and URL base directory

In the baseline/default `none` layout, `JParqConnection.listTables()` scans Parquet files in the URL's base directory and its immediate child
directories only. Files at the base become tables in `PUBLIC`; files in an immediate child become tables in
that child's schema. Discovery does not recurse into a schema's subdirectories.

For a dataset root `/datasets/example`, common layouts are:

```text
# Repository data files
/datasets/example/
  data/
    train-00000-of-00003.parquet
    train-00001-of-00003.parquet
    train-00002-of-00003.parquet
    test-00000-of-00001.parquet

# Auto-converted Parquet (refs/convert/parquet)
/datasets/example/
  default/
    train/
      0000.parquet
      0001.parquet
    test/
      0000.parquet
```

With `jdbc:jparq:/datasets/example`, the baseline repository layout exposes individual shards in schema `data`.
The baseline auto-converted layout exposes no tables: its Parquet files are two directories below the base.
Pointing the URL at `/datasets/example/default` instead exposes individual `0000`/`0001` tables in schema
`train`, and `0000` in schema `test`. There is no nested-schema model today.

The new behavior is opt-in. The examples above use `jdbc:jparq:/datasets/example?datasetLayout=hf`, pointing
at the dataset root, and expose `data.train`/`data.test` or `default.train`/`default.test`, respectively.
The exact discovery boundary and behavior for a different URL base are specified below.

## Requirements

### 1. Connection property and compatibility

- `datasetLayout` accepts `none` and `hf`, comparing values case-insensitively, consistently with the existing
  Boolean pushdown properties: `HF` and `None` are accepted, and diagnostics use lowercase `hf`/`none`.
  Do not trim whitespace; a value such as ` hf ` is invalid. The default when absent is `none`;
  explicitly specifying `none` has the same effect. An empty or unknown value causes connection
  creation to throw `SQLException` naming the invalid value and the allowed values (`none`, `hf`).
- `none` preserves the existing discovery, table naming, and single-file behavior, including existing
  collision behavior. The stricter grouping, collision, and directory rules below apply only to `hf`.
- `hf` changes logical discovery and resolution; grouped shard files are not also exposed as individual tables.
  `listTables`, `listSchemas`, JDBC metadata, SQL resolution, and CLI listings must agree on the same model.

### 2. Discovery depth and directory tables

In `hf` mode, scan only these locations relative to the URL base:

1. Parquet files directly in the base: ordinary or grouped tables in `PUBLIC`.
2. Immediate child directories: either a directory table in `PUBLIC`, or a schema directory, as defined below.
3. Inside a schema directory: direct Parquet files form ordinary or grouped tables, and eligible immediate
   subdirectories form directory tables in that schema. A subdirectory inside a schema directory can therefore
   become a table. Do not recurse beyond these table directories or create nested schemas.

Ignore dot-prefixed file and directory entries at every scanned level in `hf` mode, before eligibility,
grouping, completeness, collision, or schema checks. Do not descend into them. For example, `.cache/` does
not become a schema, and `.ipynb_checkpoints/` does not disqualify a directory table. Hidden Parquet files
are not table candidates or shards. An explicitly supplied URL base is still honored even if its own basename
starts with a dot; the exclusion applies to entries discovered beneath it. `none` retains existing behavior.

Follow symbolic links to files and directories, consistent with today's `File.isFile()` behavior. A "regular
Parquet file" includes a symlink whose target is a regular file; do not use `NOFOLLOW_LINKS` to reject it.
Derive names, grouping, hidden-entry filtering, and depth from the visible entry path, not the resolved target:
`snapshots/<rev>/default/train/0000.parquet` may point to an extensionless blob outside the URL base (including
inside a dot-prefixed cache path) and still be a shard named `0000.parquet`. Following a directory link does
not expand the discovery-depth boundary. Classify links that resolve to directories as directories first.
Any other visible link with a `.parquet` extension must resolve to a regular file: a dangling link, an
uninspectable target, or a target of the wrong kind raises a discovery `SQLException`
naming the link and target. Actual data/footer read errors on an existing file are reported on table access
under requirement 6. Links with non-Parquet names are followed if they resolve to directories; otherwise they
are ignored, including dangling `README.md` links. A directory that is identified but cannot be listed is a
discovery error. Dot-prefixed links are ignored before target resolution. Discovery errors are catalog-wide
as specified in requirement 4.
These are filesystem requirements, independent of any particular Hugging Face client's current cache format.

A directory **is exposed** as a directory table if it contains at least one visible regular Parquet file, every
visible Parquet file has a numeric basename matching `^[0-9]+\.(?i:parquet)$`, and it contains no visible child
directories. Non-Parquet regular files such as `README.md` are ignored; `.gitattributes` is already excluded
by the dot-prefix rule. Any visible child directory, including `partial-train/`, disqualifies the parent; so
does a visible nonnumeric Parquet filename.
An empty directory is not a directory table. Numeric filenames do not declare a shard total, so gaps are allowed.
Duplicate numeric indices such as `9.parquet` and `09.parquet` are errors, as specified in requirement 3.

An eligible immediate child of the base is consumed as a `PUBLIC` directory table and is **not also a schema**;
its numeric files are not separately exposed. A noneligible immediate child remains a schema directory with
ordinary/grouped direct-file discovery and eligible directory tables one level below it. Noneligible directories
inside a schema do not become tables, and their descendants are not scanned.

Examples (all with `datasetLayout=hf`):

| URL base | Relative files | Logical table |
| --- | --- | --- |
| `/datasets/example` | `default/train/0000.parquet`, `default/train/0001.parquet` | `default.train` |
| `/datasets/example/default` | `train/0000.parquet`, `train/0001.parquet` | `PUBLIC.train`; no `train` schema |
| `/datasets/example` | `data/train-00000-of-00001.parquet` | `data.train` |
| `/datasets/example/data` | `train-00000-of-00001.parquet` | `PUBLIC.train` |
| `/datasets/example` | `data/config/train-00000-of-00001.parquet` | Not discovered; outside the depth boundary |
| `/datasets/example/data` | `config/train-00000-of-00001.parquet` | `config.train` |

Thus `data/<config>/<split>-*.parquet` is out of scope when the URL points at the dataset root. Support it by
pointing the URL at `data/`; automatic traversal or flattening of that deeper dataset-root layout is not part
of this feature. Pointing the URL at a directory table itself (e.g. `default/train/`) leaves its direct numeric
files as ordinary `PUBLIC.0000`, `PUBLIC.0001` tables; grouping the base directory under its own name is not supported.

### 3. Shard filename grammar, completeness, and order

Match named shards against this exact Java regular expression (shown as regex text, not a Java string literal):

```regex
^(?<split>[A-Za-z0-9_][A-Za-z0-9_.-]*)-(?<index>[0-9]+)-of-(?<total>[0-9]+)(?:-(?<hash>[0-9a-fA-F]+))?\.(?i:parquet)$
```

- The optional hexadecimal hash suffix is allowed, including older names such as
  `train-00000-of-00003-a1b2c3d4.parquet`. Hashes may differ between shards and do not contribute to table identity.
- Dots and hyphens in split names are deliberately supported. They belong to the table identifier, not to
  schema qualification or subtraction: use `SELECT * FROM data."train.v2"` or
  `SELECT * FROM data."train-extra"` for groups named `train.v2` or `train-extra`. Listings return those
  names without SQL quote characters. Dotted table names **must be schema-qualified**, including tables in
  the default schema: when the URL points at `data/`, use `SELECT * FROM PUBLIC."train.v2"`.
  Unqualified `FROM "train.v2"` and `FROM "train.v2" t` remain unsupported: current multipart-name parsing
  splits them at the dot. Fixing that existing parser limitation is outside this feature. Document it without
  requiring regression assertions that the bug persists; test the working qualified forms.
  Hyphenated names do not have this limitation: for a `PUBLIC` table, `FROM "train-extra"` and
  `FROM "train-extra" t` both work without schema qualification and must remain supported.
- Preserve current identifier matching under `caseSensitive`; this feature does not redesign quoted-name
  normalization. With `caseSensitive=false`, discovered names are normalized to lowercase, while quoted SQL
  references are compared as written. Lowercase **both schema and table names before quoting** when generating
  SQL from listings: a listed `Train-X` in `data` is queried as `data."train-x"`, not `data."Train-X"`.
  Leave `PUBLIC` unquoted, or write `"public"`; `"PUBLIC"."top.v2"` does not resolve in this mode.
  With `caseSensitive=true`, preserve exact listed spelling/case, e.g. `data."Train-X"` and
  `"PUBLIC"."top.v2"`. Apply this rule to directory and ordinary single-file names as well as split names.
  The earlier lowercase examples work under the default `caseSensitive=false`; blindly quoting names as
  returned by listings is not a supported SQL-generation strategy.
- Group matching files by their exact `<split>` text within one directory. Case variants remain separate
  candidates until the identifier-collision check in requirement 4. Files not matching the grammar remain
  ordinary single-file tables. A matching filename with invalid numeric values is an error, not a fallback table.
- Parse indices and totals numerically, ignoring zero padding. Require `total > 0`, `0 <= index < total`, one
  common total for the group, and exactly one file for each index from `0` through `total - 1`.
  Missing shards are errors: two files from an `of-00003` group must not silently produce a partial table.
  Disagreeing totals and duplicate indices (including different hashes or padding for the same index) are errors.
  Throw `SQLException` during discovery/resolution naming the directory, logical split, expected total, and
  offending or missing indices/files. Do not read footers to perform these filename checks.
- Read named shards in ascending **numeric index** order, not lexical filename order. Read directory-table
  files in ascending numeric basename order; `9.parquet` comes before `10.parquet`. Duplicate directory-table
  indices are errors rather than tie-breaks. There is no completeness check for numeric directory names because
  they have no declared total.
- This defines deterministic internal scan order, including repeatable sampling. SQL result order still requires
  an explicit `ORDER BY`; tests must not treat unqualified encounter order as a general SQL guarantee.

### 4. Naming collisions

Choose errors, with no precedence between competing table sources. In `hf` mode, build all candidates and
reject any two distinct sources with the same `(schema, table)` identifier under the connection's existing
`caseSensitive` normalization. Do not retain today's silent `putIfAbsent` winner. Raise `SQLException` naming
both sources and the normalized logical name. Perform these checks without reading file footers.

This includes:

- `train.parquet` alongside a complete `train-*-of-*` group in the same schema.
- A named shard group versus a directory table of the same name, including `PUBLIC.train` when the base
  contains both `train-*-of-*` files and an eligible `train/` directory.
- A single file versus a directory table of the same name inside a schema.
- Case-insensitive clashes such as `Train.parquet` and `train.parquet`, or groups named `Train` and `train`,
  when `caseSensitive=false`. With `caseSensitive=true`, differently cased identifiers remain distinct.
- Colliding schema names after normalization. For example, `base/train.parquet` and
  `base/public/train.parquet` both produce `PUBLIC.train` and must fail. Schema directories that normalize
  to the same schema must be checked together so their tables cannot silently replace each other; reject
  duplicate normalized schema directory names independently of their table contents. Thus visible `Data/`
  and `data/`, both classified as schemas with `caseSensitive=false`, fail even if one or both are empty or
  contain only a README. Dot-prefixed directories are excluded before this check.

The directory-table classification in requirement 2 is not a collision: an eligible base child is deliberately
assigned only the table role, so it does not simultaneously generate a schema candidate.

Discovery is deliberately **catalog-wide and fail-fast** in `hf` mode. `resolveTable()` calls `listTables()`;
complete discovery and its filename, symlink, and collision checks before selecting the requested identifier.
A missing/duplicate named shard, inconsistent total, name collision, visible broken `.parquet` link, or
unlistable discovered directory anywhere within the supported scan boundary therefore fails even a query
against an unrelated healthy table. Catalog metadata/CLI operations that discover tables or schemas use the
same rule, even if a name pattern would otherwise exclude the broken source. Do not return a partial catalog
or defer those discovery errors only to resolving the damaged table. Repairing/removing the invalid entry
allows the next discovery to succeed; do not permanently poison the connection. Statements without filesystem
table resolution and metadata methods that do not discover the catalog are not required to fail.

Ignored non-Parquet file links (including a dangling `README.md`) and dot-prefixed entries do not trigger this
policy. Full schema-compatibility checks remain lazy under requirement 6: an ordinary query executing only a
healthy filesystem table, or a table-name/remarks listing, does not validate an unrelated table. Column metadata
requests that include an incompatible table fail. In particular, `information_schema.columns` always loads
columns for the entire catalog before applying SQL filters, so even a query filtered to a healthy table fails
if another table has incompatible shards. `/describe` and direct table execution validate their own inputs.
A table listing can read first-file remarks without asserting schema compatibility, as specified below.

### 5. Table model and shared access paths

Change the public nested record `JParqConnection.TableLocation` to:

```java
public record TableLocation(String schemaName, String tableName, List<File> files) {
  // Retain the existing (String, String, File) constructor as a delegating overload.
  // Retain file(), returning files().getFirst(), for existing single-file callers.
}
```

`files()` is an immutable, nonempty, ordered list with no null entries. Discovery supplies files in requirement 3's
order. Existing schema/table non-null requirements remain. The single-file constructor wraps its argument in a
one-element list; it and `file()` retain their existing signatures. The record components now include `files`
instead of `file`; document this change for callers inspecting record components. `file()` is a compatibility
accessor for the first physical file, not an execution API for a logical table. Remove both package-private
`JParqConnection.tableFile(...)` overloads: repository searches find no callers in `src/main` or `src/test`,
so there is no compatibility requirement to preserve them. Internal execution must use the full file list.

Introduce shared table-schema validation and a shared reader factory accepting a `TableLocation` and the
per-input read configuration/predicate. All execution and schema consumers must use this logical-table path.
Audit these current single-file sites (line numbers refer to main after PR #188):

| Consumer | Current single-file site | Required change |
| --- | --- | --- |
| `JParqConnection` | `tableFile(...)`, lines 96 and 112 | Remove both unused package-private overloads |
| `JParqPreparedStatement` | constructor/planning, line 367; `executeQuery` reader construction | Plan from validated logical schema and open the shared multi-file reader |
| `JParqPreparedStatement` | `planJoinPredicates`, line 1233 | Use the validated logical input schema |
| `JParqPreparedStatement` | `buildJoinReader`, line 1326 and its own `ParquetReader` construction | Chain **every** file for each join input using that input's predicate/configuration; do not bypass the shared factory |
| `JParqPreparedStatement` | `schemaForTableReference`, line 2562 | Use the validated logical schema for derived/correlated schema inference |
| `JParqDatabaseMetaData` | `getTables`, lines 490–491 | Read first-file remarks only; do not validate schemas across shards |
| `JParqDatabaseMetaData` | `getColumns`, line 524 | Validate every shard before reporting the representative column schema |
| `InformationSchemaColumns` / `JParqPreparedStatement` | `InformationSchemaColumns.load`, line 109; `materializeInformationSchemaColumns` | Retain eager catalog-wide `getColumns(catalog, null, "%", "%")` before SQL WHERE; document its validation scope and cost |
| `JParqCliSession` | `/describe`, lines 239–268 and 284 | Resolve one exact table, escape metadata patterns, then describe that table's files and aggregate diagnostics |

Search for other `.file()`, `tableFile(...)`, and independent `ParquetReader`/footer construction paths during
implementation, including `information_schema` delegation. Merely changing the main SELECT reader is insufficient.

### 6. Schema compatibility and lazy validation

Compatibility means equality of the normalized **logical Avro field structure** returned by JParq's existing
Parquet-to-Avro schema reading and string normalization (`ParquetSchemas.readAvroSchema` and
`normalizeStringTypes`). It is not raw Avro JSON equality or Parquet `MessageType` equality.

Compare recursively:

- Field names (exact spelling/case), field order, primitive/container types, and nullability at every level.
- Non-null union alternatives, ignoring union branch order; distinguish nullable and non-nullable fields.
- Logical types and their semantic parameters, including decimal precision/scale and timestamp unit/local-vs-UTC
  distinctions. Match array elements, map values, fixed sizes, and enum symbols/order where supported.

Ignore record names/namespaces, documentation, aliases, default values, and non-semantic key-value metadata
such as Hugging Face feature annotations and writer metadata. Differences in those properties alone are allowed.
Do not ignore logical-type metadata that changes value interpretation. Different field order, missing/additional
fields, numeric widening, or nullability changes are incompatible; schema evolution or coercion is not in scope.
Each file still uses its own physical schema/footer when opening its Parquet reader.
Accepting different record names does not imply that parquet-avro can use the first file's Avro read schema
with every shard. Requirement 7 requires per-file projection/read schemas, including nested record names.

`listTables()` and `resolveTable()` perform filesystem/name discovery only and must not read shard footers;
`resolveTable()` currently calls `listTables()` for every table resolution. This is still catalog-wide and
fail-fast for discovery errors under requirement 4; lazy footer checks do not make filename errors local.
Validate lazily when a selected logical table is opened/planned for execution or its column/schema metadata
is requested. Direct `getColumns` calls validate every shard of each table matching their catalog/schema/table
arguments; `/describe` and table execution validate their inputs. Do not validate all tables during an ordinary
unrelated filesystem-table query. Listing names through `listTables` alone reads no footers and does not
validate schemas. Reuse validated `ParquetTable` instances within a prepared statement, including join
planning, schema inference, and repeated executions; new prepared statements validate independently.
`getColumns` discovers the catalog once and uses each matching `TableLocation` directly. `/describe` relies
on `getColumns` for validation and does not repeat it before reading aggregate footer diagnostics.

`getTables` is an explicit exception to full validation: it always reads remarks and JDBC provides no switch
to omit them. Read remarks from the first file only, without checking the other shards or validating their
schemas. The same rule applies to `information_schema.tables` and CLI `/list`, which call `getTables` with
`%`. These listings may read one footer per matching logical table (unless cached), not one per shard;
they do not certify schema compatibility. A schema mismatch in another shard must not fail these listings.
An unreadable first-file footer can still fail a remarks read under the existing metadata error handling;
this exception does not suppress actual I/O failures or catalog-wide filename/discovery errors.

Column listings deliberately have broader validation scope. `getColumns(null, null, "%", "%")` (or the
equivalent current-catalog call) validates every shard of every matching table. `InformationSchemaColumns.load`
always calls `getColumns(catalog, null, "%", "%")`, and `materializeInformationSchemaColumns` loads that whole
result before applying any SQL `WHERE`. Consequently, **every `information_schema.columns` query validates
the entire catalog**, including `SELECT * FROM information_schema.columns WHERE table_name = 'train'` when
`train` is healthy. One incompatible table makes that whole query, and a wildcard `getColumns` call, fail;
do not return partial column metadata. A direct `getColumns` call limited to the healthy schema/table can
still succeed. Pushing SQL metadata filters into `InformationSchemaColumns.load` is outside this feature.

Without an optional schema-validation cache, each such broad column request reads all shard footers across
all matched tables. The materialized information-schema result is cached only within one prepared statement;
it does not prevent a new statement from repeating the catalog-wide work. An optional validation cache must
follow the invalidation rules below. This cost and failure scope are accepted for this feature and must be
documented alongside the cheaper `getTables` exception.

For execution and column/schema metadata, validate **all** files before returning rows or that metadata,
so a mismatch in a later shard cannot produce a partial successful result. Use the first file's validated
logical schema as the representative schema.
On mismatch, throw `SQLException` identifying the logical table, reference file, offending file, and differing
field path/type/nullability. Unreadable or missing files also fail explicitly; never silently skip a shard.
Footer handles must be closed. Caching is optional; any cache must invalidate on file membership, resolved
target identity/path, target size, or target modification-time changes, not just changes to the first file.
For symlinks, size and modification time are those of the link target, not the link itself. Retargeting a link
must invalidate even if the replacement target has the same size and modification time. No persistent cache
is required for this feature.

### 7. Reading, pushdown, and relational behavior

Read a multi-file table through a `RecordReader` that opens one `ParquetReader` at a time, closes it at EOF,
and advances to the next file. Empty shards are allowed. Closing the logical reader, query failure, or
cancellation closes the active reader; do not keep every shard reader open simultaneously. Cancellation must
return without waiting for a physical read already in progress. Register cancellation before execution and
share it across nested work, including aggregates, sorting, joins, CTEs, and bound-parameter replanning. That
read owns cleanup when it returns; a canceled `executeQuery()` or in-flight JDBC `next()` must throw
`SQLException` rather than return a partial result or report normal end of results. With a pushed-down
record filter, one physical read can span the rest of a shard; do not close a decoder while it is in use.

Compute the required logical column paths once per logical input, but build the Avro requested projection
and Avro read schema **separately for each file**, from that file's own Avro schema. Use a fresh copy of the
per-input `Configuration` for each reader and replace both `AvroReadSupport.setRequestedProjection` and
`setAvroReadSchema` with that shard's projected schema. Preserve its top-level and nested record names,
namespaces, and read-relevant properties while selecting the same logical paths. With projection pushdown
disabled (or SELECT *), do not leave the first file's projection/read-schema overrides in later readers;
use each file's full schema/default reader behavior. Do not mutate the shared input configuration or assume
parquet-avro tolerates different nested record names in a reused read schema. The first file's normalized
schema remains the representative metadata schema, not a universal per-shard read schema.

Create a **new `ParquetRecordReaderAdapter` for each shard** around that shard's `ParquetReader`, passing that
shard's own enriched **full** schema from `ParquetSchemas.readAvroSchema` as the adapter's second argument.
Do not pass the first file's representative schema, the projected read schema, or a metadata-only normalized
schema in its place. The full enriched schema preserves logical annotations such as float16 and nanosecond
timestamps. The adapter derives its projected schema from the first record and compares field schemas using
`Schema.equals`, which includes record names; reusing another shard's schema can therefore trigger copies
with the wrong record names on every row. Adapter state must never carry across a shard boundary. Any copying
needed to restore a shard's logical annotations still uses that shard's own full schema. This applies with
projection pushdown both enabled and disabled, including the join-input reader factory.

Apply equivalent projection choices and Parquet filter configuration, including row-group/page-index pruning,
to every file. A join input uses its own planned predicate across all its shards. Preserve residual evaluation,
NULL/NaN semantics, and the existing rule that sampled inputs disable predicate pushdown. Apply `TABLESAMPLE`
to the concatenated logical input, not independently per shard; a fixed shard order and seed must give a
repeatable logical sample. Apply query-wide LIMIT/OFFSET, DISTINCT, aggregates, windows, and final ordering to
the logical table across shard boundaries, not separately to each file. Joins, subqueries, CTEs, set operations,
and derived/correlated queries must all use this same logical input behavior.

### 8. Metadata and CLI diagnostics

`DatabaseMetaData.getTables`, `information_schema.tables`, and CLI `/list` show one logical table without
exposing constituent shard tables. They read first-file table remarks without full schema validation, even
when listing all tables. `DatabaseMetaData.getColumns` reports one validated set of columns after checking
every shard of every table matching its metadata arguments. `information_schema.columns` requests all tables
and validates the entire catalog before SQL filtering; an unrelated mismatch therefore fails even a query
filtered to a healthy table. `listSchemas` follows requirement 2's
directory classification and reads no footers. Table and column remarks/type hints come from the first file
in numeric order; non-semantic metadata differences in later files do not change them. Column metadata and
`/describe` require full validation; table-name/remarks listings do not.

CLI `/describe` selects **one exact logical table**, not a JDBC metadata pattern. Resolve the input with
`resolveTable(schema, table)` first, including the default `PUBLIC` schema when none is supplied. For
`getColumns`, use that resolved location's schema and table names as escaped literal patterns: obtain the
escape string from `DatabaseMetaData.getSearchStringEscape()` (currently `\`), escape the escape character
itself first, then escape every `_` and `%`. Pass the resulting explicit schema/table patterns and `%` only
for the column-name pattern. Do not pass an unescaped user name or a null schema that can select same-named
tables across schemas. Use the same resolved `TableLocation` for the footer summary. Filtering metadata rows
after an overbroad `getColumns` call is insufficient: that call can already have validated and failed on an
unrelated table. Exact selection must happen before column validation. This prevents both mixed-up columns
and schema-mismatch errors from tables whose names merely match wildcard characters in the requested name.
These CLI literal-name fixes also apply in `datasetLayout=none`.

The CLI currently splits `/describe` input at the **first** dot into schema and table; subsequent dots belong
to the table name. Preserve this syntax: `/describe data.train.v2` means schema `data`, table `train.v2`.
For a dotted table in `PUBLIC`, require `/describe PUBLIC.train.v2`, including when the URL points at `data/`.
`/describe train.v2` instead selects schema `train`, table `v2`; it is not an unqualified reference to
`PUBLIC.train.v2`. Document this limitation alongside the SQL dotted-name limitation without locking its
failure into a regression assertion. CLI examples use these plain command arguments, not SQL quote syntax.
An unqualified hyphenated name such as `/describe train-extra` needs no schema unless a nondefault schema
is intended. Extending CLI parsing to recognize quoted unqualified dotted identifiers is outside this feature.

CLI `/describe` reports the number of files and sums row-group counts and unfiltered footer row counts across
all files. Report page-index status across all column chunks in all shards:

- `present`: at least one column chunk exists and every chunk has both column and offset indexes.
- `absent`: no column chunk has either index, including a table with no row groups/chunks.
- `partial`: any other combination, including fully indexed and unindexed shards mixed together, partially
  indexed files, or chunks with only one of the two indexes.

Extend the footer diagnostics as needed to distinguish these cases; the original `ParquetFileInfo.pageIndexPresent()`
Boolean cannot distinguish absent from partial. `/describe` must not report a logical table as fully indexed
because only its first shard is indexed. Diagnostic totals describe the entire table before filters, sampling,
or LIMIT/OFFSET.

## Tests and fixtures

- Extend `tools/createHfOptimizedData.py`: it currently writes single-file fixtures only. Write committed fixtures
  in `src/test/resources/hf-layout/` for named shards (with and without hashes), auto-converted numeric-directory
  layouts, an equivalent single-file table, and a schema-mismatch pair. Include CDC/page-index-enabled shards,
  a plain unindexed shard for partial-index diagnostics, metadata/record-name-only schema differences that are
  accepted, and empty shards. Document the existing PyArrow version and regeneration command. Maven must not
  require Python. Tiny filesystem naming/error fixtures may be assembled from these files in Java temporary
  directories rather than duplicating binary resources.
- Cover every URL-base example in requirement 2, including pointing at `default/`, the unsupported dataset-root
  `data/config/split` depth, and the supported `data/` base alternative. Assert agreement of SQL resolution,
  `listTables`, `listSchemas`, JDBC metadata, `information_schema`, and CLI output.
- Test ignored README/.gitattributes files, empty directories, nonnumeric Parquet files, and a `partial-train/`
  child directory. Verify both directory eligibility and the specified ordinary-schema fallback at the base.
- Test dot-prefixed Parquet files and directories at each supported depth, including `.cache/huggingface/`
  at the root and `.ipynb_checkpoints/` inside an otherwise eligible table. They must neither create schemas/
  tables nor disqualify tables or participate in collisions/completeness checks in `hf` mode. Preserve `none`.
- Create file and directory symlink fixtures in Java temporary directories on supported filesystems, including
  visible `.parquet` links to extensionless blobs outside the base and through dot-prefixed target paths.
  Test discovery, schema checks, query results, and broken `.parquet` link errors. A dangling non-Parquet
  `README.md` link must be ignored and must not disqualify an otherwise eligible directory table.
  If caching is implemented, modify
  only a target's contents/size/mtime and test retargeting to a different same-size/same-mtime file; do not rely
  on changes to link metadata. Skip symlink-specific cases only when link creation is unavailable, with an
  explicit reason; ordinary layout tests must still run.
- Test exact shard grammar, optional hashes, unpadded numeric order (`9` before `10`), numeric-directory gaps,
  duplicate padded indices, missing named shards, invalid indices/totals, and disagreeing totals.
- Test all collision combinations in requirement 4, including case-insensitive schema/table collisions and
  distinct-case names under `caseSensitive=true` where the filesystem supports them. Include `Data/` versus
  `data/` when one or both contain only README files or are empty. Assert diagnostic sources.
- Verify catalog-wide discovery failure: with a healthy `default.train` table and an unrelated incomplete
  shard group under `data/`, resolving/querying `default.train`, table/schema listings, and catalog metadata
  must fail with the broken group's diagnostic. Repeat with an unrelated visible dangling `.parquet` link;
  repair/remove it and verify the same connection succeeds. An unrelated dangling `README.md` link or hidden
  broken link must not prevent the query. Schema-mismatch footers in an unrelated complete group must remain
  lazy and must not fail the healthy table query.
- With a healthy table and an unrelated complete group whose later shard has an incompatible schema,
  `getTables("%")`, CLI `/list`, and `SELECT ... FROM information_schema.tables` must succeed and list both
  logical tables with first-file remarks. A counting test seam must show these listings do not read later
  shard footers or invoke full schema validation. In contrast, direct `getColumns` for the incompatible
  group, `/describe` of it, and executing it must fail with the schema-mismatch diagnostic. Also assert that
  `getColumns(null, null, "%", "%")` and
  `SELECT * FROM information_schema.columns WHERE table_name = 'train'` fail when `train` is the healthy
  table: the information-schema view loads all columns before its WHERE. A direct `getColumns` call restricted
  to the healthy schema/table and an ordinary query against that healthy table must still succeed.
  Keep first-file footer I/O errors distinct from schema incompatibility.
- Verify `TableLocation` list immutability, nonempty/non-null validation, the old single-file constructor, and
  the first-file accessor. Verify all schema and execution consumers use the logical file list, especially joins.
- Compare row counts and exact query results with the equivalent single-file data under all four independent
  predicate/projection pushdown settings. Include filters that eliminate an entire shard, cross-shard bounds,
  aggregates, DISTINCT, windows, join inputs, subqueries/CTEs, set operations, repeatable sampling, empty results,
  and stable metadata before iteration and after exhaustion.
- Demonstrate physical row-group/page pruning on **each** indexed shard, in addition to correct query results.
  The optimized-Parquet Phase 2 helper is currently the private `filteredCount` method in
  `HfOptimizedParquetTest`; extract it into a shared test utility accepting a file/path before reusing it here,
  and keep the original tests using that utility. Include a query whose matches are only in a later shard.
- Test schema checks for field names/order, primitive/logical types, nested fields, and nullability, including
  a mismatch in a later shard. Assert no rows/metadata are returned before failure. Use a counting test seam
  to prove `listTables`, `listSchemas`, and `resolveTable` read no footers, that table listings read only
  first-file remarks, and that execution/column-metadata access validates all shards of each requested table.
  On an entirely compatible catalog, assert broad `getColumns` and a healthy-table-filtered
  `information_schema.columns` query check every table's shards. If no validation cache is introduced, verify
  a new statement repeats the footer work rather than reusing another statement's materialized result.
  Exercise cache invalidation if a cache is introduced.
- Include shards whose only schema differences are top-level and **nested** Avro record names/namespaces.
  With projection pushdown enabled, select a subset of top-level columns that includes a nested record column;
  verify complete values, metadata, and pruning. Repeat with pushdown disabled and through a join input.
  Inspect the per-file reader configuration through a test seam to verify its requested projection/read schema
  uses that shard's record names and that the shared configuration is not mutated. Also inspect adapter
  creation to assert a distinct adapter per shard with that shard's enriched full schema, never the first
  file's representative schema. Check returned records retain that shard's top-level/nested record names and
  enriched logical types, including float16/nanosecond timestamp annotations. Allow copies needed for actual
  enrichment, but do not copy records merely to substitute the first shard's record names. Do not treat a
  successful full-column read as evidence that projected reads with differing record names work.
- Test `/describe` summed rows/row groups, file counts, and `present`/`absent`/`partial` index status, including
  mixed shards and an entirely empty table. Test first-file remarks and column metadata precedence.
- Place `data.train_a`, `data.train-a`, and `data.train.a` side by side with distinguishable column schemas
  and footer totals. `/describe data.train_a` must print only its own columns and summary. Give `train-a`
  incompatible later shards: `/describe data.train_a` must still succeed, and a counting seam must prove
  it does not validate the other table's shard schemas. Describing `data.train-a` itself must fail with its
  mismatch diagnostic. Repeat literal-name selection with `%` and the metadata escape character in names,
  using ordinary single-file/directory table names where the split grammar excludes those characters.
  If the filesystem cannot represent the escape character in a filename, test that escaping through a
  metadata-pattern test seam instead.
  Include the same table name in another schema to verify an unqualified description uses only `PUBLIC`.
  Cover both dataset layouts and the applicable `caseSensitive` matching rules.
- Test `/describe data.train.v2` for schema `data` and `/describe PUBLIC.train.v2` for a `PUBLIC` dotted
  table when the URL points at `data/`. Assert exact columns and footer totals; keep the unqualified dotted
  command limitation in the README rather than requiring it to fail forever. Preserve unqualified
  `/describe train-extra` for a default-schema hyphenated name.
- Verify `datasetLayout` absent and `none` preserve existing behavior; accept mixed-case values such as `HF`
  and `None`, and reject empty, whitespace-padded, or unknown values with the specified connection error.
  Test dotted split names with schema-qualified SQL, including `PUBLIC."train.v2"`. Keep the existing
  unqualified-dotted parser limitation in the README rather than requiring a failure assertion that would
  break when the bug is fixed. Test unqualified `FROM "train-extra"` and `FROM "train-extra" t`, as well as
  the schema-qualified hyphenated forms. Add mixed-case listed schema/table names: in `caseSensitive=false`,
  lowercase quoted references resolve and original-case quoted
  references fail; in `caseSensitive=true`, the original spelling resolves and a different case fails.
  Include the quoted `"public"` versus `"PUBLIC"` distinction, and cover joins and IN subqueries as well as
  ordinary SELECT. Retain existing single-file query, metadata, and CLI tests.

## Documentation and completion

- README: document `datasetLayout=none|hf`, its default/error behavior, exact URL base examples, supported depth,
  shard grammar/completeness, directory eligibility, symlink following, dot-entry exclusion, collisions, and
  logical-table diagnostics and catalog-wide fail-fast discovery errors (contrasted with lazy per-table footer
  validation). Explain that `getTables`, `/list`, and `information_schema.tables` read first-file remarks only,
  while execution and `/describe` validate their inputs and `getColumns` validates all matching tables.
  Explicitly state that wildcard `getColumns` and every `information_schema.columns` query validate the
  entire catalog before SQL WHERE, cost a footer read per shard without a validation cache, and fail if any
  included table is incompatible, even when the SQL filter names a healthy table. State that property values are
  case-insensitive. Show schema-qualified dotted split names (e.g. `data."train.v2"`, `PUBLIC."train.v2"`),
  explicitly document the unqualified dotted-name limitation, and show that unqualified quoted hyphenated
  names work with and without aliases. Explain that `/describe` takes literal names, escapes JDBC metadata
  wildcards, and requires `PUBLIC.train.v2` for a dotted table in the default schema; show
  `/describe data.train.v2` and `/describe PUBLIC.train.v2` without SQL quote syntax. Explain lowercasing
  quoted identifiers with `caseSensitive=false`,
  exact spelling with `caseSensitive=true`, and the resulting rules for SQL generated from listed names. Replace the
  current sentence at lines 141–142 stating that dataset shards are separate tables and automatic grouping is
  outside the feature; describe opt-in grouping and retain the single-file default explicitly.
- `release.md`: add a bullet for multi-file table support and the `datasetLayout` property under the upcoming
  version section (create that section if the optimized-Parquet release has already been published).
- Javadoc: document new/changed classes and methods, `TableLocation` compatibility accessors/constructors,
  validation timing, and resource ownership. Keep American English and the repository's design/style rules.
- Run focused layout, metadata, join, CLI, and pruning tests, then full `mvn verify` on Java 21. Follow `AGENTS.md`'s
  skip-flags diagnostic requirement if verification fails, and finish with an unskipped passing build with no
  Checkstyle, PMD, or Spotless violations. Verification results are recorded below.

## Implementation and verification

Completed on 2026-10-18. `HfTableDiscovery` performs bounded, footer-free discovery; `ParquetTable`
validates logical schemas and owns sequential per-shard readers, configurations, and adapters. All SELECT,
join, schema-inference, column-metadata, and CLI paths use the logical file list. No shared schema-validation
cache was introduced. Prepared plans retain their validated inputs, column metadata uses one discovery
pass, and CLI diagnostics reuse column validation. Mismatch diagnostics name the first differing field path
and type/nullability; schema compatibility remains the acceptance gate even without a diagnostic.
Cancellation is published without waiting for an active physical read. An in-flight JDBC `next()` reports
`SQLException` once that read returns, including when it returns a matching row or shard EOF. The reader
closes its input before opening another shard; a pushed-down filter can make one physical read span the rest
of a shard. Physical closure remains serialized with reads. Execution cancellation is registered before
planning/materialization, shared across nested work, and reset for each new top-level execution. Result-set
closure is idempotent; identity-bearing system-function context handles never pop a different thread's live
context and closed handles are pruned on the owning thread's next context access. Cancellation exceptions use
SQLState `57014`; JDBC iteration unwraps evaluator/subquery causes to rethrow the original marked exception,
including correlated subqueries started after the original execution scope has ended. These subqueries
inherit the outer cancellation state and never replace the outer statement's current result; cancellation
between correlated EXISTS or IN calls therefore reaches the live outer result. The existing
quoted-name casing and unqualified dotted-name limitations remain as specified, and the default layout
preserves single-file discovery.

`mvn spotless:apply clean verify` passed on Java 21: 1,126 unit tests (one case-sensitive-filesystem test
skipped on the case-insensitive local volume), plus all 3 integration tests. Checkstyle reported zero
violations; PMD, Spotless, and coverage checks passed. The new layout tests cover grouping, errors, symlinks,
all four pushdown settings, independent physical pruning on both indexed shards, record names/enriched types,
metadata validation scope, exact CLI patterns, aggregated diagnostics, cancellation, and resource cleanup.
Committed PyArrow 21.0.0 fixtures and their generator keep Python out of the Maven build.
