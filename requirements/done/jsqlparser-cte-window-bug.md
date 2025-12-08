# CTE + Window Join Parse Failure (root cause in comment stripping)

Environment:
- JSqlParser 5.3

Context:
- The original failure surfaced after JParq stripped block comments. The stripper previously injected newlines for every line inside a block comment, yielding a comment-free SQL that JSqlParser rejected with `ParseException: Encountered unexpected token: "," "," ... Was expecting: ")"` at the comma after `salary`.

Current status:
- After fixing comment stripping, `CCJSqlParserUtil.parse` succeeds on both the original query **with** the block comment and the comment-free form.
- JParq now strips block comments without inserting blank lines and retries parsing with whitespace compaction before applying the existing derived-table fallback as a safety net.
