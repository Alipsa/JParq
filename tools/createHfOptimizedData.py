#!/usr/bin/env python3
"""Regenerate the small Hugging Face compatibility fixtures.

Requires exactly pyarrow==21.0.0 (CDC is experimental):
  python3 -m venv /tmp/jparq-arrow
  /tmp/jparq-arrow/bin/pip install pyarrow==21.0.0
  /tmp/jparq-arrow/bin/python tools/createHfOptimizedData.py

Maven uses the committed output and does not require Python.
"""

import json
import struct
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def create_table():
    """Build deterministic values with NULLs, nested media, and logical types."""
    size = 16384
    ids = range(1, size + 1)
    columns = {
        "id": pa.array(ids, type=pa.int64()),
        "n": pa.array([None if 2048 < (i - 1) % 8192 + 1 <= 4096 else i for i in ids], type=pa.int32()),
        "text": pa.array([None if i % 7 == 0 else f"prefix-{i:05d}" for i in ids]),
        "label": pa.array([i % 3 for i in ids], type=pa.int32()),
        "date32": pa.array([19000 + i for i in ids], type=pa.date32()),
        "ts_ms": pa.array([1700000000000 + i for i in ids], type=pa.timestamp("ms", "UTC")),
        "ts_us": pa.array([1700000000000000 + i for i in ids], type=pa.timestamp("us", "UTC")),
        "ts_ns": pa.array([1700000000000000000 + i for i in ids], type=pa.timestamp("ns", "UTC")),
        "ts_local": pa.array([1700000000000000 + i for i in ids], type=pa.timestamp("us")),
        "amount": pa.array([Decimal(i).scaleb(-2) for i in ids], type=pa.decimal128(12, 2)),
        "half": pa.Array.from_buffers(pa.float16(), size, [None, pa.py_buffer(struct.pack("<e", 1.5) * size)]),
        "items": pa.array([[i, i + 1] for i in ids], type=pa.list_(pa.int32())),
        "image": pa.array([{"bytes": b"\x89PNG", "path": "image.png"}] * size,
                          type=pa.struct([("bytes", pa.binary()), ("path", pa.string())])),
    }
    metadata = {"huggingface": {"dataset_info": {"features": {"image": {"_type": "Image"}}}}}
    return pa.table(columns).replace_schema_metadata({b"huggingface": json.dumps(metadata["huggingface"]).encode()})


def main():
    """Write and validate indexed CDC and ordinary Parquet files."""
    if pa.__version__ != "21.0.0":
        raise RuntimeError(f"Expected pyarrow==21.0.0, found {pa.__version__}")
    target = Path(__file__).resolve().parents[1] / "src/test/resources/hf"
    target.mkdir(parents=True, exist_ok=True)
    table = create_table()
    for optimized in (False, True):
        path = target / ("optimized.parquet" if optimized else "plain.parquet")
        pq.write_table(table, path, row_group_size=8192, data_page_size=4096, write_batch_size=128,
                       write_page_index=optimized,
                       use_content_defined_chunking={"min_chunk_size": 4096, "max_chunk_size": 16384}
                       if optimized else False)
        metadata = pq.read_metadata(path)
        assert metadata.num_row_groups >= 2, path
        assert path.stat().st_size <= 1024 * 1024, path
        for group in range(metadata.num_row_groups):
            for index in range(metadata.num_columns):
                column = metadata.row_group(group).column(index)
                assert column.has_column_index == optimized, (path, column.path_in_schema)
                assert column.has_offset_index == optimized, (path, column.path_in_schema)
        assert pq.read_table(path).equals(table), path
        print(f"{path.name}: {metadata.num_rows} rows, {metadata.num_row_groups} groups, {path.stat().st_size} bytes")


if __name__ == "__main__":
    main()
