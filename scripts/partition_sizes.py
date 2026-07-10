"""Dump per-partition sizes for a HATS catalog to a CSV, without loading row data.

For each leaf partition (one parquet file) this records three different "sizes", all
read from the parquet *footers* — no data pages are touched:

  * uncompressed_bytes  — sum of each row group's ``total_byte_size`` (the encoded,
    uncompressed size). This is the cheapest proxy for the in-memory footprint that
    ``byte_pixel_threshold`` partitions on, but it is NOT that exact number: hats's
    ``get_mem_size_per_row`` is a per-Python-object estimate (sys.getsizeof + ndarray
    .nbytes) and can only be reproduced by materializing rows. Expect this column to
    run *smaller* than the mem-size threshold the partitioner targeted.
  * compressed_bytes    — sum of each column chunk's ``total_compressed_size`` (the
    on-the-wire parquet size, before the file's own framing).
  * file_bytes          — the actual size of the ``.parquet`` file on disk (st_size).

Fast path reads the catalog's single ``dataset/_metadata`` file (all row-group metadata
in one shot). If that file is absent — e.g. the catalog was written with
``create_metadata=false`` — it falls back to reading each partition file's footer
individually (still no data, and parallelized across files).

    python scripts/partition_sizes.py /sdf/.../hats/dp2_memory_1gib/object_lc \
        -o object_lc_partition_sizes.csv
"""
from __future__ import annotations

import argparse
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# HATS lays partitions out as dataset/Norder=<k>/Dir=<d>/Npix=<n>.parquet
_NORDER_RE = re.compile(r"Norder=(\d+)")
_NPIX_RE = re.compile(r"Npix=(\d+)")


def _order_pixel(rel_path: str) -> tuple[int | None, int | None]:
    o = _NORDER_RE.search(rel_path)
    n = _NPIX_RE.search(rel_path)
    return (int(o.group(1)) if o else None, int(n.group(1)) if n else None)


def _from_metadata(dataset_dir: Path) -> list[dict]:
    """Aggregate every row group in dataset/_metadata by its partition file."""
    md = pq.read_metadata(str(dataset_dir / "_metadata"))
    by_file: dict[str, dict] = {}
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        rel = rg.column(0).file_path  # relative to dataset_dir
        rec = by_file.setdefault(
            rel, {"path": rel, "num_rows": 0, "uncompressed_bytes": 0, "compressed_bytes": 0}
        )
        rec["num_rows"] += rg.num_rows
        rec["uncompressed_bytes"] += rg.total_byte_size
        rec["compressed_bytes"] += sum(
            rg.column(j).total_compressed_size for j in range(rg.num_columns)
        )
    return list(by_file.values())


def _read_footer(path: Path, dataset_dir: Path) -> dict:
    m = pq.read_metadata(str(path))
    uncompressed = compressed = 0
    for i in range(m.num_row_groups):
        rg = m.row_group(i)
        uncompressed += rg.total_byte_size
        compressed += sum(rg.column(j).total_compressed_size for j in range(rg.num_columns))
    return {
        "path": str(path.relative_to(dataset_dir)),
        "num_rows": m.num_rows,
        "uncompressed_bytes": uncompressed,
        "compressed_bytes": compressed,
    }


def _from_footers(dataset_dir: Path, workers: int) -> list[dict]:
    """Fallback: read each partition file's footer directly (no _metadata present)."""
    files = sorted(dataset_dir.rglob("*.parquet"))
    if not files:
        raise SystemExit(f"No .parquet partition files found under {dataset_dir}")
    logger.info("No _metadata; reading %d partition footers directly.", len(files))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(lambda f: _read_footer(f, dataset_dir), files))


def _add_file_sizes(records: list[dict], dataset_dir: Path, workers: int) -> None:
    """Stat each partition file's on-disk size, in parallel, and set rec['file_bytes'].

    Runs for both paths: the _metadata reader and the footer fallback each yield records
    without a file size, and stats over shared storage are I/O-bound, so a thread pool
    (GIL released during stat) speeds this up regardless of how the records were built.
    """
    def _stat(rec: dict) -> None:
        rec["file_bytes"] = (dataset_dir / rec["path"]).stat().st_size

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(_stat, records))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("catalog", type=Path, help="Path to a HATS catalog directory")
    parser.add_argument(
        "-o", "--output", type=Path, default=None, help="Output CSV path (default: <catalog>_partition_sizes.csv)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Threads for parallel file stats and the footer-fallback reads (default: 16)",
    )
    args = parser.parse_args()

    dataset_dir = args.catalog / "dataset"
    if not dataset_dir.is_dir():
        parser.error(f"No dataset/ directory under {args.catalog} — is this a HATS catalog?")

    if (dataset_dir / "_metadata").exists():
        logger.info("Reading %s", dataset_dir / "_metadata")
        records = _from_metadata(dataset_dir)
    else:
        records = _from_footers(dataset_dir, args.workers)

    # Actual on-disk file size, one stat per partition file, parallelized.
    _add_file_sizes(records, dataset_dir, args.workers)

    df = pd.DataFrame.from_records(records)
    # Add order/pixel parsed from the path, and MiB convenience columns.
    df[["Norder", "Npix"]] = df["path"].apply(lambda p: pd.Series(_order_pixel(p)))
    for col in ("uncompressed", "compressed", "file"):
        df[f"{col}_mib"] = (df[f"{col}_bytes"] / (1024 * 1024)).round(2)

    df = df.sort_values(["Norder", "Npix"], na_position="last").reset_index(drop=True)
    df = df[
        [
            "Norder",
            "Npix",
            "num_rows",
            "uncompressed_bytes",
            "uncompressed_mib",
            "compressed_bytes",
            "compressed_mib",
            "file_bytes",
            "file_mib",
            "path",
        ]
    ]

    out = args.output or Path(f"{args.catalog.name}_partition_sizes.csv")
    df.to_csv(out, index=False)

    logger.info(
        "Wrote %d partitions to %s | rows=%d uncompressed=%.1f GiB compressed=%.1f GiB file=%.1f GiB",
        len(df),
        out,
        int(df["num_rows"].sum()),
        df["uncompressed_bytes"].sum() / 1024**3,
        df["compressed_bytes"].sum() / 1024**3,
        df["file_bytes"].sum() / 1024**3,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
