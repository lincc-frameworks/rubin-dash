"""Add objects dropped by the nesting stage's inner join back into a nested LC catalog.

The nesting stage's ``join_nested`` calls defaulted to ``how="inner"`` (fixed to
``"left"`` in stages/nesting.py), so objects with zero rows in *any* source catalog were
silently dropped from the nested light-curve catalogs — and therefore from everything
derived from them, including the uncertainty-corrected collections. The missing objects
have no sources, so they need no correction: their nested columns are simply missing
values. That makes the repair a pure merge:

1. Find the missing ids: (flat object catalog ids) minus (LC catalog ids).
2. Write "mini" parquet files holding the missing objects' base columns (taken verbatim
   from the flat catalog) with every LC-only column — the nested columns, including any
   ``*_corrected`` ones — filled with nulls (NA nested entries, matching what a
   ``how="left"`` join produces for zero-source objects).
3. Re-shuffle the LC partition files plus the mini files through hats-import
   (``reimport_from_hats`` with ``existing_pixels``), preserving the LC partitioning and
   adding new max-order pixels only where a missing object falls outside the current
   coverage. Row counts are validated end-to-end via ``expected_total_rows``.
4. Verify the output: total rows, and the nested-offsets consistency scan (all leaf
   columns of a nested column must carry equal value counts per file).

Afterwards: run the collections stage on the output to rebuild margin + index, and the
notebook 12 collection checks as the acceptance gate.

    python scripts/patch_missing_objects.py \
        --flat-path /sdf/.../hats/dp2/dia_object \
        --lc-path /sdf/.../hats/dp2-fixed/dia_object_collection_uncertainty_corrected/dia_object_lc \
        --id-column diaObjectId \
        --output-dir /sdf/.../hats/dp2-fixed/patched \
        --workdir /sdf/.../scratch/patch_dia --dry-run
"""
from __future__ import annotations

import argparse
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import hats
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from dask.distributed import Client
from hats.io.paths import HIVE_COLUMNS, get_common_metadata_pointer
from hats_import import pipeline_with_client
from hats_import.catalog import ImportArguments
from hats_import.catalog.file_readers import ParquetPyarrowReader

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SPATIAL = "_healpix_29"
SPATIAL_ORDER = 29


def partition_files(catalog_dir: Path) -> list[Path]:
    return sorted((catalog_dir / "dataset").rglob("*.parquet"))


def read_ids(files: list[Path], id_column: str, workers: int, label: str) -> np.ndarray:
    def one(f: Path) -> np.ndarray:
        return pq.read_table(f, columns=[id_column])[id_column].to_numpy()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        parts = list(ex.map(one, files))
    ids = np.concatenate(parts) if parts else np.array([], dtype=np.int64)
    logger.info("%s: %d files, %d rows", label, len(files), len(ids))
    return ids


def destination_pixels(lc_dir: Path, missing_h29: np.ndarray) -> tuple[list[tuple[int, int]], int]:
    """LC pixel list, extended with max-order pixels for rows outside current coverage."""
    pixels = hats.read_hats(lc_dir).get_healpix_pixels()
    by_order: dict[int, set[int]] = {}
    for p in pixels:
        by_order.setdefault(p.order, set()).add(p.pixel)
    max_order = max(by_order)

    covered = np.zeros(len(missing_h29), dtype=bool)
    for order, pix_set in by_order.items():
        cand = missing_h29 >> np.uint64(2 * (SPATIAL_ORDER - order))
        covered |= np.isin(cand, np.fromiter(pix_set, dtype=np.int64))
    new_pixels = sorted(
        set((missing_h29[~covered] >> np.uint64(2 * (SPATIAL_ORDER - max_order))).astype(np.int64).tolist())
    )
    if new_pixels:
        logger.info(
            "%d missing objects fall outside current coverage -> %d new order-%d pixel(s)",
            int((~covered).sum()),
            len(new_pixels),
            max_order,
        )
    existing = [(p.order, p.pixel) for p in pixels] + [(max_order, px) for px in new_pixels]
    return existing, max_order


def write_mini_files(
    flat_files: list[Path],
    missing_sorted: np.ndarray,
    id_column: str,
    schema: pa.Schema,
    workdir: Path,
    workers: int,
) -> tuple[list[Path], int]:
    """One mini parquet file (full LC schema) per flat partition that holds missing objects.

    Base columns are copied from the flat catalog; columns absent there (the nested
    columns, corrected or not) become all-null entries.
    """
    workdir.mkdir(parents=True, exist_ok=True)

    def id_range_overlaps(f: Path) -> bool:
        md = pq.read_metadata(f)
        lo = hi = None
        for rg_i in range(md.num_row_groups):
            for c_i in range(md.row_group(rg_i).num_columns):
                col = md.row_group(rg_i).column(c_i)
                if col.path_in_schema == id_column and col.statistics and col.statistics.has_min_max:
                    st = col.statistics
                    lo = st.min if lo is None else min(lo, st.min)
                    hi = st.max if hi is None else max(hi, st.max)
        if lo is None:
            return True  # no stats — cannot prune
        left = np.searchsorted(missing_sorted, lo, side="left")
        return left < len(missing_sorted) and missing_sorted[left] <= hi

    def build_one(args: tuple[int, Path]) -> tuple[Path | None, int]:
        i, f = args
        if not id_range_overlaps(f):
            return None, 0
        pf = pq.ParquetFile(f)
        kept: list[pa.Table] = []
        for rg_i in range(pf.metadata.num_row_groups):
            tbl = pf.read_row_group(rg_i)
            mask = np.isin(tbl[id_column].to_numpy(), missing_sorted, assume_unique=False)
            if mask.any():
                kept.append(tbl.filter(pa.array(mask)))
        if not kept:
            return None, 0
        rows = pa.concat_tables(kept)
        arrays = []
        for field in schema:
            if field.name in rows.column_names:
                arrays.append(rows[field.name].combine_chunks().cast(field.type))
            else:
                arrays.append(pa.nulls(len(rows), type=field.type))
        out_tbl = pa.Table.from_arrays(arrays, schema=schema)
        out = workdir / f"missing_{i:05d}.parquet"
        pq.write_table(out_tbl, out)
        return out, len(out_tbl)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(build_one, enumerate(flat_files)))
    files = [f for f, _ in results if f is not None]
    total = sum(n for _, n in results)
    logger.info("wrote %d mini file(s), %d rows, under %s", len(files), total, workdir)
    return files, total


def verify_output(catalog_dir: Path, expected_rows: int, nested_columns: list[str]) -> bool:
    md = pq.read_metadata(catalog_dir / "dataset" / "_metadata")
    ok = md.num_rows == expected_rows
    logger.info("output rows: %d (expected %d) %s", md.num_rows, expected_rows, "OK" if ok else "MISMATCH")

    counts: dict[tuple[str, str], dict[str, int]] = {}
    for rg_i in range(md.num_row_groups):
        rg = md.row_group(rg_i)
        fp = rg.column(0).file_path
        for c_i in range(rg.num_columns):
            col = rg.column(c_i)
            parts = col.path_in_schema.split(".")
            if parts[0] in nested_columns and len(parts) > 1:
                per = counts.setdefault((fp, parts[0]), {})
                per[parts[1]] = per.get(parts[1], 0) + col.num_values
    bad = {k: v for k, v in counts.items() if len(set(v.values())) > 1}
    for (fp, ncol), fields in list(bad.items())[:5]:
        logger.error("nested column %s inconsistent in %s: %s", ncol, fp, fields)
    logger.info("nested-offsets consistency: %s", "OK" if not bad else f"{len(bad)} bad (file, column) pairs")
    return ok and not bad


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--flat-path", type=Path, required=True, help="Flat object catalog (ground truth ids)")
    parser.add_argument("--lc-path", type=Path, required=True, help="Nested LC catalog missing the objects")
    parser.add_argument("--id-column", required=True, help="Object id column (e.g. diaObjectId)")
    parser.add_argument("--output-dir", type=Path, required=True, help="Where to write the merged catalog")
    parser.add_argument("--workdir", type=Path, required=True, help="Scratch dir for the mini files")
    parser.add_argument("--n-workers", type=int, default=16, help="Dask workers for the merge (default: 16)")
    parser.add_argument("--memory-limit", default="12GB", help="Dask worker memory limit (default: 12GB)")
    parser.add_argument("--threads", type=int, default=16, help="Threads for id/footer reads (default: 16)")
    parser.add_argument("--dry-run", action="store_true", help="Report missing objects and exit")
    args = parser.parse_args()

    flat_files = partition_files(args.flat_path)
    lc_files = partition_files(args.lc_path)

    flat_ids = read_ids(flat_files, args.id_column, args.threads, f"flat {args.flat_path.name}")
    lc_ids = read_ids(lc_files, args.id_column, args.threads, f"lc {args.lc_path.name}")

    stray = ~np.isin(lc_ids, flat_ids)
    if stray.any():
        logger.error("%d LC ids are not in the flat catalog — this is not an inner-join deficit; aborting",
                     int(stray.sum()))
        return 1
    missing_mask = ~np.isin(flat_ids, lc_ids)
    n_missing = int(missing_mask.sum())
    logger.info("missing objects: %d (flat %d - lc %d)", n_missing, len(flat_ids), len(lc_ids))
    if n_missing == 0 or args.dry_run:
        return 0

    # spatial coverage for the missing rows
    flat_h29 = read_ids(flat_files, SPATIAL, args.threads, "flat _healpix_29").astype(np.uint64)
    existing_pixels, max_order = destination_pixels(args.lc_path, flat_h29[missing_mask])

    schema = pq.read_schema(get_common_metadata_pointer(args.lc_path))
    missing_sorted = np.sort(flat_ids[missing_mask])
    mini_files, mini_rows = write_mini_files(
        flat_files, missing_sorted, args.id_column, schema, args.workdir, args.threads
    )
    if mini_rows != n_missing:
        logger.error("mini files hold %d rows, expected %d; aborting before the merge", mini_rows, n_missing)
        return 1

    column_names = [n for n in schema.names if n not in HIVE_COLUMNS]
    expected_total = len(lc_ids) + n_missing
    merge_args = ImportArguments.reimport_from_hats(
        args.lc_path,
        args.output_dir,
        input_file_list=[*lc_files, *mini_files],
        # row-group-wise reads: smaller buffers, and avoids the whole-file batch
        # assembly path implicated in the coord_dec offsets corruption (notebook 11)
        file_reader=ParquetPyarrowReader(column_names=column_names, iterate_by_row_groups=True),
        existing_pixels=existing_pixels,
        highest_healpix_order=max_order,
        expected_total_rows=expected_total,
        resume=False,
    )

    logger.info("merging %d lc + %d mini files -> %s", len(lc_files), len(mini_files), args.output_dir)
    with Client(n_workers=args.n_workers, memory_limit=args.memory_limit, threads_per_worker=1) as client:
        pipeline_with_client(merge_args, client)

    nested_columns = [f.name for f in schema if pa.types.is_struct(f.type)]
    out_catalog = args.output_dir / args.lc_path.name
    if not verify_output(out_catalog, expected_total, nested_columns):
        return 1

    logger.info("Done. Merged catalog at %s", out_catalog)
    logger.info("Next: run the collections stage on it (margin + index), then the notebook 12 checks.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
