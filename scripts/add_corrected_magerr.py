"""Write a copy of an uncertainty-corrected catalog with a corrected magnitude-error column added.

The uncertainty-correction stage originally emitted only ``psfFluxErr_corrected``; this
derives ``psfMagErr_corrected`` from it, using the exact formula the postprocess stage
uses for the raw MagErr columns: half the AB-magnitude spread at flux +/- err,

    magErr = -(AB(flux + err) - AB(flux - err)) / 2

stored as float32. (Future pipeline runs emit this at correction time via the
``output_mag_err_column`` config field; this script backfills catalogs that already
exist.)

The input catalog is never touched: each partition file is read, the nested subcolumn
appended, and the result written to ``<output-dir>/<catalog-name>`` mirroring the HATS
layout. Rows never move, so partitioning and sort order carry over; ancillary files
(properties, partition_info, skymaps, ...) are copied verbatim and dataset metadata is
regenerated at the end. Files already carrying the column (or lacking the nested column
entirely) are copied unchanged; existing output files are skipped, making the script
resumable.

Simplest workflow: transform only the MAIN catalog, then run the collections stage on
the output — it rebuilds the margin cache and index from the transformed catalog, so
they inherit the new column without being transformed separately.

    python scripts/add_corrected_magerr.py \
        /sdf/.../dp2-fixed/patched_object/object_lc \
        --output-dir /sdf/.../dp2-fixed/patched_object_magerr \
        --nested-column objectForcedSource

Afterwards: collections stage on the output dir, then the notebook 12 npd read sweep
and footer scan as acceptance.
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import astropy.units as u
import nested_pandas as npd
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from hats.io.parquet_metadata import write_parquet_metadata
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def process_file(
    in_path: Path, out_path: Path, nested_column: str, flux_column: str, err_column: str, out_column: str
) -> str:
    if out_path.exists():
        return "skipped-existing"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".parquet.magerr_tmp")  # not *.parquet: never scanned as data

    schema = pq.read_schema(in_path)
    transformable = False
    if nested_column in schema.names:
        struct = schema.field(nested_column).type
        fields = {struct.field(i).name for i in range(struct.num_fields)}
        transformable = err_column in fields and out_column not in fields

    if not transformable:
        shutil.copy2(in_path, tmp)
        os.replace(tmp, out_path)
        return "copied-unchanged"

    nf = npd.read_parquet(in_path)
    flux = nf[f"{nested_column}.{flux_column}"]
    err = nf[f"{nested_column}.{err_column}"]
    # postprocess._append_mag_and_magerr, applied to the corrected error
    upper = u.nJy.to(u.ABmag, flux + err)
    lower = u.nJy.to(u.ABmag, flux - err)
    nf[f"{nested_column}.{out_column}"] = pd.Series(
        (-(upper - lower) / 2).astype(np.float32), index=flux.index
    )

    nf.to_parquet(tmp)
    os.replace(tmp, out_path)
    return "written"


def process_catalog(catalog_dir: Path, output_dir: Path, args) -> bool:
    out_cat = output_dir / catalog_dir.name
    files = sorted((catalog_dir / "dataset").rglob("*.parquet"))
    if not files:
        logger.error("no partition files under %s", catalog_dir)
        return False
    logger.info("%s: %d partition files -> %s", catalog_dir, len(files), out_cat)

    # ancillary catalog files (properties, partition_info, skymaps, thumbnails, ...)
    out_cat.mkdir(parents=True, exist_ok=True)
    for item in catalog_dir.iterdir():
        if item.name == "dataset" or (out_cat / item.name).exists():
            continue
        if item.is_dir():
            shutil.copytree(item, out_cat / item.name)
        else:
            shutil.copy2(item, out_cat / item.name)

    def one(f: Path) -> str:
        out_path = out_cat / f.relative_to(catalog_dir)
        try:
            return process_file(f, out_path, args.nested_column, args.flux_column,
                                args.err_column, args.out_column)
        except Exception as e:  # noqa: BLE001 — report per-file, fail the catalog at the end
            logger.error("FAILED %s: %r", f, e)
            return "failed"

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        results = list(tqdm(ex.map(one, files), total=len(files), desc=catalog_dir.name,
                            unit="file", mininterval=5))

    tally = {status: results.count(status) for status in set(results)}
    logger.info("%s: %s", catalog_dir.name, tally)
    if tally.get("failed"):
        logger.error("%s: %d file(s) failed — re-run to retry (existing outputs are skipped)",
                     catalog_dir.name, tally["failed"])
        return False
    if tally.get("copied-unchanged"):
        logger.warning("%s: %d file(s) copied unchanged (already done, or no %s.%s) — expected only "
                       "when resuming or transforming non-main catalogs",
                       catalog_dir.name, tally["copied-unchanged"], args.nested_column, args.err_column)

    logger.info("%s: regenerating dataset metadata...", out_cat.name)
    write_parquet_metadata(out_cat)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("catalogs", type=Path, nargs="+",
                        help="Input catalog dir(s); each is written to <output-dir>/<name>")
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="Root for the transformed catalog(s); inputs are never modified")
    parser.add_argument("--nested-column", required=True,
                        help="e.g. objectForcedSource / diaObjectForcedSource")
    parser.add_argument("--flux-column", default="psfFlux")
    parser.add_argument("--err-column", default="psfFluxErr_corrected")
    parser.add_argument("--out-column", default="psfMagErr_corrected")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    for catalog_dir in args.catalogs:
        if args.output_dir.resolve() == catalog_dir.parent.resolve():
            parser.error(f"--output-dir would overwrite the input: {catalog_dir}")

    ok = True
    for catalog_dir in args.catalogs:
        ok &= process_catalog(catalog_dir, args.output_dir, args)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
