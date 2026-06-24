"""Standalone reproducer for the worker SIGSEGV seen during the import "splitting" phase.

The split phase reads every column of each input file through ``DimensionParquetReader``
and maps ra/dec to healpix. Workers die with signal 11 inside native code, and an
in-process faulthandler never fires (a third-party signal handler overrides it). This
script runs that exact read+map path in a plain process — no dask, no nanny, no
contested signal handlers — so it can be run under gdb to get the native backtrace:

    gdb -batch -ex run -ex 'thread apply all bt' \
        --args python scripts/repro_split.py --ra-column coord_ra --dec-column coord_dec \
        /sdf/.../raw/index/object/*.csv

Each input file is printed before it is read, so when gdb catches the crash the last
"READING <file>" line names the culprit file. Drop --ra-column/--dec-column to exercise
only the reader (the most common crash site); pass them to also run healpy radec2pix.
"""
from __future__ import annotations

import argparse
import sys

import numpy as np
from hats import pixel_math  # noqa: F401  (ensure hats native libs load like in-pipeline)

from rubin_dash.utils.readers import DimensionParquetReader

HIGHEST_ORDER = 11  # mapping order hats_import uses by default


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("index_files", nargs="+", help="CSV index file(s) to read")
    parser.add_argument("--chunksize", type=int, default=250_000)
    parser.add_argument("--ra-column", default=None, help="also run radec2pix if given")
    parser.add_argument("--dec-column", default=None)
    args = parser.parse_args()

    reader = DimensionParquetReader(chunksize=args.chunksize)
    do_map = bool(args.ra_column and args.dec_column)
    if do_map:
        import hats.pixel_math.healpix_shim as hp

    for path in args.index_files:
        print(f"READING {path}", flush=True)
        for chunk_number, table in enumerate(reader.read(path, read_columns=None)):
            # mimic the splitting work that happens per chunk
            if do_map:
                mapped = hp.radec2pix(
                    HIGHEST_ORDER,
                    table[args.ra_column].to_numpy(),
                    table[args.dec_column].to_numpy(),
                )
                np.unique(mapped, return_inverse=True)
            print(f"  chunk {chunk_number}: {table.num_rows} rows", flush=True)

    print("DONE — no crash", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
