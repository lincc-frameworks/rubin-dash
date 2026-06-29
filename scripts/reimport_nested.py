"""Re-run only the final import of the nesting stage, with a different threshold.

The nesting stage ends by re-tiling its joined catalog via
``ImportArguments.reimport_from_hats`` (see ``stages/nesting.py``). On a *completed*
run the ``<name>_intermediate`` collection and the margin caches that feed that step
have already been deleted, so you cannot resume into it. You don't need them: the
finished nested catalog at ``<hats_dir>/<name>`` is itself a valid HATS catalog, and
reimport can read straight from it. This script does exactly that — same data, new
partitioning — without rebuilding the margin caches or redoing the join.

By default it switches to the *memory* (byte-size) threshold via
``--byte-pixel-threshold``, which overrides the row-count ``pixel_threshold``. Pass
``--pixel-threshold`` instead to re-tile by row count.

    python scripts/reimport_nested.py -c config.toml object_lc \
        --byte-pixel-threshold 500_000_000 --output-dir /sdf/.../hats/<ver>/rethreshold

The reimported catalog is written as ``<output-dir>/<catalog_name>``; point
``--output-dir`` somewhere other than the live ``hats_dir`` so the original is kept.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from hats_import import pipeline_with_client
from hats_import.catalog import ImportArguments

from rubin_dash.config import load_config
from rubin_dash.log import setup_logging
from rubin_dash.utils.dask_client import dask_client

logger = logging.getLogger(__name__)

STAGE = "nesting"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("nesting", help="Nested catalog name to reimport (e.g. object_lc)")
    parser.add_argument(
        "-c",
        "--config",
        dest="config_paths",
        action="append",
        type=Path,
        required=True,
        help="TOML config file(s); repeat to layer overrides (left to right).",
    )
    parser.add_argument(
        "--source-path",
        type=Path,
        default=None,
        help="Path to the catalog to reimport. Defaults to <hats_dir>/<nesting>. After the "
        "collections stage runs, the nested catalog is moved into its collection, so pass "
        "e.g. <hats_dir>/object_collection/object_lc (or the collection dir itself).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Where to write the reimported catalog. Defaults to <hats_dir>/reimport. "
        "Keep this off the live hats_dir to avoid overwriting the original.",
    )
    thresh = parser.add_mutually_exclusive_group(required=True)
    thresh.add_argument(
        "--byte-pixel-threshold",
        type=int,
        help="Memory-size threshold in bytes (the 'memory import'); overrides pixel_threshold.",
    )
    thresh.add_argument(
        "--pixel-threshold",
        type=int,
        help="Row-count threshold (the default mode).",
    )
    args = parser.parse_args()

    setup_logging(None)
    cfg = load_config(args.config_paths)

    nested_cfg = cfg.enabled_nestings().get(args.nesting) or cfg.nested.configs.get(args.nesting)
    if nested_cfg is None:
        parser.error(f"No [nested.{args.nesting}] config section found.")

    hats_dir = cfg.run.hats_dir
    source_path = args.source_path or (hats_dir / args.nesting)
    output_dir = args.output_dir or (hats_dir / "reimport")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Only the threshold differs from the original reimport; everything else mirrors
    # stages/nesting.py so the re-tiled catalog matches apart from partitioning. Start
    # from the config's reimport_args and let the CLI threshold win — dropping any
    # threshold already set in config so we never pass a kwarg twice.
    reimport_kwargs = dict(nested_cfg.reimport_args)
    reimport_kwargs.pop("pixel_threshold", None)
    reimport_kwargs.pop("byte_pixel_threshold", None)
    if args.byte_pixel_threshold is not None:
        reimport_kwargs["byte_pixel_threshold"] = args.byte_pixel_threshold
    else:
        reimport_kwargs["pixel_threshold"] = args.pixel_threshold

    logger.info("Reimporting %s from %s -> %s (%s)", args.nesting, source_path, output_dir, reimport_kwargs)

    reimport_args = ImportArguments.reimport_from_hats(
        source_path,
        output_dir=output_dir,
        highest_healpix_order=nested_cfg.highest_healpix_order,
        skymap_alt_orders=nested_cfg.skymap_alt_orders,
        row_group_kwargs=nested_cfg.row_group_kwargs,
        resume=nested_cfg.resume,
        **reimport_kwargs,
    )

    with dask_client(cfg.dask.for_stage(STAGE)) as client:
        pipeline_with_client(reimport_args, client)

    logger.info("Done. Reimported catalog at %s", output_dir / args.nesting)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
