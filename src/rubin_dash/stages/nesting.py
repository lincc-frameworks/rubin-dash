from __future__ import annotations

import logging
import shutil
from pathlib import Path

import hats
import lsdb
from hats.io.validation import is_valid_catalog
from hats_import import pipeline_with_client
from hats_import.catalog import ImportArguments
from hats_import.margin_cache.margin_cache_arguments import MarginCacheArguments

from rubin_dash.config import NestedConfig, PipelineConfig
from rubin_dash.utils.dask_client import dask_client

logger = logging.getLogger(__name__)

STAGE = "nesting"


def run_nesting(cfg: PipelineConfig, nesting_filter: list[str] | None = None) -> None:
    """Build nested light-curve catalogs by joining object and source catalogs."""
    hats_dir = cfg.run.hats_dir

    with dask_client(cfg.dask.for_stage(STAGE)) as client:
        for nested_name, nested_cfg in cfg.enabled_nestings(nesting_filter).items():
            logger.info("Starting nesting for %s...", nested_name)
            _build_nested_catalog(
                nested_name=nested_name,
                nested_cfg=nested_cfg,
                hats_dir=hats_dir,
                client=client,
            )


def _build_nested_catalog(
    nested_name: str,
    nested_cfg: NestedConfig,
    hats_dir: Path,
    client,
) -> None:
    nested_path = hats_dir / nested_name
    if nested_cfg.resume and is_valid_catalog(nested_path):
        logger.info("Skipping '%s' — valid catalog already exists.", nested_name)
        return

    # Build margin caches for all source catalogs
    for source_name in nested_cfg.source_catalogs:
        margin_path = hats_dir / f"{source_name}_{nested_cfg.margin_radius_arcsec}arcs"
        if nested_cfg.resume and _is_valid_margin_cache(margin_path, nested_cfg.margin_radius_arcsec):
            logger.info("[%s] Reusing margin cache for '%s'.", nested_name, source_name)
        else:
            logger.info("[%s] Building margin cache for '%s'...", nested_name, source_name)
            args = MarginCacheArguments(
                input_catalog_path=hats_dir / source_name,
                output_path=hats_dir,
                margin_threshold=nested_cfg.margin_radius_arcsec,
                output_artifact_name=f"{source_name}_{nested_cfg.margin_radius_arcsec}arcs",
                simple_progress_bar=True,
                resume=nested_cfg.resume,
            )
            pipeline_with_client(args, client)

    intermediate_path = hats_dir / f"{nested_name}_intermediate"

    if nested_cfg.resume and is_valid_catalog(intermediate_path):
        logger.info("[%s] Reusing intermediate catalog.", nested_name)
        cols_cat = lsdb.open_catalog(intermediate_path)
    else:
        # Load object catalog
        obj_cat = lsdb.open_catalog(hats_dir / nested_cfg.object_catalog)

        # Load and join each source catalog
        nested_cat = obj_cat
        for source_name, column_name in zip(
            nested_cfg.source_catalogs, nested_cfg.nested_column_names, strict=False
        ):
            margin_path = hats_dir / f"{source_name}_{nested_cfg.margin_radius_arcsec}arcs"
            src_cat = lsdb.read_hats(hats_dir / source_name, margin_cache=margin_path)
            nested_cat = nested_cat.join_nested(
                src_cat,
                left_on=nested_cfg.join_id,
                right_on=nested_cfg.join_id,
                nested_column_name=column_name,
            )

        logger.info("[%s] Joining and writing intermediate nested catalog...", nested_name)
        source_cols = nested_cfg.nested_column_names
        nested_cat = nested_cat.map_partitions(
            lambda df: _sort_nested_sources(df, source_cols, nested_cfg.sort_column)
        )

        nested_cat.write_catalog(
            intermediate_path,
            catalog_name=nested_name,
            overwrite=not nested_cfg.resume,
            resume=nested_cfg.resume,
        )
        cols_cat = nested_cat

    # Compute hats_cols_default if default columns are specified
    addl_props: dict = {}
    if nested_cfg.default_columns:
        actual_cols = set(_full_column_names(cols_cat))
        valid_default_cols = [c for c in nested_cfg.default_columns if c in actual_cols]
        missing = sorted(set(nested_cfg.default_columns) - actual_cols)
        if missing:
            logger.warning(
                "Requested default columns missing from %s: %s", nested_name, ", ".join(missing)
            )
        addl_props["hats_cols_default"] = ",".join(valid_default_cols)

    logger.info("[%s] Reimporting from intermediate catalog...", nested_name)
    reimport_args = ImportArguments.reimport_from_hats(
        intermediate_path,
        output_dir=hats_dir,
        highest_healpix_order=nested_cfg.highest_healpix_order,
        pixel_threshold=nested_cfg.pixel_threshold,
        skymap_alt_orders=nested_cfg.skymap_alt_orders,
        row_group_kwargs=nested_cfg.row_group_kwargs,
        resume=nested_cfg.resume,
        **({"addl_hats_properties": addl_props} if addl_props else {}),
    )
    pipeline_with_client(reimport_args, client)
    shutil.rmtree(intermediate_path)
    for source_name in nested_cfg.source_catalogs:
        margin_path = hats_dir / f"{source_name}_{nested_cfg.margin_radius_arcsec}arcs"
        if margin_path.exists():
            shutil.rmtree(margin_path)


def _is_valid_margin_cache(path: Path, margin_arcsec: float) -> bool:
    if not is_valid_catalog(path):
        return False
    return hats.read_hats(path).catalog_info.margin_threshold == margin_arcsec


def _sort_nested_sources(df, source_cols: list[str], sort_col: str):
    for col in source_cols:
        flat = df[col].nest.to_flat()
        df = df.drop(columns=[col])
        df = df.join_nested(flat.sort_values([flat.index.name, sort_col]), col)
    return df


def _full_column_names(cat):
    """Yield all column names including nested sub-columns as 'nested_col.field'."""
    for c in cat.columns:
        cc = cat[c]
        if not hasattr(cc, "nest"):
            yield c
        else:
            for f in cc.nest.columns:
                yield f"{c}.{f}"
