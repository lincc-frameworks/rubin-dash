from __future__ import annotations

import logging
import shutil

from hats.io.validation import is_valid_catalog
from hats_import import pipeline_with_client
from hats_import.collection.arguments import CollectionArguments

from rubin_dash.config import PipelineConfig
from rubin_dash.utils.dask_client import dask_client

logger = logging.getLogger(__name__)

STAGE = "collections"


def run_collections(cfg: PipelineConfig, collection_filter: list[str] | None = None) -> None:
    """Build HATS collections with margin and index from nested catalogs."""
    hats_dir = cfg.run.hats_dir

    with dask_client(cfg.dask.for_stage(STAGE)) as client:
        for collection_name, collection_cfg in cfg.enabled_collections(collection_filter).items():
            logger.info("Starting collections for %s...", collection_name)
            nested_name = collection_cfg.nested_catalog
            collection_dir = hats_dir / collection_name
            nested_dest = collection_dir / nested_name

            if cfg.run.resume and is_valid_catalog(collection_dir):
                logger.info("Skipping '%s' — valid collection already exists.", collection_name)
                continue

            # Move nested catalog into the collection directory if not already there
            collection_dir.mkdir(exist_ok=True)
            if not nested_dest.exists():
                shutil.move(str(hats_dir / nested_name), str(nested_dest))

            args = (
                CollectionArguments(
                    output_artifact_name=collection_name,
                    new_catalog_name=nested_name,
                    output_path=hats_dir,
                    simple_progress_bar=True,
                    resume=cfg.run.resume,
                )
                .catalog(catalog_path=nested_dest)
                .add_margin(margin_threshold=collection_cfg.margin_threshold, is_default=True)
                .add_index(indexing_column=collection_cfg.index_column)
            )
            pipeline_with_client(args, client)
