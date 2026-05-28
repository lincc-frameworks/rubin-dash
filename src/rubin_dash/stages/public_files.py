from __future__ import annotations

import logging
from shutil import copyfileobj

from lsst.daf.butler import Butler

from rubin_dash.config import PipelineConfig

logger = logging.getLogger(__name__)


def run_public_files(cfg: PipelineConfig) -> None:
    """Stream configured butler datasets into hats_dir/public-files/ as parquet files."""
    if not cfg.public_files.datasets:
        logger.info("No datasets configured for public_files stage — skipping.")
        return
    logger.info(
        "Starting public_files stage for datasets: %s",
        ", ".join(d.type for d in cfg.public_files.datasets),
    )

    col_butler = Butler(cfg.run.repo)
    collections = list(col_butler.registry.queryCollections(cfg.run.butler_collection))
    butler = Butler(cfg.run.repo, collections=collections)

    out_dir = cfg.run.public_files_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    for dataset in cfg.public_files.datasets:
        dest = out_dir / dataset.name
        uri = butler.getURI(dataset.type, dataId={"instrument": cfg.run.instrument})
        with uri.open("rb") as src, dest.open("wb") as dst:
            copyfileobj(src, dst)
        logger.info("Saved %s → %s", dataset.type, dest)
