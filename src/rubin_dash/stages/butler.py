from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from lsst.daf.butler import Butler

from rubin_dash.config import PipelineConfig

logger = logging.getLogger(__name__)


def run_butler(cfg: PipelineConfig, catalog_filter: list[str] | None = None) -> None:
    """Export catalog parquet URIs and visit table from the Butler repository."""
    raw_dir = cfg.run.raw_dir
    for subdir in ("paths", "refs", "sizes"):
        (raw_dir / subdir).mkdir(parents=True, exist_ok=True)

    col_butler = Butler(cfg.run.repo)

    # Expand your pattern into a real list
    collections = list(col_butler.registry.queryCollections(cfg.run.butler_collection))

    if len(collections) > 1:
        logger.info(
            "Found %d collections matching pattern '%s':", len(collections), cfg.run.butler_collection
        )
        logger.info(", ".join(collections))

    butler = Butler(cfg.run.repo, collections=collections)

    for catalog_name in cfg.enabled_catalogs(catalog_filter):
        _get_uris_from_butler(butler, catalog_name, raw_dir)

    _get_visits_from_butler(butler, cfg.run.instrument, cfg.run.visit_table_name, raw_dir)


def _get_uris_from_butler(butler, dataset_type: str, raw_dir: Path) -> None:
    start = time.perf_counter()
    refs = butler.query_datasets(dataset_type, limit=None)
    uris = butler._datastore.getManyURIs(refs)
    paths = [value.primaryURI.geturl() for value in uris.values()]

    (raw_dir / "paths" / f"{dataset_type}.txt").write_text("\n".join(paths) + "\n", encoding="utf8")

    ref_ids = [ref.dataId.mapping for ref in refs]
    pd.DataFrame(ref_ids).to_csv(raw_dir / "refs" / f"{dataset_type}.csv", index=False)

    logger.info("Found %6d files for %30s in %10.2fs", len(paths), dataset_type, time.perf_counter() - start)


def _get_visits_from_butler(butler, instrument: str, visits_type: str, raw_dir: Path) -> None:
    visits = butler.get(visits_type, dataId={"instrument": instrument})
    parquet_path = raw_dir / f"{visits_type}.parquet"
    pq.write_table(pa.Table.from_pandas(visits.to_pandas()), parquet_path)
    logger.info("Saved %d visit rows to %s", len(visits), parquet_path)
