"""Generate per-partition statistics and static summary files for an RC release folder.

Walks every entry in the given HATS root (e.g. ``hats/dp2_rc1``), classifies it, and
generates the appropriate artifacts:

- **collection**: skymap.png, partition_info.png, and summary files (html + markdown) at
  the collection root; then each member catalog by its own type (below).
- **catalog** (object/source, incl. a collection's main catalog): everything —
  ``per_partition_statistics.parquet``, skymap.png, partition_info.png, summary files.
- **index**: summary files only.
- **margin**: summary files only.

Entries in the RC folder are typically symlinks — all generated files are written
*through* them into the target catalogs (dp2 / dp2-fixed), which is what makes them
show up in every view of those catalogs. Everything written is additive (plus a
regenerated ``_common_metadata``, a side effect of the stats writer). Existing artifacts
are skipped unless ``--force`` is given, so the script is resumable; failures are logged
per artifact and reflected in the exit code.

    python scripts/generate_rc_static_files.py /sdf/data/rubin/shared/lsdb_commissioning/hats/dp2_rc1
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from hats.io.parquet_metadata import write_parquet_metadata
from hats.io.summary_file import (
    write_catalog_summary_file,
    write_partition_info_png,
    write_skymap_png,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SUMMARY_ARTIFACTS = {
    "summary html": ("index.html", lambda p: write_catalog_summary_file(p, fmt="html")),
    "summary markdown": ("README.md", lambda p: write_catalog_summary_file(p, fmt="markdown")),
}
PNG_ARTIFACTS = {
    "skymap png": ("skymap.png", write_skymap_png),
    "partition png": ("partition_info.png", write_partition_info_png),
}


def _run_artifacts(path: Path, artifacts: dict, force: bool) -> list[str]:
    failures = []
    for label, (filename, fn) in artifacts.items():
        if not force and (path / filename).exists():
            logger.info("    %-18s exists, skipped", label)
            continue
        try:
            fn(path)
            logger.info("    %-18s written", label)
        except Exception as e:  # noqa: BLE001 — keep going; report at the end
            logger.error("    %-18s FAILED: %r", label, e)
            failures.append(f"{path.name}: {label}: {e!r}")
    return failures


def _run_stats(path: Path, force: bool) -> list[str]:
    if not force and (path / "per_partition_statistics.parquet").exists():
        logger.info("    %-18s exists, skipped", "partition stats")
        return []
    n_files = sum(1 for _ in (path / "dataset").rglob("*.parquet"))
    logger.info("    partition stats: sweeping %d file footers (no progress output — "
                "expect minutes for large catalogs)...", n_files)
    try:
        write_parquet_metadata(path, create_metadata=False, create_per_partition_stats=True)
        logger.info("    %-18s written", "partition stats")
        return []
    except Exception as e:  # noqa: BLE001
        logger.error("    %-18s FAILED: %r", "partition stats", e)
        return [f"{path.name}: partition stats: {e!r}"]


def catalog_kind(path: Path) -> str:
    """'collection', 'margin', 'index', or 'catalog' (object/source/anything else)."""
    if (path / "collection.properties").exists():
        return "collection"
    # parse the properties file directly — no need to load the whole catalog to classify
    ctype = ""
    for line in (path / "properties").read_text().splitlines():
        if line.strip().startswith("dataproduct_type"):
            ctype = line.split("=", 1)[1].strip().lower()
            break
    if "margin" in ctype:
        return "margin"
    if "index" in ctype:
        return "index"
    return "catalog"


def process_entry(path: Path, force: bool) -> list[str]:
    kind = catalog_kind(path)
    target = f" -> {path.resolve()}" if path.is_symlink() else ""
    logger.info("%s [%s]%s", path.name, kind, target)

    failures = []
    if kind == "collection":
        # collection-level pngs + summaries, then members by their own type
        failures += _run_artifacts(path, {**PNG_ARTIFACTS, **SUMMARY_ARTIFACTS}, force)
        for member in sorted(p for p in path.iterdir() if p.is_dir() and (p / "dataset").is_dir()):
            failures += process_entry(member, force)
    elif kind in ("margin", "index"):
        failures += _run_artifacts(path, SUMMARY_ARTIFACTS, force)
    else:  # plain catalog (incl. a collection's main catalog)
        failures += _run_stats(path, force)
        failures += _run_artifacts(path, {**PNG_ARTIFACTS, **SUMMARY_ARTIFACTS}, force)
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("root", type=Path, help="RC folder holding catalogs/collections (or symlinks)")
    parser.add_argument("--only", nargs="*", default=None,
                        help="Optional entry names to process (default: everything)")
    parser.add_argument("--force", action="store_true", help="Regenerate artifacts that already exist")
    args = parser.parse_args()

    entries = sorted(p for p in args.root.iterdir() if p.is_dir())
    if args.only:
        entries = [p for p in entries if p.name in set(args.only)]
    if not entries:
        logger.error("nothing to process under %s", args.root)
        return 1
    logger.info("processing %d entries under %s", len(entries), args.root)

    failures = []
    for entry in entries:
        try:
            failures += process_entry(entry, args.force)
        except Exception as e:  # noqa: BLE001 — classification/read failure for the whole entry
            logger.error("%s: FAILED to process: %r", entry.name, e)
            failures.append(f"{entry.name}: {e!r}")

    if failures:
        logger.error("%d artifact(s) failed:", len(failures))
        for f in failures:
            logger.error("  %s", f)
        return 1
    logger.info("all artifacts generated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
