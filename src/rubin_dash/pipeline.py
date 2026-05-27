from __future__ import annotations

import logging
import shutil
import time

import typer

from rubin_dash import __version__
from rubin_dash.config import PipelineConfig
from rubin_dash.stages.butler import run_butler
from rubin_dash.stages.collections import run_collections
from rubin_dash.stages.crossmatch import run_crossmatch
from rubin_dash.stages.generate_json import run_generate_json
from rubin_dash.stages.import_catalogs import run_import
from rubin_dash.stages.nesting import run_nesting
from rubin_dash.stages.postprocess import run_postprocess
from rubin_dash.stages.raw_sizes import run_raw_sizes

logger = logging.getLogger(__name__)

STAGE_ORDER = [
    "butler",
    "raw_sizes",
    "import",
    "postprocess",
    "nesting",
    "collections",
    "crossmatch",
    "generate_json",
]

# Stages that require the LSST stack to be active
LSST_STAGES = {"butler", "raw_sizes", "import"}


def check_lsst() -> None:
    """Verify the LSST stack is importable; exit with a helpful message if not."""
    try:
        import lsst.resources  # noqa: F401
    except ImportError:
        logger.error(
            "LSST stack is not available.\n"
            "Please activate it before running:\n"
            "  source /sdf/group/rubin/sw/loadLSST.sh && setup lsst_distrib"
        )
        raise typer.Exit(1) from None


def resolve_stages(
    cfg: PipelineConfig,
    stages_opt: str | None,
    from_stage_opt: str | None,
) -> list[str]:
    """Return the ordered list of stages to run, validated against STAGE_ORDER."""
    if stages_opt and from_stage_opt:
        logger.error("--stages and --from-stage are mutually exclusive.")
        raise typer.Exit(1)

    if stages_opt:
        requested = [s.strip() for s in stages_opt.split(",")]
    elif from_stage_opt:
        if from_stage_opt not in STAGE_ORDER:
            logger.error("Unknown stage '%s'. Valid stages: %s", from_stage_opt, ", ".join(STAGE_ORDER))
            raise typer.Exit(1)
        start = STAGE_ORDER.index(from_stage_opt)
        requested = [s for s in STAGE_ORDER[start:] if s in cfg.stages.enabled]
    else:
        requested = list(cfg.stages.enabled)

    invalid = set(requested) - set(STAGE_ORDER)
    if invalid:
        logger.error(
            "Unknown stage(s): %s. Valid stages: %s", ", ".join(sorted(invalid)), ", ".join(STAGE_ORDER)
        )
        raise typer.Exit(1)

    return [s for s in STAGE_ORDER if s in requested]


def preflight_checks(
    stages_to_run: list[str],
    cfg: PipelineConfig,
    nesting_filter: list[str] | None,
    collection_filter: list[str] | None,
) -> None:
    """Check that each stage's required inputs are either produced earlier in this run
    or already exist on disk. Collected and reported together before anything runs."""
    hats_dir = cfg.run.hats_dir
    errors: list[str] = []

    active_nestings = cfg.enabled_nestings(nesting_filter)
    active_collections = cfg.enabled_collections(collection_filter)

    if "nesting" in stages_to_run:
        for nested_name, nested_cfg in active_nestings.items():
            for cat_name in [nested_cfg.object_catalog] + nested_cfg.source_catalogs:
                if cat_name not in cfg.catalogs.enabled and not (hats_dir / cat_name).exists():
                    errors.append(
                        f"nesting '{nested_name}' needs catalog '{cat_name}' but it is not "
                        f"in catalogs.enabled and {hats_dir / cat_name} does not exist."
                    )

    if "collections" in stages_to_run:
        for collection_name, collection_cfg in active_collections.items():
            nested_name = collection_cfg.nested_catalog
            produced = "nesting" in stages_to_run and nested_name in active_nestings
            if not produced and not (hats_dir / nested_name).exists():
                errors.append(
                    f"collections '{collection_name}' needs nested catalog '{nested_name}' but "
                    f"nesting is not running and {hats_dir / nested_name} does not exist."
                )

    if "crossmatch" in stages_to_run:
        for collection_name in active_collections:
            produced = "collections" in stages_to_run
            if not produced and not (hats_dir / collection_name).exists():
                errors.append(
                    f"crossmatch needs collection '{collection_name}' but collections stage "
                    f"is not running and {hats_dir / collection_name} does not exist."
                )

    if "generate_json" in stages_to_run:
        for collection_name in active_collections:
            produced = "collections" in stages_to_run
            if not produced and not (hats_dir / collection_name).exists():
                errors.append(
                    f"generate_json needs collection '{collection_name}' but collections stage "
                    f"is not running and {hats_dir / collection_name} does not exist."
                )

    if errors:
        logger.error("Preflight checks failed:")
        for error in errors:
            logger.error("  - %s", error)
        raise typer.Exit(1)


def constrain_to_catalogs(
    cfg: PipelineConfig,
    catalog_names: list[str],
) -> tuple[list[str], list[str]]:
    """Prune nestings and collections whose required catalogs are not all active.

    Skipped for any filter that was explicitly set by the user — only applies to
    filters that were inferred from config (i.e. still None at call time).
    Returns explicit lists (possibly empty) for nesting and collection filters,
    and prints warnings for anything that gets dropped.
    """
    catalog_set = set(catalog_names)
    feasible_nestings = []
    for name, nested_cfg in cfg.enabled_nestings(None).items():
        required = set([nested_cfg.object_catalog] + nested_cfg.source_catalogs)
        missing = required - catalog_set
        if missing:
            logger.warning(
                "Skipping nesting '%s' — required catalog(s) not active: %s",
                name,
                ", ".join(sorted(missing)),
            )
        else:
            feasible_nestings.append(name)

    feasible_nesting_set = set(feasible_nestings)
    feasible_collections = []
    for name, coll_cfg in cfg.enabled_collections(None).items():
        nested_name = coll_cfg.nested_catalog
        if nested_name not in feasible_nesting_set:
            logger.warning(
                "Skipping collection '%s' — nested catalog '%s' is not being built",
                name,
                nested_name,
            )
        else:
            feasible_collections.append(name)

    return feasible_nestings, feasible_collections


def run_stage(
    stage: str,
    cfg: PipelineConfig,
    catalog_filter: list[str] | None,
    nesting_filter: list[str] | None,
    collection_filter: list[str] | None,
) -> None:
    """Dispatch a single stage to its run function."""
    if stage == "butler":
        run_butler(cfg, catalog_filter)
    elif stage == "raw_sizes":
        run_raw_sizes(cfg, catalog_filter)
    elif stage == "import":
        run_import(cfg, catalog_filter)
    elif stage == "postprocess":
        run_postprocess(cfg, catalog_filter)
    elif stage == "nesting":
        run_nesting(cfg, nesting_filter)
    elif stage == "collections":
        run_collections(cfg, collection_filter)
    elif stage == "crossmatch":
        run_crossmatch(cfg, collection_filter)
    elif stage == "generate_json":
        run_generate_json(cfg, collection_filter)


def run_pipeline(
    cfg: PipelineConfig,
    stages_opt: str | None,
    from_stage_opt: str | None,
    catalogs_opt: str | None,
    nestings_opt: str | None,
    collections_opt: str | None,
) -> None:
    """Resolve options, run preflight checks, and execute the pipeline."""

    stages_to_run = resolve_stages(cfg, stages_opt, from_stage_opt)

    if any(s in LSST_STAGES for s in stages_to_run):
        check_lsst()

    catalog_filter = [c.strip() for c in catalogs_opt.split(",")] if catalogs_opt else None
    nesting_filter = [n.strip() for n in nestings_opt.split(",")] if nestings_opt else None
    collection_filter = [c.strip() for c in collections_opt.split(",")] if collections_opt else None

    active_catalogs = list(cfg.enabled_catalogs(catalog_filter).keys())
    if nesting_filter is None and collection_filter is None:
        nesting_filter, collection_filter = constrain_to_catalogs(cfg, active_catalogs)
    active_nestings = list(cfg.enabled_nestings(nesting_filter).keys())
    active_collections = list(cfg.enabled_collections(collection_filter).keys())

    logger.info("----- DASH Import Pipeline -----")
    logger.info("rubin-dash version  : %s", __version__)
    logger.info("Version    : %s", cfg.run.version)
    logger.info("Full Collection: %s", cfg.run.butler_collection)
    logger.info("Stages     : %s", ", ".join(stages_to_run))
    logger.info("Catalogs   : %s", ", ".join(active_catalogs))
    logger.info("Nestings   : %s", ", ".join(active_nestings))
    logger.info("Collections: %s", ", ".join(active_collections))
    logger.info("")

    preflight_checks(stages_to_run, cfg, nesting_filter, collection_filter)

    cfg.run.pipeline_state_dir.mkdir(parents=True, exist_ok=True)

    total_start = time.perf_counter()
    for stage in stages_to_run:
        marker = cfg.run.pipeline_state_dir / f"{stage}.done"
        if cfg.run.resume and marker.exists():
            logger.info("[%s] already complete — skipping. (delete %s to re-run)", stage, marker)
            continue
        stage_start = time.perf_counter()
        logger.info("[%s] starting...", stage)
        run_stage(stage, cfg, catalog_filter, nesting_filter, collection_filter)
        marker.touch()
        elapsed = time.perf_counter() - stage_start
        h, rem = divmod(int(elapsed), 3600)
        m, s = divmod(rem, 60)
        logger.info("[%s] done in %02d:%02d:%02d\n", stage, h, m, s)

    if cfg.run.pipeline_state_dir.exists():
        shutil.rmtree(cfg.run.pipeline_state_dir)

    total = time.perf_counter() - total_start
    h, rem = divmod(int(total), 3600)
    m, s = divmod(rem, 60)
    logger.info("Pipeline complete. Total time: %02d:%02d:%02d", h, m, s)
