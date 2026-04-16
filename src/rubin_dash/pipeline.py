from __future__ import annotations

from typing import Optional

import time
import typer

from rubin_dash.config import PipelineConfig
from rubin_dash.stages.butler import run_butler
from rubin_dash.stages.collections import run_collections
from rubin_dash.stages.crossmatch import run_crossmatch
from rubin_dash.stages.generate_json import run_generate_json
from rubin_dash.stages.import_catalogs import run_import
from rubin_dash.stages.nesting import run_nesting
from rubin_dash.stages.postprocess import run_postprocess
from rubin_dash.stages.raw_sizes import run_raw_sizes

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
    try:
        import lsst.resources  # noqa: F401
    except ImportError:
        typer.echo(
            "Error: LSST stack is not available.\n"
            "Please activate it before running:\n"
            "  source /sdf/group/rubin/sw/loadLSST.sh && setup lsst_distrib",
            err=True,
        )
        raise typer.Exit(1)


def resolve_stages(
    cfg: PipelineConfig,
    stages_opt: Optional[str],
    from_stage_opt: Optional[str],
) -> list[str]:
    if stages_opt and from_stage_opt:
        typer.echo("Error: --stages and --from-stage are mutually exclusive.", err=True)
        raise typer.Exit(1)

    if stages_opt:
        requested = [s.strip() for s in stages_opt.split(",")]
    elif from_stage_opt:
        if from_stage_opt not in STAGE_ORDER:
            typer.echo(f"Error: unknown stage '{from_stage_opt}'. Valid stages: {', '.join(STAGE_ORDER)}", err=True)
            raise typer.Exit(1)
        start = STAGE_ORDER.index(from_stage_opt)
        requested = [s for s in STAGE_ORDER[start:] if s in cfg.stages.enabled]
    else:
        requested = list(cfg.stages.enabled)

    invalid = set(requested) - set(STAGE_ORDER)
    if invalid:
        typer.echo(f"Error: unknown stage(s): {', '.join(sorted(invalid))}. Valid stages: {', '.join(STAGE_ORDER)}", err=True)
        raise typer.Exit(1)

    return [s for s in STAGE_ORDER if s in requested]


def preflight_checks(
    stages_to_run: list[str],
    cfg: PipelineConfig,
    nesting_filter: Optional[list[str]],
    collection_filter: Optional[list[str]],
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
        typer.echo("Preflight checks failed:", err=True)
        for error in errors:
            typer.echo(f"  - {error}", err=True)
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
            typer.echo(
                f"Warning: skipping nesting '{name}' — required catalog(s) not active: "
                f"{', '.join(sorted(missing))}",
                err=True,
            )
        else:
            feasible_nestings.append(name)

    feasible_nesting_set = set(feasible_nestings)
    feasible_collections = []
    for name, coll_cfg in cfg.enabled_collections(None).items():
        nested_name = coll_cfg.nested_catalog
        if nested_name not in feasible_nesting_set:
            typer.echo(
                f"Warning: skipping collection '{name}' — nested catalog '{nested_name}' is not being built",
                err=True,
            )
        else:
            feasible_collections.append(name)

    return feasible_nestings, feasible_collections


def run_stage(
    stage: str,
    cfg: PipelineConfig,
    catalog_filter: Optional[list[str]],
    nesting_filter: Optional[list[str]],
    collection_filter: Optional[list[str]],
) -> None:
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
    stages_opt: Optional[str],
    from_stage_opt: Optional[str],
    catalogs_opt: Optional[str],
    nestings_opt: Optional[str],
    collections_opt: Optional[str],
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

    typer.echo("----- DASH Import Pipeline -----")
    typer.echo(f"Version    : {cfg.run.version}")
    typer.echo(f"Full Collection: {cfg.run.butler_collection}")
    typer.echo(f"Stages     : {', '.join(stages_to_run)}")
    typer.echo(f"Catalogs   : {', '.join(active_catalogs)}")
    typer.echo(f"Nestings   : {', '.join(active_nestings)}")
    typer.echo(f"Collections: {', '.join(active_collections)}")
    typer.echo("")

    preflight_checks(stages_to_run, cfg, nesting_filter, collection_filter)

    total_start = time.perf_counter()
    for stage in stages_to_run:
        stage_start = time.perf_counter()
        typer.echo(f"[{stage}] starting...")
        run_stage(stage, cfg, catalog_filter, nesting_filter, collection_filter)
        elapsed = time.perf_counter() - stage_start
        h, rem = divmod(int(elapsed), 3600)
        m, s = divmod(rem, 60)
        typer.echo(f"[{stage}] done in {h:02d}:{m:02d}:{s:02d}\n")

    total = time.perf_counter() - total_start
    h, rem = divmod(int(total), 3600)
    m, s = divmod(rem, 60)
    typer.echo(f"Pipeline complete. Total time: {h:02d}:{m:02d}:{s:02d}")