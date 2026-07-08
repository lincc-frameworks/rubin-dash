from __future__ import annotations

import importlib.resources
import tomllib
from collections.abc import Iterable
from os import PathLike
from pathlib import Path
from typing import Any

from pydantic import BaseModel, model_validator

# ImportArguments fields that are always set programmatically — not allowed in import_args config
_IMPORT_ARGS_MANAGED = frozenset(
    {
        "output_path",
        "output_artifact_name",
        "input_file_list",
        "file_reader",
        "use_schema_file",  # handled via the top-level use_schema_file bool
        "resume",  # handled via the top-level resume bool
    }
)

# CollectionArguments.add_margin fields managed by CollectionConfig — not allowed in margin_import_args
_MARGIN_IMPORT_ARGS_MANAGED = frozenset({"margin_threshold", "is_default"})

# CollectionArguments.add_index fields managed by CollectionConfig — not allowed in index_import_args
_INDEX_IMPORT_ARGS_MANAGED = frozenset({"indexing_column"})

# reimport_from_hats fields managed by NestedConfig — not allowed in reimport_args config
_REIMPORT_ARGS_MANAGED = frozenset(
    {
        "output_dir",
        "highest_healpix_order",
        "pixel_threshold",
        "skymap_alt_orders",
        "row_group_kwargs",
        "resume",
        "addl_hats_properties",
    }
)


class RunConfig(BaseModel):
    """Run-specific fields: instrument, repo, version, collection, and output location."""

    instrument: str
    repo: str
    version: str
    output_dir: Path
    run: str | None = None
    collection: str | None = None
    butler_collection: str | None = None
    visit_table_name: str = "visit_table"
    resume: bool = True

    @model_validator(mode="after")
    def _fill_butler_collection(self) -> RunConfig:
        """Construct butler_collection from component parts if not set explicitly."""
        if self.butler_collection is None:
            parts = [self.instrument, "runs", "DRP"]
            if self.run:
                parts.append(self.run)
            parts.append(self.version)
            if self.collection:
                parts.append(self.collection)
            self.butler_collection = "/".join(parts)
        return self

    @property
    def pipeline_state_dir(self) -> Path:
        """Directory for per-stage completion markers."""
        return self.hats_dir / ".pipeline_state"

    @property
    def raw_dir(self) -> Path:
        """Directory for raw butler exports (paths, refs, sizes, index files)."""
        return self.output_dir / "raw" / self.version

    @property
    def hats_dir(self) -> Path:
        """Directory for output HATS catalogs."""
        return self.output_dir / "hats" / self.version

    @property
    def public_files_dir(self) -> Path:
        """Directory for public parquet files exported from Butler."""
        return self.hats_dir / "public-files"

    @property
    def validation_dir(self) -> Path:
        """Directory for validation outputs."""
        return self.output_dir / "validation" / self.version


class StagesConfig(BaseModel):
    """Controls which pipeline stages are enabled for a run."""

    enabled: list[str] = [
        "butler",
        "raw_sizes",
        "import",
        "postprocess",
        "nesting",
        "collections",
        "crossmatch",
        "generate_json",
        "public_files",
    ]


_ALL_CATALOGS = [
    "dia_object",
    "dia_source",
    "dia_object_forced_source",
    "object",
    "source",
    "object_forced_source",
]


class CatalogConfig(BaseModel):
    """Per-catalog import settings: dimensions, flux columns, chunksize, and hats-import args."""

    dims: list[str] = []  # dimension columns added to index files from refs CSV
    group_by: list[str] = []  # columns to group index batch files by
    flux_columns: list[str] = []
    add_mjds: bool = False
    use_schema_file: bool = False
    chunksize: int = 500_000  # DimensionParquetReader batch size
    resume: bool = True
    import_args: dict[str, Any] = {}

    @model_validator(mode="after")
    def _validate_import_args(self) -> CatalogConfig:
        managed = set(self.import_args.keys()) & _IMPORT_ARGS_MANAGED
        if managed:
            raise ValueError(
                f"These fields are managed automatically and cannot be set in import_args: "
                f"{', '.join(sorted(managed))}"
            )
        return self


class CatalogsConfig(BaseModel):
    """Holds the enabled list and the per-catalog config tables.

    TOML shape:
        [catalogs]
        enabled = ["dia_object", "object"]

        [catalogs.dia_object]
        dims = ["tract"]
        ...
    """

    enabled: list[str] = list(_ALL_CATALOGS)
    configs: dict[str, CatalogConfig] = {}

    @model_validator(mode="before")
    @classmethod
    def _split_enabled_and_configs(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        configs = {k: v for k, v in data.items() if k != "enabled" and isinstance(v, dict)}
        result: dict[str, Any] = {"configs": configs}
        if "enabled" in data:
            result["enabled"] = data["enabled"]
        return result


class NestedConfig(BaseModel):
    """Configuration for building a single nested (light-curve) catalog."""

    object_catalog: str
    join_id: str  # e.g. "diaObjectId" or "objectId"
    source_catalogs: list[str]
    nested_column_names: list[str]  # parallel to source_catalogs
    sort_column: str = "midpointMjdTai"
    margin_radius_arcsec: int = 2
    resume: bool = True
    pixel_threshold: int = 15_000
    highest_healpix_order: int = 11
    skymap_alt_orders: list[int] = [2, 4, 6]
    row_group_kwargs: dict[str, Any] = {"subtile_order_delta": 1}
    default_columns: list[str] = []  # hats_cols_default; empty = all columns
    reimport_args: dict[str, Any] = {}

    @model_validator(mode="after")
    def _validate_reimport_args(self) -> NestedConfig:
        managed = set(self.reimport_args.keys()) & _REIMPORT_ARGS_MANAGED
        if managed:
            raise ValueError(
                f"These fields are managed automatically and cannot be set in reimport_args: "
                f"{', '.join(sorted(managed))}"
            )
        return self


class CollectionConfig(BaseModel):
    """Configuration for building a single HATS collection from a nested catalog."""

    nested_catalog: str
    margin_threshold: float = 5.0
    index_column: str
    margin_import_args: dict[str, Any] = {}
    index_import_args: dict[str, Any] = {}

    @model_validator(mode="after")
    def _validate_collection_args(self) -> CollectionConfig:
        managed_margin = set(self.margin_import_args.keys()) & _MARGIN_IMPORT_ARGS_MANAGED
        if managed_margin:
            raise ValueError(
                f"These fields are managed automatically and cannot be set in margin_import_args: "
                f"{', '.join(sorted(managed_margin))}"
            )
        managed_index = set(self.index_import_args.keys()) & _INDEX_IMPORT_ARGS_MANAGED
        if managed_index:
            raise ValueError(
                f"These fields are managed automatically and cannot be set in index_import_args: "
                f"{', '.join(sorted(managed_index))}"
            )
        return self


class NestedConfigs(BaseModel):
    """Holds the enabled list and per-nested-catalog config tables.

    TOML shape:
        [nested]
        enabled = ["object_lc"]   # optional — omit to run all

        [nested.object_lc]
        object_catalog = "object"
        ...
    """

    enabled: list[str] | None = None
    configs: dict[str, NestedConfig] = {}

    @model_validator(mode="before")
    @classmethod
    def _split_enabled_and_configs(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        configs = {k: v for k, v in data.items() if k != "enabled" and isinstance(v, dict)}
        result: dict[str, Any] = {"configs": configs}
        if "enabled" in data:
            result["enabled"] = data["enabled"]
        return result


class CollectionsConfig(BaseModel):
    """Holds the enabled list and per-collection config tables.

    TOML shape:
        [collections]
        enabled = ["object_collection"]   # optional — omit to run all

        [collections.object_collection]
        nested_catalog = "object_lc"
        ...
    """

    enabled: list[str] | None = None
    configs: dict[str, CollectionConfig] = {}

    @model_validator(mode="before")
    @classmethod
    def _split_enabled_and_configs(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        configs = {k: v for k, v in data.items() if k != "enabled" and isinstance(v, dict)}
        result: dict[str, Any] = {"configs": configs}
        if "enabled" in data:
            result["enabled"] = data["enabled"]
        return result


class PublicFileDataset(BaseModel):
    """A single dataset to export in the public_files stage."""

    type: str
    name: str

    @model_validator(mode="before")
    @classmethod
    def _coerce_string(cls, value: Any) -> Any:
        if isinstance(value, str):
            return {"type": value, "name": f"{value}.parquet"}
        return value


class PublicFilesConfig(BaseModel):
    """Configuration for the public_files stage."""

    datasets: list[PublicFileDataset] = []


class CrossmatchSurveyConfig(BaseModel):
    """Configuration for a single external survey used in crossmatching."""

    path: str
    radius_arcsec: float = 0.2
    n_neighbors: int = 20
    suffix: str  # e.g. "_ztf" — appended by crossmatch, used to derive xmatch col name
    join_id_column: str  # ID column in the external catalog, e.g. "objectid"
    s3_endpoint_url: str | None = None
    s3_anon: bool = False


class CrossmatchConfig(BaseModel):
    """Holds the set of external surveys to crossmatch against."""

    surveys: dict[str, CrossmatchSurveyConfig] = {}


class DaskConfig(BaseModel):
    """Global Dask client settings and per-stage overrides.

    TOML shape:
        [dask]
        n_workers = 8
        threads_per_worker = 1
        # any other kwargs accepted by dask.distributed.Client

        [dask.stages.import]
        n_workers = 8
        memory_limit = "16GB"

        [dask.stages.postprocess]
        n_workers = 16
    """

    global_kwargs: dict[str, Any] = {}
    stages: dict[str, dict[str, Any]] = {}

    @model_validator(mode="before")
    @classmethod
    def _split_global_and_stages(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        stages = data.get("stages", {})
        global_kwargs = {k: v for k, v in data.items() if k != "stages"}
        return {"global_kwargs": global_kwargs, "stages": stages}

    def for_stage(self, stage: str) -> dict[str, Any]:
        """Return merged client kwargs: global defaults overridden by stage-specific settings."""
        return {**self.global_kwargs, **self.stages.get(stage, {})}


class UncertaintyCorrectionModelConfig(BaseModel):
    """Configuration for a single onnx uncertainty-correction model."""

    model_path: str
    n_inputs: int
    min_value: float
    max_value: float


class UncertaintyCorrectionColumnConfig(BaseModel):
    """Configuration for correcting one nested source column with a model."""

    source_column: str
    model: str
    input_columns: list[str]
    output_column: str


class UncertaintyCorrectionConfig(BaseModel):
    """Configuration for uncertainty correction."""

    models: dict[str, UncertaintyCorrectionModelConfig] = {}
    collections: dict[str, dict[str, UncertaintyCorrectionColumnConfig]] = {}

    @model_validator(mode="after")
    def _validate_model_inputs(self) -> UncertaintyCorrectionConfig:
        for collection_name, collection_columns in self.collections.items():
            for column_name, column_config in collection_columns.items():
                if len(column_config.input_columns) != self.models[column_config.model].n_inputs:
                    raise ValueError(
                        f"Collection '{collection_name}', column '{column_name}' "
                        f"has {len(column_config.input_columns)} input columns, "
                        f"but model '{column_config.model}' expects "
                        f"{self.models[column_config.model].n_inputs} inputs."
                    )
        return self


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration, combining all stage and catalog settings."""

    run: RunConfig
    stages: StagesConfig = StagesConfig()
    catalogs: CatalogsConfig = CatalogsConfig()
    nested: NestedConfigs = NestedConfigs()
    collections: CollectionsConfig = CollectionsConfig()
    crossmatch: CrossmatchConfig = CrossmatchConfig()
    public_files: PublicFilesConfig = PublicFilesConfig()
    dask: DaskConfig = DaskConfig()
    uncertainty_correction: UncertaintyCorrectionConfig = UncertaintyCorrectionConfig()

    def enabled_catalogs(self, filter: list[str] | None = None) -> dict[str, CatalogConfig]:
        """Return configs for enabled catalogs, optionally filtered to a subset by name."""
        names = self.catalogs.enabled
        if filter is not None:
            names = [n for n in names if n in filter]
        result = {}
        for name in names:
            if name not in self.catalogs.configs:
                raise ValueError(f"Catalog '{name}' is listed in catalogs.enabled but has no config section")
            result[name] = self.catalogs.configs[name]
        return result

    def enabled_nestings(self, filter: list[str] | None = None) -> dict[str, NestedConfig]:
        """Return configs for enabled nested catalogs, optionally filtered to a subset by name."""
        names = self.nested.enabled if self.nested.enabled is not None else list(self.nested.configs.keys())
        if filter is not None:
            names = [n for n in names if n in filter]
        return {n: self.nested.configs[n] for n in names if n in self.nested.configs}

    def enabled_collections(self, filter: list[str] | None = None) -> dict[str, CollectionConfig]:
        """Return configs for enabled collections, optionally filtered to a subset by name."""
        names = (
            self.collections.enabled
            if self.collections.enabled is not None
            else list(self.collections.configs.keys())
        )
        if filter is not None:
            names = [n for n in names if n in filter]
        return {n: self.collections.configs[n] for n in names if n in self.collections.configs}


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base. Dicts are merged; all other types are replaced."""
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _load_builtin_defaults() -> dict:
    ref = importlib.resources.files("rubin_dash").joinpath("default_config.toml")
    with ref.open("rb") as f:
        return tomllib.load(f)


def load_config(paths: str | PathLike[str] | Iterable[str | PathLike[str]]) -> PipelineConfig:
    """Load pipeline config by merging built-in defaults with one or more TOML files.

    Files are applied left to right; later files override earlier ones.
    """
    if isinstance(paths, str | PathLike):
        paths = [paths]

    merged = _load_builtin_defaults()
    for path in paths:
        with open(path, "rb") as f:
            data = tomllib.load(f)
        merged = _deep_merge(merged, data)

    return PipelineConfig.model_validate(merged)
