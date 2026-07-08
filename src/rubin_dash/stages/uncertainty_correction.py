import json
import logging
from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory

import lsdb
import nested_pandas as npd
import numpy as np
import pandas as pd
from hats.io.validation import is_valid_catalog
from onnxruntime import InferenceSession
from upath import UPath

from rubin_dash.config import (
    PipelineConfig,
    UncertaintyCorrectionColumnConfig,
    UncertaintyCorrectionModelConfig,
)
from rubin_dash.utils.dask_client import dask_client

logger = logging.getLogger(__name__)

STAGE = "uncertainty_correction"

# Directory suffix appended to the source collection name for the corrected output.
_CORRECTED_SUFFIX = "_uncertainty_corrected"


def run_uncertainty_correction(cfg: PipelineConfig, collection_filter: list[str] | None = None) -> None:
    """Apply Uncle Val uncertainty correction to configured HATS collections."""
    uc_cfg = cfg.uncertainty_correction

    collections = uc_cfg.collections
    if collection_filter is not None:
        collections = {name: c for name, c in collections.items() if name in collection_filter}

    with TemporaryDirectory(prefix="uncle_val_models") as tmpdir:
        local_model_paths = _download_models(Path(tmpdir), uc_cfg.models)
        visit_detector_table = _load_visit_detector_table(cfg)

        with dask_client(cfg.dask.for_stage(STAGE)) as client:
            local_model_paths_scatter, visit_detector_scatter = client.scatter(
                [local_model_paths, visit_detector_table], broadcast=True
            )

            for collection_name, column_cfgs in collections.items():
                logger.info("Starting uncertainty correction for %s...", collection_name)
                _uncertainty_correction_catalog(
                    cfg,
                    collection_name=collection_name,
                    column_cfgs=column_cfgs,
                    model_cfgs=uc_cfg.models,
                    local_model_paths=local_model_paths_scatter,
                    visit_detector_table=visit_detector_scatter,
                )


def _uncertainty_correction_catalog(
    cfg: PipelineConfig,
    *,
    collection_name: str,
    column_cfgs: dict[str, UncertaintyCorrectionColumnConfig],
    model_cfgs: dict[str, UncertaintyCorrectionModelConfig],
    local_model_paths,
    visit_detector_table,
) -> None:
    hats_dir = cfg.run.hats_dir
    catalog = lsdb.open_catalog(hats_dir / collection_name, columns="all")
    catalog_name = catalog.hc_structure.catalog_name

    # Keep the catalog's own name identical to the source; only the collection
    # directory it lives under changes.
    output_path = hats_dir / f"{collection_name}{_CORRECTED_SUFFIX}" / catalog_name
    if cfg.run.resume and is_valid_catalog(output_path):
        logger.info("Skipping '%s' — corrected catalog already exists.", collection_name)
        return

    corrected = catalog.map_partitions(
        _apply_uncle_val_to_partition,
        column_cfgs=column_cfgs,
        model_cfgs=model_cfgs,
        local_model_paths=local_model_paths,
        visit_detector_table=visit_detector_table,
    )

    corrected.write_catalog(
        output_path,
        catalog_name=catalog_name,
        overwrite=not cfg.run.resume,
        resume=cfg.run.resume,
    )


def _apply_uncle_val_to_partition(
    nf: npd.NestedFrame,
    *,
    column_cfgs: dict[str, UncertaintyCorrectionColumnConfig],
    model_cfgs: dict[str, UncertaintyCorrectionModelConfig],
    local_model_paths: dict[str, Path],
    visit_detector_table: npd.NestedFrame,
) -> npd.NestedFrame:
    base_columns = []
    source_subcolumns = defaultdict(list)
    for col_cfg in column_cfgs.values():
        for col in col_cfg.input_columns:
            if col in nf.base_columns and col not in base_columns:
                base_columns.append(col)
                continue
            if (
                col in nf.all_columns[col_cfg.source_column]
                and col not in source_subcolumns[col_cfg.source_column]
            ):
                source_subcolumns[col_cfg.source_column].append(col)

    for source_column, subcols in source_subcolumns.items():
        nf = _add_corrected_error_columns(
            nf,
            source_column=source_column,
            base_columns=base_columns,
            source_subcolumns=subcols,
            column_cfgs=column_cfgs,
            model_cfgs=model_cfgs,
            local_model_paths=local_model_paths,
            visit_detector_table=visit_detector_table,
        )
    return nf


def _run_uncle_val_model(model_path: Path, inputs: np.ndarray, *, batch_size: int = 1 << 15) -> np.ndarray:
    session = InferenceSession(str(model_path))

    expected_inputs = json.loads(session.get_modelmeta().custom_metadata_map["input_names"])
    if len(expected_inputs) != inputs.shape[1]:
        raise ValueError(
            f"Model at {model_path} expects {len(expected_inputs)} inputs, but got {inputs.shape[1]}"
        )

    outputs = []
    for start in range(0, len(inputs), batch_size):
        batch = inputs[start : start + batch_size]
        # Flatten per-batch: session.run returns a column vector (n, 1), and
        # reshaping here (rather than once at the end) keeps every chunk 1-D
        # so a shape hiccup on one batch can't break concatenation of the rest.
        outputs.append(session.run(["output"], {"input": batch})[0].reshape(-1))
    return np.concatenate(outputs)


def _add_corrected_error_columns(
    nf: npd.NestedFrame,
    *,
    source_column: str,
    base_columns: list[str],
    source_subcolumns: list[str],
    column_cfgs: dict[str, UncertaintyCorrectionColumnConfig],
    model_cfgs: dict[str, UncertaintyCorrectionModelConfig],
    local_model_paths: dict[str, Path],
    visit_detector_table: npd.NestedFrame,
) -> npd.NestedFrame:
    flat_frame = nf.drop(
        columns=[
            f"{source_column}.{subcol}"
            for subcol in nf.all_columns[source_column]
            if subcol not in source_subcolumns
        ]
    )
    flat_frame = flat_frame[base_columns + [source_column]]
    flat_frame = flat_frame.explode(source_column)
    input_frame = flat_frame.join(visit_detector_table, on=["visit", "detector"], how="inner")
    if len(flat_frame) != len(input_frame):
        raise ValueError("Source table has some visit/detector pairs missing from the visit_detector table.")

    for col_cfg in column_cfgs.values():
        if col_cfg.source_column != source_column:
            continue
        model_cfg = model_cfgs[col_cfg.model]

        inputs = input_frame[col_cfg.input_columns].to_numpy(dtype=np.float32)
        uu = pd.Series(
            _run_uncle_val_model(local_model_paths[col_cfg.model], inputs),
            index=input_frame.index,
        )
        flag = (uu < model_cfg.min_value) | (uu > model_cfg.max_value)
        uu = uu.clip(lower=model_cfg.min_value, upper=model_cfg.max_value)

        nf[f"{source_column}.{col_cfg.output_column}"] = uu * nf[f"{source_column}.psfFlux"]
        nf[f"{source_column}.{col_cfg.output_column}_flag"] = flag

    return nf


def _download_models(root: Path, model_cfgs: dict[str, UncertaintyCorrectionModelConfig]) -> dict[str, Path]:
    paths = {}
    for name, model_cfg in model_cfgs.items():
        upath = UPath(model_cfg.model_path)
        paths[name] = root / f"{name}.onnx"
        with open(paths[name], "wb") as f:
            f.write(upath.read_bytes())
    return paths


def _polar_from_xy(x, y):
    rho = np.hypot(x, y)
    angle = np.arctan2(y, x)
    cos_phi = np.cos(angle)
    sin_phi = np.sin(angle)
    return rho, cos_phi, sin_phi


def _produce_detector_coord_table() -> npd.NestedFrame:
    from lsst.afw import cameraGeom
    from lsst.obs.lsst import LsstCam

    camera = LsstCam().getCamera()
    detectors = defaultdict(list)
    for det in camera:
        detector_id = det.getId()
        focal_plane = det.getCenter(cameraGeom.FOCAL_PLANE)
        rho, cos_phi, sin_phi = _polar_from_xy(focal_plane.x, focal_plane.y)

        detectors["detector_id"].append(detector_id)
        detectors["detector_rho"].append(np.float32(rho))
        detectors["detector_cos_phi"].append(np.float32(cos_phi))
        detectors["detector_sin_phi"].append(np.float32(sin_phi))

    nf = npd.NestedFrame.from_dict(detectors)
    nf = nf.set_index("detector_id")
    return nf


def _fix_null_seeing(nf: npd.NestedFrame) -> npd.NestedFrame:
    mean_seeing = nf["seeing"].mean(skipna=True)
    nf["seeing"] = nf["seeing"].fillna(mean_seeing)
    return nf


def _add_detector_coords(nf: npd.NestedFrame) -> npd.NestedFrame:
    detector_coord = _produce_detector_coord_table()
    nf: npd.NestedFrame = pd.merge(nf, detector_coord, left_on="detector_id", right_index=True)
    return nf


def _one_hot_encode_band(nf: npd.NestedFrame) -> npd.NestedFrame:
    nf = nf.copy()
    for band in "ugrizy":
        nf[f"is_{band}_band"] = np.asarray(nf["band"] == band, dtype=np.float32)
    nf = nf.drop(columns=["band"])
    return nf


def _load_visit_detector_table(cfg: PipelineConfig) -> npd.NestedFrame:
    for dataset in cfg.public_files.datasets:
        if dataset.type == "visit_detector_table":
            path = cfg.run.public_files_dir / dataset.name
            break
    else:
        raise ValueError("No visit_detector_table dataset found in public_files.datasets")
    upath = UPath(path)

    nf = npd.read_parquet(
        upath,
        columns=[
            "visit_id",
            "detector_id",
            "skyBg",
            "seeing",
            "expTime",
            "band",
        ],
    )
    nf = _fix_null_seeing(nf)
    nf = _one_hot_encode_band(nf)
    nf = _add_detector_coords(nf)

    # Renamed to match the join columns used by the forced-source tables ("visit"/"detector").
    nf = nf.rename(columns={"visit_id": "visit", "detector_id": "detector"})
    nf = nf.set_index(["visit", "detector"])
    return nf
