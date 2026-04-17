
# rubin-dash

**D**RP **A**fterburner for **S**uper **HATS** — converts Rubin DRP outputs into
[HATS](https://hats.readthedocs.io/) catalogs suitable for use with
[lsdb](https://lsdb.readthedocs.io/).

[![Template](https://img.shields.io/badge/Template-LINCC%20Frameworks%20Python%20Project%20Template-brightgreen)](https://lincc-ppt.readthedocs.io/en/latest/)
[![PyPI](https://img.shields.io/pypi/v/rubin-dash?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/rubin-dash/)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/lincc-frameworks/rubin-dash/smoke-test.yml)](https://github.com/lincc-frameworks/rubin-dash/actions/workflows/smoke-test.yml)
[![Codecov](https://codecov.io/gh/lincc-frameworks/rubin-dash/branch/main/graph/badge.svg)](https://codecov.io/gh/lincc-frameworks/rubin-dash)
[![Read The Docs](https://img.shields.io/readthedocs/rubin-dash)](https://rubin-dash.readthedocs.io/)

## Overview

The pipeline runs a sequence of stages that read from a Butler repository and
write HATS catalogs to an output directory:

| Stage | Description                                           |
|---|-------------------------------------------------------|
| `butler` | Find catalog parquet files from the Butler repository |
| `raw_sizes` | Measure raw parquet file sizes                        |
| `import` | Import catalogs into HATS format                      |
| `postprocess` | Post-process imported catalogs                        |
| `nesting` | Build nested (light-curve) catalogs                   |
| `collections` | Generate HATS collections                             |
| `crossmatch` | Cross-match against external surveys (e.g. ZTF, PS1)  |
| `generate_json` | Generate JSON metadata for the HATS collections       |

## Setting up the environment

This pipeline requires IDAC access and is normally run on USDF SLAC nodes. It
cannot be run on the login node. It is *highly recommended* to use `tmux` or `screen` so
you can detach and reattach without losing your session. The pipeline typically
takes at least ~5h and can take closer to ~15h.

### Request a reserved node


Your connection path should look like this:

```mermaid
graph LR
    L["<i>login node</i>"] --> T("<code>tmux/screen</code>")
    T --> I["<i>interactive node</i>"]
    I --> R["<i>reserved node</i>"]
style T fill:lightblue,stroke:darkblue,stroke-width:2px
```
From an interactive node, request a reserved node:

```shell
srun --pty --exclusive --nodes=1 --time=48:00:00 \
     --partition=milano --account=rubin:commissioning bash
```

Do not exit the reserved node shell directly — use `tmux detach` or screen's `ctrl+a -> d` instead so the
job keeps running.

### Load the LSST stack

```shell
source /sdf/group/rubin/sw/loadLSST.sh
setup lsst_distrib
```

### Install rubin-dash

```shell
pip install rubin-dash
```

## Running the pipeline

### 1. Create a config file

The package ships a `default_config.toml` with sensible defaults for all
catalogs, nested catalogs, collections, crossmatch surveys, and Dask settings.
Your config file is merged on top of those defaults — you only need to specify
what changes for your run.

Copy `example_config.toml` and fill in the `[run]` section. The values come
from the JIRA ticket associated with the weekly release. For example, the
collection string `LSSTCam/runs/DRP/20250417_20250921/w_2025_49/DM-53545`
breaks down as:

```toml
[run]
instrument = "LSSTCam"
repo       = "/repo/embargo"         # Butler repo path
version    = "w_2025_49"
collection = "DM-53545"
output_dir = "/sdf/data/rubin/shared/lsdb_commissioning"
run        = "20250417_20250921"      # optional — omit for releases without a run segment
```

#### Overriding stages

By default all stages run. Restrict to a subset:

```toml
[stages]
enabled = ["butler", "raw_sizes", "import", "postprocess"]
```

#### Overriding catalogs

By default all six catalogs are processed: `dia_object`, `dia_source`,
`dia_object_forced_source`, `object`, `source`, `object_forced_source`.
Restrict to a subset:

```toml
[catalogs]
enabled = ["dia_object", "object"]
```

Override settings for a specific catalog:

```toml
[catalogs.object]
chunksize = 100_000   # DimensionParquetReader batch size (default 250_000 for object)

[catalogs.object.import_args]
pixel_threshold = 500_000   # override any hats-import argument
```

Add a custom catalog not in the defaults (all fields required):

```toml
[catalogs.my_catalog]
dims            = ["tract"]
group_by        = ["tract"]
flux_columns    = []
add_mjds        = false
use_schema_file = false
chunksize       = 500_000

[catalogs.my_catalog.import_args]
ra_column       = "ra"
dec_column      = "dec"
catalog_type    = "object"
pixel_threshold = 1_000_000
```

#### Overriding nested catalogs

The defaults define two nested catalogs (`dia_object_lc` and `object_lc`).
Override settings or restrict which ones are built:

```toml
[nested]
enabled = ["object_lc"]   # omit to run all

[nested.object_lc]
pixel_threshold       = 20_000   # override any field
highest_healpix_order = 10
```

#### Overriding collections

```toml
[collections]
enabled = ["object_collection"]   # omit to run all

[collections.object_collection]
margin_threshold = 10.0
```

#### Overriding crossmatch surveys

The defaults cross-match against ZTF DR22 and PS1. Add, remove, or reconfigure:

```toml
# Disable all crossmatches by leaving surveys empty
[crossmatch]

# Or override a survey's search radius
[crossmatch.surveys.ztf_dr22]
radius_arcsec = 0.5
```

#### Overriding Dask settings

Global settings apply to all stages; stage-specific sections override them for
that stage only:

```toml
[dask]
n_workers        = 32
threads_per_worker = 1
memory_limit     = "16GB"

[dask.stages.nesting]
n_workers    = 8
memory_limit = "32GB"
```

#### Layering multiple config files

You can split settings across files and layer them at run time — later files
override earlier ones:

```shell
rubin-dash run --config base.toml --config this_week.toml --config overrides.toml
```

### 2. Run the full pipeline

```shell
rubin-dash run --config my_config.toml
```

### CLI options

```
rubin-dash run --config CONFIG [--config CONFIG ...]
               [--stages butler,import,postprocess]
               [--from-stage STAGE]
               [--catalogs dia_object,object]
               [--nestings object_lc]
               [--collections object_collection]
```

| Option | Description |
|---|---|
| `--config` | TOML config file. Repeat to layer overrides (later files win). |
| `--stages` | Comma-separated list of stages to run. |
| `--from-stage` | Run all enabled stages starting from this one. |
| `--catalogs` | Restrict to a subset of catalogs. |
| `--nestings` | Restrict to specific nested catalogs. |
| `--collections` | Restrict to specific collections. |

Examples:

```shell
# Re-run only the import and postprocess stages
rubin-dash run --config my_config.toml --stages import,postprocess

# Resume from the nesting stage onward
rubin-dash run --config my_config.toml --from-stage nesting

# Layer a base config with per-run overrides
rubin-dash run --config base.toml --config overrides.toml
```

### 3. Interactive notebook access

To open the notebooks interactively from within the processing environment:

```shell
rubin-dash notebook --port 8769
```

This starts a Jupyter server and prints the SSH tunnel command you need to run
on your laptop to forward the port. It will look something like:

```shell
ssh -J user@sdflogin003.slac.stanford.edu,user@sdfiana004 \
    -L 8769:localhost:8769 \
    user@sdfmilan005
```

### 4. Rerunning a single stage after a failure

If the pipeline fails partway through, you can rerun from a specific stage:

```shell
rubin-dash run --config my_config.toml --from-stage import
```

Or run a single stage in isolation:

```shell
rubin-dash run --config my_config.toml --stages import
```

If you need to debug interactively, the `notebooks/` directory contains a
notebook for each stage. Run them individually after confirming the environment
variables are set. If you encounter unexpected issues with upstream data, reach
out in `#dm-algorithms-pipelines` on the Rubin Observatory Slack.

## Development

```shell
conda create -n rubin-dash python=3.11
conda activate rubin-dash
pip install -e ".[dev]"
chmod +x .setup_dev.sh
./.setup_dev.sh
```