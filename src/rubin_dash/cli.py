# ruff: noqa: B008
from __future__ import annotations

import os
import socket
import subprocess
from pathlib import Path

import typer

from rubin_dash.config import load_config

app = typer.Typer(help="DASH Import Pipeline — convert Rubin DRP outputs to HATS catalogs.")


@app.command()
def run(
    config_paths: list[Path] = typer.Option(
        ...,
        "--config",
        "-c",
        help="TOML config file(s). Specify multiple times to layer overrides (left to right).",
    ),
    stages: str | None = typer.Option(
        None, "--stages", help="Comma-separated list of stages to run (e.g. butler,import,postprocess)."
    ),
    from_stage: str | None = typer.Option(
        None, "--from-stage", help="Run all enabled stages starting from this one."
    ),
    catalogs: str | None = typer.Option(
        None, "--catalogs", help="Comma-separated catalog names to process (e.g. dia_object,object)."
    ),
    nestings: str | None = typer.Option(
        None,
        "--nestings",
        help="Comma-separated nested catalog names to build (e.g. object_lc,dia_object_lc).",
    ),
    collections: str | None = typer.Option(
        None, "--collections", help="Comma-separated collection names to build (e.g. object_collection)."
    ),
) -> None:
    """Run the DASH pipeline."""
    from rubin_dash.pipeline import run_pipeline

    cfg = load_config(config_paths)
    run_pipeline(cfg, stages, from_stage, catalogs, nestings, collections)


@app.command()
def notebook(
    port: int = typer.Option(8769, "--port", "-p", help="Port for the Jupyter notebook server."),
    login_node: str = typer.Option(
        "s3dflogin.slac.stanford.edu",
        "--login-node",
        help="SLAC login node hostname for the SSH tunnel.",
    ),
) -> None:
    """Start a Jupyter notebook server and print the SSH tunnel command to reach it."""
    user = os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown"
    local_host = socket.gethostname().split(".")[0]
    jump_host = _detect_ssh_client_host()

    typer.echo("\nTo connect from your laptop, run:\n")
    if jump_host:
        typer.echo(
            f"  ssh -J {user}@{login_node},{user}@{jump_host} \\\n"
            f"      -L {port}:localhost:{port} \\\n"
            f"      {user}@{local_host}\n"
        )
    else:
        typer.echo(
            f"  ssh -J {user}@{login_node} \\\n"
            f"      -L {port}:localhost:{port} \\\n"
            f"      {user}@{local_host}\n"
        )
        typer.echo(
            "  (Could not detect intermediate jump host — SSH_CLIENT not set. "
            "Add the iana machine manually if needed.)\n"
        )

    typer.echo("Starting Jupyter...\n")
    subprocess.run(["jupyter", "notebook", "--no-browser", f"--port={port}"])


def _detect_ssh_client_host() -> str | None:
    """Return the short hostname of the machine that SSH'd into this one, if detectable."""
    ssh_client = os.environ.get("SSH_CLIENT", "")
    if not ssh_client:
        return None
    client_ip = ssh_client.split()[0]
    try:
        fqdn = socket.gethostbyaddr(client_ip)[0]
        return fqdn.split(".")[0]
    except (socket.herror, OSError):
        # Fall back to the raw IP if reverse DNS fails
        return client_ip
