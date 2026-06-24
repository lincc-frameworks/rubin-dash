from __future__ import annotations

import logging
import tempfile
from contextlib import contextmanager
from typing import Any

import dask
from dask.distributed import Client

# Distributed's own log format, reused so the nanny death-message handler matches
# the rest of distributed's output.
_DISTRIBUTED_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def _enable_worker_faulthandler() -> None:
    """Turn on Python's faulthandler in spawned worker processes.

    Workers occasionally die with SIGSEGV/SIGABRT inside native libraries (pyarrow,
    healpy, lsst.resources/S3) during the split phase, which the nanny reports only as
    "killed by signal N". faulthandler dumps the crashing C-level Python stack to stderr
    at fault time, naming the library and call. ``PYTHONFAULTHANDLER`` is read at
    interpreter startup, so it must be set *before* the worker spawns — hence
    pre-spawn-environ, which is also reapplied on every nanny respawn. Merge rather than
    overwrite to preserve distributed's MALLOC_TRIM / thread-count defaults.
    """
    pre_spawn = dict(dask.config.get("distributed.nanny.pre-spawn-environ", {}) or {})
    pre_spawn.setdefault("PYTHONFAULTHANDLER", "1")
    dask.config.set({"distributed.nanny.pre-spawn-environ": pre_spawn})


def _resolve_silence_logs(value: Any) -> int:
    """Coerce a config-supplied log level (name or number) to a logging int.

    Accepts level names like ``"WARNING"`` from TOML as well as raw ints.
    Falls back to ``WARNING`` for unrecognized names.
    """
    if isinstance(value, str):
        return logging.getLevelNamesMapping().get(value.upper(), logging.WARNING)
    return int(value)


@contextmanager
def _nanny_deaths_visible():
    """Keep the nanny's INFO worker-death message visible while the rest of distributed
    is quieted to WARNING.

    The "Worker process N was killed by signal M" line that explains *why* a worker
    restarted is logged at INFO by ``distributed.nanny``. With distributed silenced to
    WARNING that line is dropped, leaving only a bare "Restarting worker". This attaches
    a dedicated INFO handler to the nanny logger (with propagation off so WARNING+ records
    aren't also emitted by distributed's shared handler) and tears it down on exit.
    """
    nanny_logger = logging.getLogger("distributed.nanny")
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(_DISTRIBUTED_LOG_FORMAT))

    prev_propagate = nanny_logger.propagate
    nanny_logger.addHandler(handler)
    nanny_logger.propagate = False
    try:
        yield
    finally:
        nanny_logger.removeHandler(handler)
        nanny_logger.propagate = prev_propagate


@contextmanager
def dask_client(client_kwargs: dict[str, Any] | None = None):
    """Context manager that creates a Dask client with a temporary local directory.

    Args:
        client_kwargs: Keyword arguments forwarded to ``dask.distributed.Client``.
            ``local_directory`` is set automatically to a temp dir unless already provided.
            ``silence_logs`` defaults to ``WARNING`` to suppress distributed's noisy INFO
            output. The nanny's "Worker process N was killed by signal M" death message
            (logged at INFO) is kept visible regardless, so worker restarts still explain
            themselves instead of showing a bare "Restarting worker".
    """
    kwargs = dict(client_kwargs or {})
    kwargs["silence_logs"] = _resolve_silence_logs(kwargs.get("silence_logs", logging.WARNING))
    _enable_worker_faulthandler()
    tmp = None
    if "local_directory" not in kwargs:
        tmp = tempfile.TemporaryDirectory()
        kwargs["local_directory"] = tmp.name
    client = Client(**kwargs)
    with _nanny_deaths_visible():
        try:
            yield client
        finally:
            client.close()
            if tmp is not None:
                tmp.cleanup()
