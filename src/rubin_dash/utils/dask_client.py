from __future__ import annotations

import logging
import tempfile
from contextlib import contextmanager
from typing import Any

from dask.distributed import Client


def _resolve_silence_logs(value: Any) -> int:
    """Coerce a config-supplied log level (name or number) to a logging int.

    Accepts level names like ``"INFO"`` from TOML as well as raw ints.
    Falls back to ``INFO`` for unrecognized names.
    """
    if isinstance(value, str):
        return logging.getLevelNamesMapping().get(value.upper(), logging.INFO)
    return int(value)


@contextmanager
def dask_client(client_kwargs: dict[str, Any] | None = None):
    """Context manager that creates a Dask client with a temporary local directory.

    Args:
        client_kwargs: Keyword arguments forwarded to ``dask.distributed.Client``.
            ``local_directory`` is set automatically to a temp dir unless already provided.
            ``silence_logs`` defaults to ``INFO`` so distributed's own INFO records are
            visible — most importantly the nanny's "Worker process N was killed by
            signal M" death message, which the LocalCluster ``silence_logs=WARN`` default
            otherwise hides behind a bare "Restarting worker" warning.
    """
    kwargs = dict(client_kwargs or {})
    kwargs["silence_logs"] = _resolve_silence_logs(kwargs.get("silence_logs", logging.INFO))
    tmp = None
    if "local_directory" not in kwargs:
        tmp = tempfile.TemporaryDirectory()
        kwargs["local_directory"] = tmp.name
    client = Client(**kwargs)
    try:
        yield client
    finally:
        client.close()
        if tmp is not None:
            tmp.cleanup()
