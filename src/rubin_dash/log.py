from __future__ import annotations

import logging
import sys
from pathlib import Path


def setup_logging(log_file: Path | None = None) -> None:
    """Configure the rubin_dash logger: plain stdout + optional timestamped file.

    Stdout always receives INFO+ messages as plain text. When log_file is given,
    INFO+ messages are also written there with ISO timestamps.
    """
    logger = logging.getLogger("rubin_dash")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(stream_handler)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-8s %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        logger.addHandler(file_handler)
