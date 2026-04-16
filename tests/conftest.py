"""Root conftest — installed before any collection so LSST stubs are in place
when pytest imports src/ modules for --doctest-modules."""
import sys
from unittest.mock import MagicMock

# Stub out LSST packages that are only available on USDF.
# MagicMock satisfies attribute access (e.g. Butler, ResourcePath) automatically.
_LSST_MODS = [
    "lsst",
    "lsst.daf",
    "lsst.daf.butler",
    "lsst.resources",
]
for _mod in _LSST_MODS:
    sys.modules.setdefault(_mod, MagicMock())
