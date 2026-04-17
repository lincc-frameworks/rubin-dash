import rubin_dash


def test_version():
    """Check to see that we can get the package version"""
    assert rubin_dash.__version__ is not None
