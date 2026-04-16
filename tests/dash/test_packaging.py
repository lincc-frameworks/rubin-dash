import dash


def test_version():
    """Check to see that we can get the package version"""
    assert dash.__version__ is not None
