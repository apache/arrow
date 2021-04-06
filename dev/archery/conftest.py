import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--enable-integration",
        action="store_true",
        default=False,
        help="run slow tests"
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        (
            "integration: mark test as integration tests involving more "
            "extensive setup (only used for crossbow at the moment)"
        )
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--enable-integration"):
        return
    marker = pytest.mark.skip(reason="need --enable-integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(marker)
