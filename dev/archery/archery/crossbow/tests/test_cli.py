from click.testing import CliRunner
import pytest

from archery.crossbow.cli import crossbow


def test_crossbow_submit():
    runner = CliRunner()
    result = runner.invoke(crossbow, ['submit', '--dry-run', '-g', 'wheel'])
    assert result.exit_code == 0


@pytest.mark.integration
def test_crossbow_status():
    runner = CliRunner()
    result = runner.invoke(crossbow, ['status', 'build-1'])
    assert result.exit_code == 0


@pytest.mark.integration
def test_crossbow_check_config():
    runner = CliRunner()
    result = runner.invoke(crossbow, ['check-config'])
    assert result.exit_code == 0


@pytest.mark.integration
def test_crossbow_latest_prefix():
    runner = CliRunner()
    result = runner.invoke(crossbow, ['latest-prefix', 'build'])
    assert result.exit_code == 0


@pytest.mark.integration
def test_crossbow_email_report():
    runner = CliRunner()
    result = runner.invoke(crossbow, ['report', '--dry-run', 'build-1'])
    assert result.exit_code == 0


@pytest.mark.integration
def test_crossbow_download_artifacts():
    runner = CliRunner()
    result = runner.invoke(
        crossbow, ['download-artifacts', '--dry-run', 'build-1']
    )
    assert result.exit_code == 0
