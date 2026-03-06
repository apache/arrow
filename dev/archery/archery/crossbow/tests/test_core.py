# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from archery.utils.source import ArrowSources
from archery.crossbow import Config, Queue
from archery.crossbow.core import CrossbowError, Repo, TaskAssets, TaskStatus

import pathlib
from datetime import date
from unittest import mock

import pytest
from github import GithubException


def test_config():
    src = ArrowSources.find()
    conf = Config.load_yaml(src.dev / "tasks" / "tasks.yml")
    conf.validate()


def test_task_select(request):
    conf = Config.load_yaml(pathlib.Path(
        request.node.fspath).parent / "fixtures" / "tasks.yaml")
    conf.validate()

    test_out = conf.select(tasks=["test-a-test-two"])
    assert test_out.keys() >= {"test-a-test-two"}


def test_group_select(request):
    conf = Config.load_yaml(pathlib.Path(
        request.node.fspath).parent / "fixtures" / "tasks.yaml")
    conf.validate()

    test_out = conf.select(groups=["test"])
    assert test_out.keys() >= {"test-a-test-two", "test-a-test"}


def test_group_select_blocklist(request):
    conf = Config.load_yaml(pathlib.Path(
        request.node.fspath).parent / "fixtures" / "tasks.yaml")
    conf.validate()

    # we respect the nightly blocklist
    nightly_out = conf.select(groups=["nightly"])
    assert nightly_out.keys() >= {"test-a-test", "nightly-fine"}

    # but if a task is not blocked in both groups, it shows up at least once
    test_nightly_out = conf.select(groups=["nightly", "test"])
    assert test_nightly_out.keys() >= {
        "test-a-test-two", "test-a-test", "nightly-fine"}

    # but can then over-ride by requesting the task
    test_nightly_out = conf.select(
        tasks=["nightly-not-fine", "nightly-fine"], groups=["nightly", "test"])
    assert test_nightly_out.keys() >= {
        "test-a-test-two", "test-a-test", "nightly-fine", "nightly-not-fine"}

    # and we can glob with the blocklist too!
    test_nightly_no_test_out = conf.select(groups=["nightly-no-test"])
    assert test_nightly_no_test_out.keys(
    ) >= {"nightly-fine", "nightly-not-fine"}


def test_latest_for_prefix(request):
    queue = Queue(pathlib.Path(request.node.fspath).parent)
    with mock.patch("archery.crossbow.core.Repo.repo") as mocked_repo:
        mocked_repo.branches = [
            "origin/nightly-packaging-2022-04-10-0",
            "origin/nightly-packaging-2022-04-11-0",
        ]
        with mock.patch("archery.crossbow.core.Queue.get") as mocked_get:
            queue.latest_for_prefix("nightly-packaging-2022-04-10")
            mocked_get.assert_called_once_with(
                "nightly-packaging-2022-04-10-0")

    with mock.patch("archery.crossbow.core.Repo.repo") as mocked_repo:
        mocked_repo.branches = [
            "origin/nightly-packaging-2022-04-10-0",
            "origin/nightly-packaging-2022-04-11-0",
        ]
        with mock.patch("archery.crossbow.core.Queue.get") as mocked_get:
            queue.latest_for_prefix("nightly-packaging")
            mocked_get.assert_called_once_with(
                "nightly-packaging-2022-04-11-0")


def test_github_release_not_found():
    """Test github_release returns None for 404."""
    with mock.patch.object(Repo, 'as_github_repo') as mock_repo:
        mock_repo.return_value.get_release.side_effect = GithubException(
            404, {'message': 'Not Found'}, None
        )
        repo = Repo('/tmp/test', github_token='test_token')
        result = repo.github_release('nonexistent')
        assert result is None


def test_github_pr_create():
    """Test creating a pull request via PyGithub."""
    with mock.patch.object(Repo, 'as_github_repo') as mock_repo:
        with mock.patch.object(Repo, 'default_branch_name', 'main'):
            mock_pr = mock.MagicMock()
            mock_repo.return_value.create_pull.return_value = mock_pr

            repo = Repo('/tmp/test', github_token='test_token')
            result = repo.github_pr('Test PR', head='feature',
                                    base='main', create=True)

            mock_repo.return_value.create_pull.assert_called_once()
            assert result == mock_pr


def test_github_pr_find():
    """Test finding an existing pull request."""
    with mock.patch.object(Repo, 'as_github_repo') as mock_repo:
        mock_pr = mock.MagicMock()
        mock_pr.title = 'Test PR Title'
        mock_repo.return_value.get_pulls.return_value = [mock_pr]

        repo = Repo('/tmp/test', github_token='test_token')
        result = repo.github_pr('Test PR', head='feature', base='main')

        assert result == mock_pr


def test_task_status_success():
    """Test TaskStatus with successful commit status."""
    mock_commit = mock.MagicMock()
    mock_status = mock.MagicMock()
    mock_status.statuses = [
        mock.MagicMock(state='success', target_url='http://example.com')
    ]
    mock_commit.get_combined_status.return_value = mock_status
    mock_commit.get_check_runs.return_value = []

    status = TaskStatus(mock_commit)
    assert status.combined_state == 'success'


def test_task_status_failure():
    """Test TaskStatus with failed commit status."""
    mock_commit = mock.MagicMock()
    mock_status = mock.MagicMock()
    mock_status.statuses = [
        mock.MagicMock(state='failure', target_url='http://example.com')
    ]
    mock_commit.get_combined_status.return_value = mock_status
    mock_commit.get_check_runs.return_value = []

    status = TaskStatus(mock_commit)
    assert status.combined_state == 'failure'


def test_task_status_with_check_runs():
    """Test TaskStatus with GitHub check runs."""
    mock_commit = mock.MagicMock()
    mock_status = mock.MagicMock()
    mock_status.statuses = []
    mock_commit.get_combined_status.return_value = mock_status

    mock_check = mock.MagicMock()
    mock_check.status = 'completed'
    mock_check.conclusion = 'failure'
    mock_check.html_url = 'https://example.com'
    mock_commit.get_check_runs.return_value = [mock_check]

    status = TaskStatus(mock_commit)
    assert status.combined_state == 'failure'


def test_task_status_pending():
    """Test TaskStatus with pending check runs."""
    mock_commit = mock.MagicMock()
    mock_status = mock.MagicMock()
    mock_status.statuses = []
    mock_commit.get_combined_status.return_value = mock_status

    mock_check = mock.MagicMock()
    mock_check.status = 'in_progress'
    mock_check.html_url = 'https://example.com'
    mock_commit.get_check_runs.return_value = [mock_check]

    status = TaskStatus(mock_commit)
    assert status.combined_state == 'pending'


def test_token_expiration_date():
    """Test token_expiration_date returns correct date from header."""
    with mock.patch.object(Repo, '_github_login') as mock_login:
        mock_github = mock.MagicMock()
        mock_login.return_value = mock_github
        mock_github._Github__requester.requestJsonAndCheck.return_value = (
            {'github-authentication-token-expiration': '2024-12-31 23:59:59 UTC'},
            {}
        )

        repo = Repo('/tmp/test', github_token='test_token')
        result = repo.token_expiration_date()

        assert result == date(2024, 12, 31)


def test_token_expiration_date_no_header():
    """Test token_expiration_date returns None when header is missing."""
    with mock.patch.object(Repo, '_github_login') as mock_login:
        mock_github = mock.MagicMock()
        mock_login.return_value = mock_github
        mock_github._Github__requester.requestJsonAndCheck.return_value = (
            {},
            {}
        )

        repo = Repo('/tmp/test', github_token='test_token')
        result = repo.token_expiration_date()

        assert result is None


def test_github_upload_asset_success():
    """Test successful asset upload."""
    with mock.patch.object(Repo, 'as_github_repo'):
        repo = Repo('/tmp/test', github_token='test_token')

        mock_release = mock.MagicMock()
        mock_asset = mock.MagicMock()
        mock_release.upload_asset.return_value = mock_asset

        result = repo.github_upload_asset_requests(
            mock_release, '/path/to/file', 'file.tar.gz', 'application/gzip'
        )

        assert result == mock_asset
        mock_release.upload_asset.assert_called_once()


def test_github_upload_asset_retry_on_422():
    """Test asset upload retries after deleting existing asset on 422."""
    with mock.patch.object(Repo, 'as_github_repo'):
        repo = Repo('/tmp/test', github_token='test_token')

        mock_release = mock.MagicMock()
        mock_asset = mock.MagicMock()
        mock_existing_asset = mock.MagicMock()
        mock_existing_asset.name = 'file.tar.gz'

        # First call raises 422, second succeeds
        mock_release.upload_asset.side_effect = [
            GithubException(422, {'message': 'Validation Failed'}, None),
            mock_asset
        ]
        mock_release.get_assets.return_value = [mock_existing_asset]

        with mock.patch('time.sleep'):  # Skip actual sleep
            result = repo.github_upload_asset_requests(
                mock_release, '/path/to/file', 'file.tar.gz',
                'application/gzip', max_retries=3, retry_backoff=0
            )

        assert result == mock_asset
        mock_existing_asset.delete_asset.assert_called_once()


def test_github_overwrite_release_assets_creates_new():
    """Test creating a new release when none exists."""
    with mock.patch.object(Repo, 'as_github_repo') as mock_repo_method:
        mock_repo = mock.MagicMock()
        mock_repo_method.return_value = mock_repo
        mock_release = mock.MagicMock()

        # Release doesn't exist (404)
        mock_repo.get_release.side_effect = GithubException(
            404, {'message': 'Not Found'}, None
        )
        mock_repo.create_git_release.return_value = mock_release

        repo = Repo('/tmp/test', github_token='test_token')

        with mock.patch('glob.glob', return_value=[]):
            repo.github_overwrite_release_assets(
                'v1.0.0', 'abc123', ['*.tar.gz']
            )

        mock_repo.create_git_release.assert_called_once_with(
            'v1.0.0', 'v1.0.0', '', target_commitish='abc123'
        )


def test_github_overwrite_release_assets_deletes_existing():
    """Test deleting existing release before creating new one."""
    with mock.patch.object(Repo, 'as_github_repo') as mock_repo_method:
        mock_repo = mock.MagicMock()
        mock_repo_method.return_value = mock_repo
        mock_existing_release = mock.MagicMock()
        mock_new_release = mock.MagicMock()

        mock_repo.get_release.return_value = mock_existing_release
        mock_repo.create_git_release.return_value = mock_new_release

        repo = Repo('/tmp/test', github_token='test_token')

        with mock.patch('glob.glob', return_value=[]):
            repo.github_overwrite_release_assets(
                'v1.0.0', 'abc123', ['*.tar.gz']
            )

        mock_existing_release.delete_release.assert_called_once()
        mock_repo.create_git_release.assert_called_once()


def test_task_assets_empty_patterns():
    """Test TaskAssets returns empty dict for empty artifact patterns."""
    mock_release = mock.MagicMock()
    assets = TaskAssets(mock_release, [])
    assert len(assets) == 0
    # Verify get_assets was never called (early return)
    mock_release.get_assets.assert_not_called()


def test_task_assets_no_release():
    """Test TaskAssets with None release sets patterns to None."""
    assets = TaskAssets(None, ['file.tar.gz'])
    assert assets['file.tar.gz'] is None
    assert assets.missing_patterns() == ['file.tar.gz']
    assert assets.uploaded_assets() == []


def test_task_assets_pattern_matching():
    """Test TaskAssets matches patterns to assets."""
    mock_release = mock.MagicMock()
    mock_asset = mock.MagicMock()
    mock_asset.name = 'arrow-1.0.0.tar.gz'
    mock_release.get_assets.return_value = [mock_asset]

    assets = TaskAssets(mock_release, ['arrow-1.0.0.tar.gz'])
    assert assets['arrow-1.0.0.tar.gz'] == mock_asset
    assert assets.missing_patterns() == []
    assert assets.uploaded_assets() == [mock_asset]


def test_task_assets_regex_pattern():
    """Test TaskAssets with regex patterns."""
    mock_release = mock.MagicMock()
    mock_asset = mock.MagicMock()
    mock_asset.name = 'arrow-1.0.0-linux-x86_64.tar.gz'
    mock_release.get_assets.return_value = [mock_asset]

    assets = TaskAssets(mock_release, [r'arrow-.*\.tar\.gz'])
    assert assets[r'arrow-.*\.tar\.gz'] == mock_asset


def test_task_assets_multiple_matches_error():
    """Test TaskAssets raises error when pattern matches multiple assets."""
    mock_release = mock.MagicMock()
    mock_asset1 = mock.MagicMock()
    mock_asset1.name = 'arrow-1.0.0-linux.tar.gz'
    mock_asset2 = mock.MagicMock()
    mock_asset2.name = 'arrow-1.0.0-darwin.tar.gz'
    mock_release.get_assets.return_value = [mock_asset1, mock_asset2]

    with pytest.raises(CrossbowError, match="Only a single asset should match"):
        TaskAssets(mock_release, [r'arrow-.*\.tar\.gz'])


def test_task_assets_skip_validation():
    """Test TaskAssets without pattern validation returns all assets."""
    mock_release = mock.MagicMock()
    mock_asset1 = mock.MagicMock()
    mock_asset1.name = 'file1.tar.gz'
    mock_asset2 = mock.MagicMock()
    mock_asset2.name = 'file2.tar.gz'
    mock_release.get_assets.return_value = [mock_asset1, mock_asset2]

    assets = TaskAssets(mock_release, ['unused-pattern'],
                        validate_patterns=False)
    assert assets['file1.tar.gz'] == mock_asset1
    assert assets['file2.tar.gz'] == mock_asset2
