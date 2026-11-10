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

from datetime import date, timedelta
from unittest import mock

from click.testing import CliRunner
import pytest

from archery.crossbow.cli import crossbow
from archery.utils.git import git


@pytest.mark.integration
def test_crossbow_submit(tmp_path):
    runner = CliRunner()

    def invoke(*args):
        return runner.invoke(crossbow, ['--queue-path', str(tmp_path), *args])

    # initialize an empty crossbow repository
    git.run_cmd("init", str(tmp_path))
    git.run_cmd("-C", str(tmp_path), "remote", "add", "origin",
                "https://github.com/dummy/repo")
    git.run_cmd("-C", str(tmp_path), "commit", "-m", "initial",
                "--allow-empty")

    result = invoke('check-config')
    assert result.exit_code == 0

    result = invoke('submit', '--no-fetch', '--no-push', '-g', 'wheel')
    assert result.exit_code == 0


def test_crossbow_notify_token_expiration(monkeypatch):
    # The report carries the From/To headers, so the sender and recipient
    # options have to reach it: send_email() only passes the rendered message
    # on to smtplib. Dropping them left this command raising before it ever
    # built a message.
    queue = mock.MagicMock()
    queue.token_expiration_date.return_value = date.today() + timedelta(days=7)
    monkeypatch.setattr('archery.crossbow.cli.Queue',
                        mock.MagicMock(return_value=queue))
    monkeypatch.setattr('archery.crossbow.cli.Repo', mock.MagicMock())

    result = CliRunner().invoke(crossbow, [
        'notify-token-expiration',
        '--sender-name', 'Crossbow Reporter',
        '--sender-email', 'sender@arrow.apache.org',
        '--recipient-email', 'recipient@arrow.apache.org',
    ])

    assert result.exit_code == 0, result.exception
    assert 'From: Crossbow Reporter <sender@arrow.apache.org>' in result.output
    assert 'To: recipient@arrow.apache.org' in result.output
