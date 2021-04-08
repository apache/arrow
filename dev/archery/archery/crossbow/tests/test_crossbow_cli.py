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

from click.testing import CliRunner
import pytest

from archery.crossbow.cli import crossbow


def test_crossbow_submit():
    runner = CliRunner()
    result = runner.invoke(crossbow, ['submit', '--dry-run', '-g', 'wheel'])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_crossbow_commnds():
    runner = CliRunner()

    result = runner.invoke(crossbow, ['check-config'])
    assert result.exit_code == 0

    result = runner.invoke(crossbow, ['latest-prefix', 'build'])
    assert result.exit_code == 0
    build_id = result.stdout.strip()

    result = runner.invoke(crossbow, ['status', build_id])
    assert result.exit_code == 0

    result = runner.invoke(crossbow, ['report', '--dry-run', build_id])
    assert result.exit_code == 0

    result = runner.invoke(
        crossbow, ['download-artifacts', '--dry-run', build_id]
    )
    assert result.exit_code == 0
