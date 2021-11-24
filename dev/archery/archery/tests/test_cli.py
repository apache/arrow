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

from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

from archery.cli import archery


@patch("archery.linking.check_dynamic_library_dependencies")
def test_linking_check_dependencies(fn):
    args = [
        "linking",
        "check-dependencies",
        "-a", "libarrow",
        "-d", "libcurl",
        "somelib.so"
    ]
    result = CliRunner().invoke(archery, args)
    assert result.exit_code == 0
    fn.assert_called_once_with(
        Path('somelib.so'), allowed={'libarrow'}, disallowed={'libcurl'}
    )
