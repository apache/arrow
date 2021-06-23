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

import pathlib

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


@pytest.fixture
def load_fixture(request):
    current_test_directory = pathlib.Path(request.node.fspath).parent

    def decoder(path):
        with path.open('r') as fp:
            if path.suffix == '.json':
                import json
                return json.load(fp)
            elif path.suffix == '.yaml':
                import yaml
                return yaml.load(fp)
            else:
                return fp.read()

    def loader(name, decoder=decoder):
        path = current_test_directory / 'fixtures' / name
        return decoder(path)

    return loader
