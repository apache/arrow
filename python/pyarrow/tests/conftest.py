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

import pytest

try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib  # py2 compat


groups = [
    'hdfs',
    'large_memory',
    'orc',
    'parquet',
    'plasma',
    's3',
    'tensorflow'
]


defaults = {
    'hdfs': False,
    'large_memory': False,
    'orc': False,
    'parquet': False,
    'plasma': False,
    's3': False,
    'tensorflow': False
}

try:
    import pyarrow.orc # noqa
    defaults['orc'] = True
except ImportError:
    pass

try:
    import pyarrow.parquet  # noqa
    defaults['parquet'] = True
except ImportError:
    pass

try:
    import pyarrow.plasma as plasma  # noqa
    defaults['plasma'] = True
except ImportError:
    pass


try:
    import tensorflow  # noqa
    defaults['tensorflow'] = True
except ImportError:
    pass


def pytest_configure(config):
    pass


def pytest_addoption(parser):
    for group in groups:
        parser.addoption('--{0}'.format(group), action='store_true',
                         default=defaults[group],
                         help=('Enable the {0} test group'.format(group)))

    for group in groups:
        parser.addoption('--disable-{0}'.format(group), action='store_true',
                         default=False,
                         help=('Disable the {0} test group'.format(group)))

    for group in groups:
        parser.addoption('--only-{0}'.format(group), action='store_true',
                         default=False,
                         help=('Run only the {0} test group'.format(group)))

    parser.addoption('--runslow', action='store_true',
                     default=False, help='run slow tests')


def pytest_collection_modifyitems(config, items):
    if not config.getoption('--runslow'):
        skip_slow = pytest.mark.skip(reason='need --runslow option to run')

        for item in items:
            if 'slow' in item.keywords:
                item.add_marker(skip_slow)


def pytest_runtest_setup(item):
    only_set = False

    for group in groups:
        only_flag = '--only-{0}'.format(group)
        disable_flag = '--disable-{0}'.format(group)
        flag = '--{0}'.format(group)

        if item.config.getoption(only_flag):
            only_set = True
        elif getattr(item.obj, group, None):
            if (item.config.getoption(disable_flag) or
                    not item.config.getoption(flag)):
                pytest.skip('{0} NOT enabled'.format(flag))

    if only_set:
        skip_item = True
        for group in groups:
            only_flag = '--only-{0}'.format(group)
            if (getattr(item.obj, group, False) and
                    item.config.getoption(only_flag)):
                skip_item = False

        if skip_item:
            pytest.skip('Only running some groups with only flags')


@pytest.fixture
def tempdir(tmpdir):
    # convert pytest's LocalPath to pathlib.Path
    return pathlib.Path(tmpdir.strpath)


@pytest.fixture(scope='session')
def datadir():
    return pathlib.Path(__file__).parent / 'data'
