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

import os
import pytest
import hypothesis as h

try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib  # py2 compat


# setup hypothesis profiles
h.settings.register_profile('ci', max_examples=1000)
h.settings.register_profile('dev', max_examples=10)
h.settings.register_profile('debug', max_examples=10,
                            verbosity=h.Verbosity.verbose)

# load default hypothesis profile, either set HYPOTHESIS_PROFILE environment
# variable or pass --hypothesis-profile option to pytest, to see the generated
# examples try: pytest pyarrow -sv --only-hypothesis --hypothesis-profile=debug
h.settings.load_profile(os.environ.get('HYPOTHESIS_PROFILE', 'dev'))


groups = [
    'cython',
    'hypothesis',
    'gandiva',
    'hdfs',
    'large_memory',
    'orc',
    'pandas',
    'parquet',
    'plasma',
    's3',
    'tensorflow',
    'flight'
]


defaults = {
    'cython': False,
    'hypothesis': False,
    'gandiva': False,
    'hdfs': False,
    'large_memory': False,
    'orc': False,
    'pandas': False,
    'parquet': False,
    'plasma': False,
    's3': False,
    'tensorflow': False,
    'flight': False,
}

try:
    import cython  # noqa
    defaults['cython'] = True
except ImportError:
    pass

try:
    import pyarrow.gandiva # noqa
    defaults['gandiva'] = True
except ImportError:
    pass

try:
    import pyarrow.orc # noqa
    defaults['orc'] = True
except ImportError:
    pass

try:
    import pandas  # noqa
    defaults['pandas'] = True
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

try:
    import pyarrow.flight  # noqa
    defaults['flight'] = True
except ImportError:
    pass


def pytest_configure(config):
    for mark in groups:
        config.addinivalue_line(
            "markers", mark,
        )


def pytest_addoption(parser):
    def bool_env(name, default=None):
        value = os.environ.get(name.upper())
        if value is None:
            return default
        value = value.lower()
        if value in {'1', 'true', 'on', 'yes', 'y'}:
            return True
        elif value in {'0', 'false', 'off', 'no', 'n'}:
            return False
        else:
            raise ValueError('{}={} is not parsable as boolean'
                             .format(name.upper(), value))

    for group in groups:
        for flag, envvar in [('--{}', 'PYARROW_TEST_{}'),
                             ('--enable-{}', 'PYARROW_TEST_ENABLE_{}')]:
            default = bool_env(envvar.format(group), defaults[group])
            parser.addoption(flag.format(group),
                             action='store_true', default=default,
                             help=('Enable the {} test group'.format(group)))

        default = bool_env('PYARROW_TEST_DISABLE_{}'.format(group), False)
        parser.addoption('--disable-{}'.format(group),
                         action='store_true', default=default,
                         help=('Disable the {} test group'.format(group)))

        default = bool_env('PYARROW_TEST_ONLY_{}'.format(group), False)
        parser.addoption('--only-{}'.format(group),
                         action='store_true', default=default,
                         help=('Run only the {} test group'.format(group)))

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

    item_marks = {mark.name: mark for mark in item.iter_markers()}

    for group in groups:
        flag = '--{0}'.format(group)
        only_flag = '--only-{0}'.format(group)
        enable_flag = '--enable-{0}'.format(group)
        disable_flag = '--disable-{0}'.format(group)

        if item.config.getoption(only_flag):
            only_set = True
        elif group in item_marks:
            is_enabled = (item.config.getoption(flag) or
                          item.config.getoption(enable_flag))
            is_disabled = item.config.getoption(disable_flag)
            if is_disabled or not is_enabled:
                pytest.skip('{0} NOT enabled'.format(flag))

    if only_set:
        skip_item = True
        for group in groups:
            only_flag = '--only-{0}'.format(group)
            if group in item_marks and item.config.getoption(only_flag):
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
