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
import subprocess
import tempfile

import pytest
import hypothesis as h

try:
    import pathlib
except ImportError:
    import pathlib2 as pathlib  # py2 compat

from pyarrow.util import find_free_port


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
    'dataset',
    'hypothesis',
    'fastparquet',
    'gandiva',
    'hdfs',
    'large_memory',
    'nopandas',
    'orc',
    'pandas',
    'parquet',
    'plasma',
    's3',
    'tensorflow',
    'flight',
    'slow',
    'requires_testing_data',
]

defaults = {
    'cython': False,
    'dataset': False,
    'fastparquet': False,
    'hypothesis': False,
    'gandiva': False,
    'hdfs': False,
    'large_memory': False,
    'orc': False,
    'nopandas': False,
    'pandas': False,
    'parquet': False,
    'plasma': False,
    's3': False,
    'tensorflow': False,
    'flight': False,
    'slow': False,
    'requires_testing_data': True,
}

try:
    import cython  # noqa
    defaults['cython'] = True
except ImportError:
    pass

try:
    import fastparquet  # noqa
    defaults['fastparquet'] = True
except ImportError:
    pass

try:
    import pyarrow.gandiva  # noqa
    defaults['gandiva'] = True
except ImportError:
    pass

try:
    import pyarrow.dataset  # noqa
    defaults['dataset'] = True
except ImportError:
    raise
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
    defaults['nopandas'] = True

try:
    import pyarrow.parquet  # noqa
    defaults['parquet'] = True
except ImportError:
    pass

try:
    import pyarrow.plasma  # noqa
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

try:
    from pyarrow.fs import S3FileSystem  # noqa
    defaults['s3'] = True
except ImportError:
    pass

try:
    from pyarrow.fs import HadoopFileSystem  # noqa
    defaults['hdfs'] = True
except ImportError:
    pass


def pytest_addoption(parser):
    # Create options to selectively enable test groups
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


class PyArrowConfig(object):
    def __init__(self):
        self.is_enabled = {}

    def apply_mark(self, mark):
        group = mark.name
        if group in groups:
            self.requires(group)

    def requires(self, group):
        if not self.is_enabled[group]:
            pytest.skip('{0} NOT enabled'.format(group))


def pytest_configure(config):
    # Apply command-line options to initialize PyArrow-specific config object
    config.pyarrow = PyArrowConfig()

    for mark in groups:
        config.addinivalue_line(
            "markers", mark,
        )

        flag = '--{0}'.format(mark)
        enable_flag = '--enable-{0}'.format(mark)

        is_enabled = (config.getoption(flag) or
                      config.getoption(enable_flag))
        config.pyarrow.is_enabled[mark] = is_enabled


def pytest_runtest_setup(item):
    # Apply test markers to skip tests selectively
    for mark in item.iter_markers():
        item.config.pyarrow.apply_mark(mark)


@pytest.fixture
def tempdir(tmpdir):
    # convert pytest's LocalPath to pathlib.Path
    return pathlib.Path(tmpdir.strpath)


@pytest.fixture(scope='session')
def datadir():
    return pathlib.Path(__file__).parent / 'data'


try:
    from tempfile import TemporaryDirectory
except ImportError:
    import shutil

    class TemporaryDirectory(object):
        """Temporary directory implementation for python 2"""

        def __enter__(self):
            self.tmp = tempfile.mkdtemp()
            return self.tmp

        def __exit__(self, exc_type, exc_value, traceback):
            shutil.rmtree(self.tmp)


# TODO(kszucs): move the following fixtures to test_fs.py once the previous
# parquet dataset implementation and hdfs implementation are removed.

@pytest.mark.hdfs
@pytest.fixture(scope='session')
def hdfs_server():
    host = os.environ.get('ARROW_HDFS_TEST_HOST', 'default')
    port = int(os.environ.get('ARROW_HDFS_TEST_PORT', 0))
    user = os.environ.get('ARROW_HDFS_TEST_USER', 'hdfs')
    return host, port, user


@pytest.mark.s3
@pytest.fixture(scope='session')
def minio_server():
    host, port = 'localhost', find_free_port()
    access_key, secret_key = 'arrow', 'apachearrow'

    address = '{}:{}'.format(host, port)
    env = os.environ.copy()
    env.update({
        'MINIO_ACCESS_KEY': access_key,
        'MINIO_SECRET_KEY': secret_key
    })

    with TemporaryDirectory() as tempdir:
        args = ['minio', '--compat', 'server', '--quiet', '--address',
                address, tempdir]
        proc = None
        try:
            proc = subprocess.Popen(args, env=env)
        except (OSError, IOError):
            pytest.skip('`minio` command cannot be located')
        else:
            yield address, access_key, secret_key
        finally:
            if proc is not None:
                proc.kill()
