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

groups = [
    'cuda',
    'dataset',
    'orc',
    'parquet',
    'parquet/encryption',
    'plasma',
    'flight',
    'fs',
]

defaults = {
    'cuda': False,
    'dataset': False,
    'flight': False,
    'orc': False,
    'parquet': False,
    'parquet/encryption': False,
    'plasma': False,
    'fs': False,
}

try:
    import pyarrow.cuda  # noqa
    defaults['cuda'] = True
except ImportError:
    pass

try:
    import pyarrow.dataset  # noqa
    defaults['dataset'] = True
except ImportError:
    pass

try:
    import pyarrow.orc  # noqa
    defaults['orc'] = True
except ImportError:
    pass

try:
    import pyarrow.parquet  # noqa
    defaults['parquet'] = True
except ImportError:
    pass

try:
    import pyarrow.parquet.encryption  # noqa
    defaults['parquet/encryption'] = True
except ImportError:
    pass

try:
    import pyarrow.plasma  # noqa
    defaults['plasma'] = True
except ImportError:
    pass

try:
    import pyarrow.flight  # noqa
    defaults['flight'] = True
except ImportError:
    pass

try:
    from pyarrow.fs import S3FileSystem  # noqa
    defaults['fs'] = True
except ImportError:
    pass


def pytest_ignore_collect(path, config):
    if config.option.doctestmodules:
        # don't try to run doctests on the /tests directory
        if "/pyarrow/tests/" in str(path):
            return True

        # handle cuda, flight, etc
        for group in groups:
            if 'pyarrow/{}'.format(group) in str(path) and not defaults[group]:
                return True

    return False
