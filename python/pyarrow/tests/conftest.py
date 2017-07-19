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

from pytest import skip


groups = ['hdfs', 'parquet', 'plasma', 'large_memory']

defaults = {
    'hdfs': False,
    'parquet': False,
    'plasma': False,
    'large_memory': False
}

try:
    import pyarrow.parquet  # noqa
    defaults['parquet'] = True
except ImportError:
    pass

try:
    import pyarrow.plasma as plasma
    defaults['plasma'] = True
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
        parser.addoption('--only-{0}'.format(group), action='store_true',
                         default=False,
                         help=('Run only the {0} test group'.format(group)))


def pytest_runtest_setup(item):
    only_set = False

    for group in groups:
        only_flag = '--only-{0}'.format(group)
        flag = '--{0}'.format(group)

        if item.config.getoption(only_flag):
            only_set = True
        elif getattr(item.obj, group, None):
            if not item.config.getoption(flag):
                skip('{0} NOT enabled'.format(flag))

    if only_set:
        skip_item = True
        for group in groups:
            only_flag = '--only-{0}'.format(group)
            if (getattr(item.obj, group, False) and
                    item.config.getoption(only_flag)):
                skip_item = False

        if skip_item:
            skip('Only running some groups with only flags')
