#!/usr/bin/env python
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

import functools
import operator
import sys
from setuptools import setup

if sys.version_info < (3, 5):
    sys.exit('Python < 3.5 is not supported')

extras = {
    'benchmark': ['pandas'],
    'bot': ['ruamel.yaml', 'pygithub'],
    'docker': ['ruamel.yaml', 'python-dotenv'],
    'release': ['jinja2', 'jira', 'semver', 'gitpython']
}
extras['all'] = list(set(functools.reduce(operator.add, extras.values())))

setup(
    name='archery',
    version="0.1.0",
    description='Apache Arrow Developers Tools',
    url='http://github.com/apache/arrow',
    maintainer='Arrow Developers',
    maintainer_email='dev@arrow.apache.org',
    packages=[
        'archery',
        'archery.benchmark',
        'archery.integration',
        'archery.lang',
        'archery.utils'
    ],
    include_package_data=True,
    install_requires=['click>=7'],
    tests_require=['pytest', 'responses'],
    extras_require=extras,
    entry_points='''
        [console_scripts]
        archery=archery.cli:archery
    '''
)
