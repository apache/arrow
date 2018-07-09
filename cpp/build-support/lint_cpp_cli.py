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

import argparse
import re
import os

parser = argparse.ArgumentParser(
    description="Check for illegal headers for C++/CLI applications")
parser.add_argument("source_path",
                    help="Path to source code")
arguments = parser.parse_args()


_STRIP_COMMENT_REGEX = re.compile('(.+)?(?=//)')


def _strip_comments(line):
    m = _STRIP_COMMENT_REGEX.match(line)
    if not m:
        return line
    else:
        return m.group(0)


def lint_file(path):
    fail_rules = [
        (lambda x: '<mutex>' in x, 'Uses <mutex>'),
        (lambda x: 'nullptr' in x, 'Uses nullptr')
    ]

    with open(path) as f:
        for i, line in enumerate(f):
            stripped_line = _strip_comments(line)
            for rule, why in fail_rules:
                if rule(stripped_line):
                    raise Exception('File {0} failed C++/CLI lint check: {1}\n'
                                    'Line {2}: {3}'
                                    .format(path, why, i + 1, line))


EXCLUSIONS = [
    'arrow/util/macros.h',
    'arrow/util/parallel.h',
    'arrow/io/hdfs-internal.h'
]


for dirpath, _, filenames in os.walk(arguments.source_path):
    for filename in filenames:
        full_path = os.path.join(dirpath, filename)

        exclude = False
        for exclusion in EXCLUSIONS:
            if exclusion in full_path:
                exclude = True
                break

        if exclude:
            continue

        # Only run on header files
        if filename.endswith('.h'):
            lint_file(full_path)
