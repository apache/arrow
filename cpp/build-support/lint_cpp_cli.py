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
import sys
import traceback

parser = argparse.ArgumentParser(
    description="Check for illegal headers for C++/CLI applications")
parser.add_argument("source_path",
                    help="Path to source code")
arguments = parser.parse_args()


_STRIP_COMMENT_REGEX = re.compile('(.+)?(?=//)')
_NULLPTR_REGEX = re.compile(r'.*\bnullptr\b.*')
_RETURN_NOT_OK_REGEX = re.compile(r'.*\sRETURN_NOT_OK.*')


def _strip_comments(line):
    m = _STRIP_COMMENT_REGEX.match(line)
    if not m:
        return line
    else:
        return m.group(0)


def lint_file(path):
    fail_rules = [
        # rule, error message, rule-specific exclusions list
        (lambda x: '<mutex>' in x, 'Uses <mutex>', []),
        (lambda x: re.match(_NULLPTR_REGEX, x), 'Uses nullptr', []),
        (lambda x: re.match(_RETURN_NOT_OK_REGEX, x),
         'Use ARROW_RETURN_NOT_OK in header files',
         ['arrow/status.h',
          'test',
          'arrow/util/hash.h',
          'arrow/python/util'])
    ]

    with open(path) as f:
        for i, line in enumerate(f):
            stripped_line = _strip_comments(line)
            for rule, why, rule_exclusions in fail_rules:
                if any([True for excl in rule_exclusions if excl in path]):
                    continue

                if rule(stripped_line):
                    raise Exception('File {0} failed C++/CLI lint check: {1}\n'
                                    'Line {2}: {3}'
                                    .format(path, why, i + 1, line))


EXCLUSIONS = [
    'arrow/python/iterators.h',
    'arrow/util/date.h',
    'arrow/util/hashing.h',
    'arrow/util/macros.h',
    'arrow/util/parallel.h',
    'arrow/util/string_view/string_view.hpp',
    'arrow/util/xxhash/xxhash.c',
    'arrow/util/xxhash/xxhash.h',
    'arrow/visitor_inline.h',
    'gandiva/cache.h',
    'gandiva/jni',
    'test',
    'internal'
]

try:
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
except Exception:
    traceback.print_exc()
    sys.exit(1)
