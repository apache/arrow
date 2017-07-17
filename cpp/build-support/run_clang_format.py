#!/usr/bin/python
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

import fnmatch
import os
import subprocess
import sys

if len(sys.argv) < 4:
    sys.stderr.write("Usage: %s $CLANG_FORMAT_VERSION exclude_globs.txt "
                     "$source_dir\n" %
                     sys.argv[0])
    sys.exit(1)

CLANG_FORMAT = 'clang-format-{0}'.format(sys.argv[1])
EXCLUDE_GLOBS_FILENAME = sys.argv[2]
SOURCE_DIR = sys.argv[3]

exclude_globs = [line.strip() for line in open(EXCLUDE_GLOBS_FILENAME, "r")]

files_to_format = []
matches = []
for directory, subdirs, files in os.walk(SOURCE_DIR):
    for name in files:
        name = os.path.join(directory, name)
        if not (name.endswith('.h') or name.endswith('.cc')):
            continue

        excluded = False
        for g in exclude_globs:
            if fnmatch.fnmatch(name, g):
                excluded = True
                break
        if not excluded:
            files_to_format.append(name)

# TODO(wesm): Port this to work with Python, for check-format
# NUM_CORRECTIONS=`$CLANG_FORMAT -output-replacements-xml  $@ |
# grep offset | wc -l`
# if [ "$NUM_CORRECTIONS" -gt "0" ]; then
#   echo "clang-format suggested changes, please run 'make format'!!!!"
#   exit 1
# fi

subprocess.check_output([CLANG_FORMAT, '-i'] + files_to_format,
                        stderr=subprocess.STDOUT)
