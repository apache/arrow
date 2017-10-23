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

import os
import os.path as osp
import shutil
import sys


def is_coverage_file(path):
    return path.endswith('gcno') or path.endswith('gcda')


def copy_files(path, outpath='.'):
    for root, dirs, files in os.walk(path):
        for fname in files:
            if not is_coverage_file(fname):
                continue
            relpath = osp.join(root, fname)
            dstpath = '_'.join((root.replace(path, '').replace('/', '_'),
                                fname))

            shutil.copy(relpath, osp.join(outpath, dstpath))


if __name__ == '__main__':
    path = sys.argv[1]
    outpath = sys.argv[2]
    copy_files(path, outpath)
