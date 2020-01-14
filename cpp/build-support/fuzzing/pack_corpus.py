#!/usr/bin/env python3
#
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

# Rename a bunch of corpus files to their SHA1 hashes, and
# pack them into a ZIP archive.

import hashlib
from pathlib import Path
import sys
import zipfile


def process_dir(corpus_dir, zip_output):
    seen = set()

    for child in corpus_dir.iterdir():
        if not child.is_file():
            raise IOError("Not a file: {0}".format(child))
        with child.open('rb') as f:
            data = f.read()
        arcname = hashlib.sha1(data).hexdigest()
        if arcname in seen:
            raise ValueError("Duplicate hash: {0} (in file {1})"
                             .format(arcname, child))
        zip_output.writestr(str(arcname), data)
        seen.add(arcname)


def main(corpus_dir, zip_output_name):
    with zipfile.ZipFile(zip_output_name, 'w') as zip_output:
        process_dir(Path(corpus_dir), zip_output)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: {0} <corpus dir> <output zip file>".format(sys.argv[0]))
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
