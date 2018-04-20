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
import difflib
import fnmatch
import os
import subprocess
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Runs clang format on all of the source "
        "files. If --fix is specified,  and compares the output "
        "with the existing file, outputting a unifiied diff if "
        "there are any necessary changes")
    parser.add_argument("clang_format_binary",
                        help="Path to the clang-format binary")
    parser.add_argument("exclude_globs",
                        help="Filename containing globs for files "
                        "that should be excluded from the checks")
    parser.add_argument("source_dir", 
                        help="Root directory of the source code")
    parser.add_argument("--fix", default=False,
                        action="store_true",
                        help="If specified, will re-format the source "
                        "code instead of comparing the re-formatted "
                        "output, defaults to %(default)s")

    arguments = parser.parse_args()

    formatted_filenames = []
    exclude_globs = [line.strip() for line in open(arguments.exclude_globs)]
    for directory, subdirs, filenames in os.walk(arguments.source_dir):
        fullpaths = (os.path.join(directory, filename) for filename in filenames)
        source_files = filter(lambda x: x.endswith(".h") or x.endswith(".cc"), fullpaths)
        formatted_filenames.extend(
            # Filter out files that match the globs in the globs file
            [filename for filename in source_files
             if not any((fnmatch.fnmatch(filename, exclude_glob)
                         for exclude_glob in exclude_globs))])
        
    error = False
    if arguments.fix:
        # Print out each file on its own line, but run
        # clang format once for all of the files
        for filename in formatted_filenames:
            print("Formatting {}".format(filename))
        subprocess.check_call([arguments.clang_format_binary,
                               "-i"] + formatted_filenames)
    else:
        for filename in formatted_filenames:
            print("Checking {}".format(filename))
            with open(filename) as reader:
                # Run clang-format and capture its output
                formatted = subprocess.check_output(
                    [arguments.clang_format_binary,
                     filename]).decode("utf-8")
                # Read the original file
                original = reader.read()
                # Run the equivalent of diff -u
                diff = list(difflib.unified_diff(
                    original.splitlines(True),
                    formatted.splitlines(True),
                    fromfile=filename,
                    tofile="{} (after clang format)".format(
                        filename)))
                if diff:
                    # Print out the diff to stderr
                    error = True
                    sys.stderr.writelines(diff)

    sys.exit(1 if error else 0)
