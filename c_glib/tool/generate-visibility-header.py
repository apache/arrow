#!/usr/bin/env python3

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
from io import TextIOBase
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
            description="Generate C header with visibility macros")
    parser.add_argument(
            "--library",
            required=True,
            help="The library name to use in macro prefixes")
    parser.add_argument(
            "--output",
            type=Path,
            required=True,
            help="Path to the output file to generate")

    args = parser.parse_args()

    with open(args.output, "w", encoding="utf-8") as output_file:
        write_header(output_file, args.library)


def write_header(output_file: TextIOBase, library: str):
    output_file.write(f"""#pragma once

#if (defined(_WIN32) || defined(__CYGWIN__)) && defined(_MSVC_LANG) && \
  !defined({library}_STATIC_COMPILATION)
#  define {library}_EXPORT __declspec(dllexport)
#  define {library}_IMPORT __declspec(dllimport)
#else
#  define {library}_EXPORT
#  define {library}_IMPORT
#endif

#ifdef {library}_COMPILATION
#  define {library}_API {library}_EXPORT
#else
#  define {library}_API {library}_IMPORT
#endif

#define {library}_EXTERN {library}_API extern
""")


if __name__ == '__main__':
    main()
