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
import re


def main():
    parser = argparse.ArgumentParser(
        description="Generate C header with version macros")
    parser.add_argument(
        "--library",
        required=True,
        help="The library name to use in macro prefixes")
    parser.add_argument(
        "--version",
        required=True,
        help="The library version number")
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to the input template file")
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to the output file to generate")

    args = parser.parse_args()

    with open(args.input, "r", encoding="utf-8") as input_file, \
            open(args.output, "w", encoding="utf-8") as output_file:
        write_header(
            input_file, output_file, args.library, args.version)


def write_header(
        input_file: TextIOBase,
        output_file: TextIOBase,
        library_name: str,
        version: str):
    if "-" in version:
        version, version_tag = version.split("-")
    else:
        version_tag = ""
    version_major, version_minor, version_micro = [int(v) for v in version.split(".")]

    encoded_versions = generate_encoded_versions(library_name)
    visibility_macros = generate_visibility_macros(library_name)
    availability_macros = generate_availability_macros(library_name)

    replacements = {
        "VERSION_MAJOR": str(version_major),
        "VERSION_MINOR": str(version_minor),
        "VERSION_MICRO": str(version_micro),
        "VERSION_TAG": version_tag,
        "ENCODED_VERSIONS": encoded_versions,
        "VISIBILITY_MACROS": visibility_macros,
        "AVAILABILITY_MACROS": availability_macros,
    }

    output_file.write(re.sub(
        r"@([A-Z_]+)@", lambda match: replacements[match[1]], input_file.read()))


def generate_visibility_macros(library: str) -> str:
    return f"""#if (defined(_WIN32) || defined(__CYGWIN__)) && defined(_MSC_VER) && \
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

#define {library}_EXTERN {library}_API extern"""


def generate_encoded_versions(library: str) -> str:
    macros = []

    for major_version, minor_version in ALL_VERSIONS:
        macros.append(f"""/**
 * {library}_VERSION_{major_version}_{minor_version}:
 *
 * You can use this macro value for compile time API version check.
 *
 * Since: {major_version}.{minor_version}.0
 */
#define {library}_VERSION_{major_version}_{minor_version} G_ENCODE_VERSION({major_version}, {minor_version})""")  # noqa: E501

    return "\n\n".join(macros)


def generate_availability_macros(library: str) -> str:
    macros = [f"""#define {library}_AVAILABLE_IN_ALL {library}_EXTERN"""]

    for major_version, minor_version in ALL_VERSIONS:
        macros.append(f"""#if {library}_VERSION_MIN_REQUIRED >= {library}_VERSION_{major_version}_{minor_version}
#  define {library}_DEPRECATED_IN_{major_version}_{minor_version}               {library}_DEPRECATED
#  define {library}_DEPRECATED_IN_{major_version}_{minor_version}_FOR(function) {library}_DEPRECATED_FOR(function)
#else
#  define {library}_DEPRECATED_IN_{major_version}_{minor_version}
#  define {library}_DEPRECATED_IN_{major_version}_{minor_version}_FOR(function)
#endif

#if {library}_VERSION_MAX_ALLOWED < {library}_VERSION_{major_version}_{minor_version}
#  define {library}_AVAILABLE_IN_{major_version}_{minor_version} {library}_EXTERN {library}_UNAVAILABLE({major_version}, {minor_version})
#else
#  define {library}_AVAILABLE_IN_{major_version}_{minor_version} {library}_EXTERN
#endif""")  # noqa: E501

    return "\n\n".join(macros)


ALL_VERSIONS = [
    (22, 0),
    (21, 0),
    (20, 0),
    (19, 0),
    (18, 0),
    (17, 0),
    (16, 0),
    (15, 0),
    (14, 0),
    (13, 0),
    (12, 0),
    (11, 0),
    (10, 0),
    (9, 0),
    (8, 0),
    (7, 0),
    (6, 0),
    (5, 0),
    (4, 0),
    (3, 0),
    (2, 0),
    (1, 0),
    (0, 17),
    (0, 16),
    (0, 15),
    (0, 14),
    (0, 13),
    (0, 12),
    (0, 11),
    (0, 10),
]


if __name__ == '__main__':
    main()
