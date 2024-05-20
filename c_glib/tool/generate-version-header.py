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
    parser.add_argument(
            "--version-library",
            default="GARROW",
            help="The library name prefix to use in MIN_REQUIRED and "
            "MAX_ALLOWED checks")

    args = parser.parse_args()

    with open(args.input, "r", encoding="utf-8") as input_file, \
            open(args.output, "w", encoding="utf-8") as output_file:
        write_header(
                input_file, output_file,
                args.library, args.version, args.version_library)


def write_header(
        input_file: TextIOBase,
        output_file: TextIOBase,
        library_name: str,
        version: str,
        version_library: str):
    if "-" in version:
        version, version_tag = version.split("-")
    else:
        version_tag = ""
    version_major, version_minor, version_micro = [int(v) for v in version.split(".")]

    availability_macros = generate_availability_macros(library_name, version_library)

    replacements = {
            "VERSION_MAJOR": str(version_major),
            "VERSION_MINOR": str(version_minor),
            "VERSION_MICRO": str(version_micro),
            "VERSION_TAG": version_tag,
            "AVAILABILITY_MACROS": availability_macros,
    }

    output_file.write(re.sub(
        r"@([A-Z_]+)@", lambda match: replacements[match[1]], input_file.read()))


def generate_availability_macros(library: str, version_library: str) -> str:
    versions = [
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
    macros = []

    macros.append(f"""#ifdef {version_library}_DISABLE_DEPRECATION_WARNINGS
#  define {library}_DEPRECATED
#  define {library}_DEPRECATED_FOR(function)
#  define {library}_UNAVAILABLE(major, minor)
#else
#  define {library}_DEPRECATED G_DEPRECATED
#  define {library}_DEPRECATED_FOR(function) G_DEPRECATED_FOR(function)
#  define {library}_UNAVAILABLE(major, minor) G_UNAVAILABLE(major, minor)
#endif""")

    macros.append(f"""#define {library}_AVAILABLE_IN_ALL""")

    for major_version, minor_version in versions:
        macros.append(f"""#if {version_library}_VERSION_MIN_REQUIRED >= {version_library}_VERSION_{major_version}_{minor_version}
#  define {library}_DEPRECATED_IN_{major_version}_{minor_version}               {library}_DEPRECATED
#  define {library}_DEPRECATED_IN_{major_version}_{minor_version}_FOR(function) {library}_DEPRECATED_FOR(function)
#else
#  define {library}_DEPRECATED_IN_{major_version}_{minor_version}
#  define {library}_DEPRECATED_IN_{major_version}_{minor_version}_FOR(function)
#endif

#if {version_library}_VERSION_MAX_ALLOWED < {version_library}_VERSION_{major_version}_{minor_version}
#  define {library}_AVAILABLE_IN_{major_version}_{minor_version} {library}_UNAVAILABLE({major_version}, {minor_version})
#else
#  define {library}_AVAILABLE_IN_{major_version}_{minor_version}
#endif""")  # noqa: E501

    return "\n\n".join(macros)


if __name__ == '__main__':
    main()
