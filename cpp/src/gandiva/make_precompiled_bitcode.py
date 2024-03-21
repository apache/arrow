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

import sys


def expand(data):
    """
    Expand *data* as a initializer list of hexadecimal char escapes.
    """
    expanded_data = ", ".join([hex(c) for c in bytearray(data)])
    return expanded_data.encode('ascii')


def apply_template(template, marker, data):
    if template.count(marker) != 1:
        raise ValueError("Invalid template")
    return template.replace(marker, expand(data))

def read_file(filepath):
    with open(filepath, "rb") as file:
        return file.read()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise ValueError("Usage: {0} <template file> <mandatory data file> <data file> "
                         "<output file>".format(sys.argv[0]))

    template = read_file(sys.argv[1])
    mandatory_data = read_file(sys.argv[2])
    data = read_file(sys.argv[3])

    expanded_data = apply_template(template, b"<MANDATORY_DATA_CHARS>", mandatory_data)
    expanded_data = apply_template(expanded_data, b"<DATA_CHARS>", data)

    with open(sys.argv[4], "wb") as f:
        f.write(expanded_data)
