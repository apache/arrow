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

marker = b"<DATA_CHARS>"

def expand(data):
    """
    Expand *data* as a initializer list of hexadecimal char escapes.
    """
    expanded_data = ", ".join([hex(c) for c in bytearray(data)])
    return expanded_data.encode('ascii')


def apply_template(template, data):
    if template.count(marker) != 1:
        raise ValueError("Invalid template")
    return template.replace(marker, expand(data))


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise ValueError("Usage: {0} <template file> <data file> "
                         "<output file>".format(sys.argv[0]))
    with open(sys.argv[1], "rb") as f:
        template = f.read()
    with open(sys.argv[2], "rb") as f:
        data = f.read()

    expanded_data = apply_template(template, data)
    with open(sys.argv[3], "wb") as f:
        f.write(expanded_data)
