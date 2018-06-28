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

# Pretty-printing and other formatting utilities for Arrow data structures

import pyarrow.lib as lib
import warnings

try:
    from textwrap import indent
except ImportError:
    def indent(text, prefix):
        return ''.join(prefix + line for line in text.splitlines(True))


def array_format(arr, window=10):
    warnings.warn("array_format is deprecated, use Array.format() instead",
                  FutureWarning)
    return arr.format(window=window)


def value_format(x, indent_level=0):
    warnings.warn("value_format is deprecated",
                  FutureWarning)
    if isinstance(x, lib.ListValue):
        contents = ',\n'.join(value_format(item) for item in x)
        return '[{0}]'.format(indent(contents, ' ').strip())
    else:
        return repr(x)
