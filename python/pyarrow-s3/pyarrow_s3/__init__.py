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

import ctypes
import glob
import os
import sys

# libarrow_s3 depends on libarrow, which ships in the pyarrow wheel.
# Importing pyarrow first loads libarrow into the process, so the dynamic
# loader resolves libarrow_s3's dependency on it by SONAME.
import pyarrow  # noqa: F401

if sys.platform == "win32":
    _pattern = "arrow_s3.dll"
elif sys.platform == "darwin":
    _pattern = "libarrow_s3.*.dylib"
else:
    _pattern = "libarrow_s3.so.*"

# Keep a reference so the library stays loaded for the process lifetime.
_libarrow_s3 = ctypes.CDLL(
    glob.glob(os.path.join(os.path.dirname(__file__), _pattern))[0],
    mode=ctypes.RTLD_GLOBAL,
)
