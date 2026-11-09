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
import types

import pyarrow.lib as lib


def _stub_only_type(name: str) -> type[lib.DataType]:
    # The factories of the stub-only DataType subclasses (Int32Type, ...)
    # return plain DataType instances at runtime.
    if name.endswith("Type") and not name.startswith("_"):
        return lib.DataType
    raise AttributeError(name)


# pyarrow._stubs_typing only exists in the type stubs. Provide it at runtime,
# so that the tests can import the stub-only types without a TYPE_CHECKING
# block.
_stubs_typing = types.ModuleType("pyarrow._stubs_typing")
setattr(_stubs_typing, "__getattr__", _stub_only_type)
sys.modules["pyarrow._stubs_typing"] = _stubs_typing
