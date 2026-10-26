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

# Stub for the pyarrow.lib C extension module. Re-exports the modules that
# have been annotated so far; everything else falls back to Any via
# __getattr__ until it gets annotated in subsequent PRs.

from typing import Any

from pyarrow._types import *  # noqa: F401,F403
from pyarrow.array import *  # noqa: F401,F403
from pyarrow.builder import *  # noqa: F401,F403
from pyarrow.config import *  # noqa: F401,F403
from pyarrow.device import *  # noqa: F401,F403
from pyarrow.error import *  # noqa: F401,F403
from pyarrow.io import *  # noqa: F401,F403
from pyarrow.memory import *  # noqa: F401,F403
from pyarrow.scalar import *  # noqa: F401,F403
from pyarrow.table import *  # noqa: F401,F403
from pyarrow.tensor import *  # noqa: F401,F403

def __getattr__(name: str) -> Any: ...
