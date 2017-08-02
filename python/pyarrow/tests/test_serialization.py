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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os

import pyarrow as pa
import numpy as np

obj = pa.lib.serialize_sequence([np.array([1, 2, 3]), None, np.array([4, 5, 6])])

SIZE = 4096
arr = np.random.randint(0, 256, size=SIZE).astype('u1')
data = arr.tobytes()[:SIZE]
path = os.path.join("/tmp/temp")
with open(path, 'wb') as f:
    f.write(data)

f = pa.memory_map(path, mode="w")

pa.lib.write_python_object(obj, f)

f = pa.memory_map(path, mode="r")

res = pa.lib.read_python_object(f)

pa.lib.deserialize_sequence(res, res)
