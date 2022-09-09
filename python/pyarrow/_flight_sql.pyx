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

# cython: language_level = 3

from libc.stdint cimport uintptr_t

from pyarrow.includes.libarrow_flight_sql cimport *


def connect_raw(uri: str, **kwargs):
    """Create a low level ADBC connection via Flight SQL."""
    import adbc_driver_manager
    return adbc_driver_manager.AdbcDatabase(init_func=int(<uintptr_t> &AdbcDriverInit),
                                            uri=uri, **kwargs)
