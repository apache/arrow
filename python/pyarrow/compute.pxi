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

# separating out this base class is easier than unifying it into
# FunctionRegistry, which lives outside libarrow
cdef class BaseFunctionRegistry(_Weakrefable):
    cdef CFunctionRegistry* registry

cdef class ExtensionIdRegistry(_Weakrefable):
    def __cinit__(self):
        self.registry = NULL

    def __init__(self):
        raise TypeError("Do not call ExtensionIdRegistry's constructor directly, use "
                        "the `MakeExtensionIdRegistry` function instead.")

    cdef void init(self, shared_ptr[CExtensionIdRegistry]& registry):
        self.sp_registry = registry
        self.registry = registry.get()
