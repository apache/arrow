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

from pyarrow.includes.libarrow_acero cimport *

cdef public object pyarrow_wrap_exec_node_options(const shared_ptr[CExecNodeOptions]& options)
cdef public shared_ptr[CExecNodeOptions] pyarrow_unwrap_exec_node_options(object options)

cdef public object pyarrow_wrap_declaration(const CDeclaration& declaration)
cdef public CDeclaration pyarrow_unwrap_declaration(object declaration)
