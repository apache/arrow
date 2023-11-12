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

# cython: profile = False
# cython: nonecheck = True
# distutils: language = c++

from pyarrow._acero cimport Declaration, ExecNodeOptions

import_pyarrow_acero()

cdef api bint pyarrow_is_exec_node_options(object options):
    return isinstance(options, ExecNodeOptions)

cdef api shared_ptr[CExecNodeOptions] pyarrow_unwrap_exec_node_options(object options):
    cdef ExecNodeOptions e
    if pyarrow_is_declaration(options):
        d = <ExecNodeOptions>(options)
        return d.wrapped

    return shared_ptr[CExecNodeOptions]()

cdef api object pyarrow_wrap_exec_node_options(
        const shared_ptr[CExecNodeOptions]& options):
    cdef ExecNodeOptions e = ExecNodeOptions.__new__(ExecNodeOptions)
    e.init(options)
    return e


cdef api bint pyarrow_is_declaration(object declaration):
    return isinstance(declaration, Declaration)

cdef api CDeclaration pyarrow_unwrap_declaration(object declaration):
    cdef Declaration d
    if pyarrow_is_declaration(declaration):
        d = <Declaration>(declaration)
        return d.decl

    return CDeclaration()

cdef api object pyarrow_wrap_declaration(
        const CDeclaration& declaration):
    cdef Declaration decl = Declaration.__new__(Declaration)
    decl.init(declaration)
    return decl


