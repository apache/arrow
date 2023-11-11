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

from pyarrow.includes.libarrow cimport *
from pyarrow.includes.libarrow_acero cimport *
from pyarrow._acero cimport ExecNodeOptions, Declaration

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
