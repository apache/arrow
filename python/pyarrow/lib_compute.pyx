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

from pyarrow.lib cimport *
from pyarrow._compute cimport Expression

import_pyarrow_compute()

cdef api bint pyarrow_is_expression(object expression):
    return isinstance(expression, Expression)

cdef api CExpression pyarrow_unwrap_expression(object expression):
    cdef Expression e
    if pyarrow_is_expression(expression):
        e = <Expression>(expression)
        return e.expr

    return CExpression()

cdef api object pyarrow_wrap_expression(
        const CExpression& cexpr):
    cdef Expression expr = Expression.__new__(Expression)
    expr.init(cexpr)
    return expr
