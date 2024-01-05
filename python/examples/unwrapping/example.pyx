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

# distutils: language=c++

import pyarrow as pa
import pyarrow.acero as pac
import pyarrow.compute as pc

from pyarrow cimport *
from pyarrow.lib cimport *
from pyarrow.lib_compute cimport *
from pyarrow.lib_acero cimport *

def unwrap_wrap_arr(array):
    cdef shared_ptr[CArray] arr = pyarrow_unwrap_array(array)
    out = pyarrow_wrap_array(arr)
    return out

def unwrap_wrap_declaration(declaration):
    cdef CDeclaration decl = pyarrow_unwrap_declaration(declaration)
    output = pyarrow_wrap_declaration(decl)
    return output

def unwrap_wrap_options(options):
    cdef shared_ptr[CExecNodeOptions] decl = pyarrow_unwrap_exec_node_options(options)
    output = pyarrow_wrap_exec_node_options(decl)
    return output

def unwrap_wrap_expression(expression):
    cdef CExpression expr = pyarrow_unwrap_expression(expression)
    output = pyarrow_wrap_expression(expr)
    return output


def run_test():
    print("Starting demo")
    arr = pa.array(["a", "b", "a"])

    print("Wrapping and unwrapping array")
    arr = unwrap_wrap_arr(arr)
    print(arr)

    print("Constructing a table")
    table = pa.Table.from_arrays([arr], names=["foo"])
    print(table)

    expression = (pc.field("foo") == pc.scalar("a"))
    print("Wrapping and unwrapping expression")
    expression = unwrap_wrap_expression(expression)
    print(expression)

    print("Filtering the table")
    table = table.filter(expression)

    print("Running a no-op acero node")
    options = pac.TableSourceNodeOptions(table)
    options = unwrap_wrap_options(options)

    source_node = pac.Declaration("table_source", options, [])
    source_node = unwrap_wrap_declaration(source_node)
    print(source_node.to_table())


