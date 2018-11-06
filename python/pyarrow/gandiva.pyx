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

# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True

from libcpp cimport bool as c_bool, nullptr
from libcpp.memory cimport shared_ptr, unique_ptr, make_shared
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector
from libc.stdint cimport int64_t, uint8_t, uintptr_t

from pyarrow.includes.libarrow cimport *
from pyarrow.compat import frombytes
from pyarrow.lib cimport (check_status, pyarrow_wrap_array)

from pyarrow.includes.libgandiva cimport (CCondition, CExpression,
                                          CNode, CProjector, CFilter,
                                          CSelectionVector,
                                          TreeExprBuilder_MakeExpression,
                                          TreeExprBuilder_MakeFunction,
                                          TreeExprBuilder_MakeLiteral,
                                          TreeExprBuilder_MakeField,
                                          TreeExprBuilder_MakeIf,
                                          TreeExprBuilder_MakeCondition,
                                          SelectionVector_MakeInt32,
                                          Projector_Make,
                                          Filter_Make)

from pyarrow.lib cimport (Array, DataType, Field, MemoryPool,
                          RecordBatch, Schema)

cdef class Node:
    cdef:
        shared_ptr[CNode] node

    cdef void init(self, shared_ptr[CNode] node):
        self.node = node

cdef make_node(shared_ptr[CNode] node):
    cdef Node result = Node()
    result.init(node)
    return result

cdef class Expression:
    cdef:
        shared_ptr[CExpression] expression

    cdef void init(self, shared_ptr[CExpression] expression):
        self.expression = expression

cdef class Condition:
    cdef:
        shared_ptr[CCondition] condition

    cdef void init(self, shared_ptr[CCondition] condition):
        self.condition = condition

cdef make_condition(shared_ptr[CCondition] condition):
    cdef Condition result = Condition()
    result.init(condition)
    return result

cdef class SelectionVector:
    cdef:
        shared_ptr[CSelectionVector] selection_vector

    cdef void init(self, shared_ptr[CSelectionVector] selection_vector):
        self.selection_vector = selection_vector

    def to_array(self):
        cdef shared_ptr[CArray] result = self.selection_vector.get().ToArray()
        return pyarrow_wrap_array(result)

cdef make_selection_vector(shared_ptr[CSelectionVector] selection_vector):
    cdef SelectionVector result = SelectionVector()
    result.init(selection_vector)
    return result

cdef class Projector:
    cdef:
        shared_ptr[CProjector] projector
        MemoryPool pool

    cdef void init(self, shared_ptr[CProjector] projector, MemoryPool pool):
        self.projector = projector
        self.pool = pool

    def evaluate(self, RecordBatch batch):
        cdef vector[shared_ptr[CArray]] results
        check_status(self.projector.get().Evaluate(
            batch.sp_batch.get()[0], self.pool.pool, &results))
        cdef shared_ptr[CArray] result
        arrays = []
        for result in results:
            arrays.append(pyarrow_wrap_array(result))
        return arrays

cdef class Filter:
    cdef:
        shared_ptr[CFilter] filter

    cdef void init(self, shared_ptr[CFilter] filter):
        self.filter = filter

    def evaluate(self, RecordBatch batch, MemoryPool pool):
        cdef shared_ptr[CSelectionVector] selection
        check_status(SelectionVector_MakeInt32(
            batch.num_rows, pool.pool, &selection))
        check_status(self.filter.get().Evaluate(
            batch.sp_batch.get()[0], selection))
        return make_selection_vector(selection)

cdef class TreeExprBuilder:

    def make_literal(self, value):
        cdef shared_ptr[CNode] r = TreeExprBuilder_MakeLiteral(value)
        return make_node(r)

    def make_expression(self, Node root_node, Field return_field):
        cdef shared_ptr[CExpression] r = TreeExprBuilder_MakeExpression(
            root_node.node, return_field.sp_field)
        cdef Expression expression = Expression()
        expression.init(r)
        return expression

    def make_function(self, name, children, DataType return_type):
        cdef c_vector[shared_ptr[CNode]] c_children
        cdef Node child
        for child in children:
            c_children.push_back(child.node)
        cdef shared_ptr[CNode] r = TreeExprBuilder_MakeFunction(
            name.encode(), c_children, return_type.sp_type)
        return make_node(r)

    def make_field(self, Field field):
        cdef shared_ptr[CNode] r = TreeExprBuilder_MakeField(field.sp_field)
        return make_node(r)

    def make_if(self, Node condition, Node this_node,
                Node else_node, DataType return_type):
        cdef shared_ptr[CNode] r = TreeExprBuilder_MakeIf(
            condition.node, this_node.node, else_node.node,
            return_type.sp_type)
        return make_node(r)

    def make_condition(self, Node condition):
        cdef shared_ptr[CCondition] r = TreeExprBuilder_MakeCondition(
            condition.node)
        return make_condition(r)

cpdef make_projector(Schema schema, children, MemoryPool pool):
    cdef c_vector[shared_ptr[CExpression]] c_children
    cdef Expression child
    for child in children:
        c_children.push_back(child.expression)
    cdef shared_ptr[CProjector] result
    check_status(Projector_Make(schema.sp_schema, c_children,
                                &result))
    cdef Projector projector = Projector()
    projector.init(result, pool)
    return projector

cpdef make_filter(Schema schema, Condition condition):
    cdef shared_ptr[CFilter] result
    check_status(Filter_Make(schema.sp_schema, condition.condition, &result))
    cdef Filter filter = Filter()
    filter.init(result)
    return filter
