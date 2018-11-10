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

# distutils: language = c++

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *

cdef extern from "gandiva/gandiva_aliases.h" namespace "gandiva" nogil:

    cdef cppclass CNode" gandiva::Node":
        pass

    cdef cppclass CExpression" gandiva::Expression":
        pass

    ctypedef vector[shared_ptr[CNode]] CNodeVector" gandiva::NodeVector"

    ctypedef vector[shared_ptr[CExpression]] \
        CExpressionVector" gandiva::ExpressionVector"

cdef extern from "gandiva/selection_vector.h" namespace "gandiva" nogil:

    cdef cppclass CSelectionVector" gandiva::SelectionVector":

        shared_ptr[CArray] ToArray()

    cdef CStatus SelectionVector_MakeInt16\
        "gandiva::SelectionVector::MakeInt16"(
            int64_t max_slots, CMemoryPool* pool,
            shared_ptr[CSelectionVector]* selection_vector)

    cdef CStatus SelectionVector_MakeInt32\
        "gandiva::SelectionVector::MakeInt32"(
            int64_t max_slots, CMemoryPool* pool,
            shared_ptr[CSelectionVector]* selection_vector)

    cdef CStatus SelectionVector_MakeInt64\
        "gandiva::SelectionVector::MakeInt64"(
            int64_t max_slots, CMemoryPool* pool,
            shared_ptr[CSelectionVector]* selection_vector)

cdef extern from "gandiva/condition.h" namespace "gandiva" nogil:

    cdef cppclass CCondition" gandiva::Condition":
        pass

cdef extern from "gandiva/arrow.h" namespace "gandiva" nogil:

    ctypedef vector[shared_ptr[CArray]] CArrayVector" gandiva::ArrayVector"


cdef extern from "gandiva/tree_expr_builder.h" namespace "gandiva" nogil:

    cdef shared_ptr[CNode] TreeExprBuilder_MakeBoolLiteral \
        "gandiva::TreeExprBuilder::MakeLiteral"(c_bool value)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeUInt8Literal \
        "gandiva::TreeExprBuilder::MakeLiteral"(uint8_t value)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeUInt16Literal \
        "gandiva::TreeExprBuilder::MakeLiteral"(uint16_t value)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeUInt32Literal \
        "gandiva::TreeExprBuilder::MakeLiteral"(uint32_t value)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeUInt64Literal \
        "gandiva::TreeExprBuilder::MakeLiteral"(uint64_t value)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeInt8Literal \
        "gandiva::TreeExprBuilder::MakeLiteral"(int8_t value)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeInt16Literal \
        "gandiva::TreeExprBuilder::MakeLiteral"(int16_t value)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeInt32Literal \
        "gandiva::TreeExprBuilder::MakeLiteral"(int32_t value)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeInt64Literal \
        "gandiva::TreeExprBuilder::MakeLiteral"(int64_t value)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeFloatLiteral \
        "gandiva::TreeExprBuilder::MakeLiteral"(float value)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeDoubleLiteral \
        "gandiva::TreeExprBuilder::MakeLiteral"(double value)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeStringLiteral \
        "gandiva::TreeExprBuilder::MakeStringLiteral"(const c_string& value)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeBinaryLiteral \
        "gandiva::TreeExprBuilder::MakeBinaryLiteral"(const c_string& value)

    cdef shared_ptr[CExpression] TreeExprBuilder_MakeExpression\
        "gandiva::TreeExprBuilder::MakeExpression"(
            shared_ptr[CNode] root_node, shared_ptr[CField] result_field)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeFunction \
        "gandiva::TreeExprBuilder::MakeFunction"(
            const c_string& name, const CNodeVector& children,
            shared_ptr[CDataType] return_type)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeField \
        "gandiva::TreeExprBuilder::MakeField"(shared_ptr[CField] field)

    cdef shared_ptr[CNode] TreeExprBuilder_MakeIf \
        "gandiva::TreeExprBuilder::MakeIf"(
            shared_ptr[CNode] condition, shared_ptr[CNode] this_node,
            shared_ptr[CNode] else_node, shared_ptr[CDataType] return_type)

    cdef shared_ptr[CCondition] TreeExprBuilder_MakeCondition \
        "gandiva::TreeExprBuilder::MakeCondition"(
            shared_ptr[CNode] condition)

    cdef CStatus Projector_Make \
        "gandiva::Projector::Make"(
            shared_ptr[CSchema] schema, const CExpressionVector& children,
            shared_ptr[CProjector]* projector)

cdef extern from "gandiva/projector.h" namespace "gandiva" nogil:

    cdef cppclass CProjector" gandiva::Projector":

        CStatus Evaluate(
            const CRecordBatch& batch, CMemoryPool* pool,
            const CArrayVector* output)

cdef extern from "gandiva/filter.h" namespace "gandiva" nogil:

    cdef cppclass CFilter" gandiva::Filter":

        CStatus Evaluate(
            const CRecordBatch& batch,
            shared_ptr[CSelectionVector] out_selection)

    cdef CStatus Filter_Make \
        "gandiva::Filter::Make"(
            shared_ptr[CSchema] schema, shared_ptr[CCondition] condition,
            shared_ptr[CFilter]* filter)
