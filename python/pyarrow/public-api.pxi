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

from libcpp.memory cimport shared_ptr
from pyarrow.includes.libarrow cimport (CArray, CColumn, CDataType, CField,
                                        CRecordBatch, CSchema,
                                        CTable, CTensor)


cdef public api object pyarrow_wrap_buffer(const shared_ptr[CBuffer]& buf):
    cdef Buffer result = Buffer()
    result.init(buf)
    return result


cdef public api object pyarrow_wrap_data_type(
    const shared_ptr[CDataType]& type):
    cdef:
        DataType out

    if type.get() == NULL:
        return None

    if type.get().id() == _Type_DICTIONARY:
        out = DictionaryType()
    elif type.get().id() == _Type_TIMESTAMP:
        out = TimestampType()
    elif type.get().id() == _Type_FIXED_SIZE_BINARY:
        out = FixedSizeBinaryType()
    elif type.get().id() == _Type_DECIMAL:
        out = DecimalType()
    else:
        out = DataType()

    out.init(type)
    return out


cdef public api object pyarrow_wrap_field(const shared_ptr[CField]& field):
    if field.get() == NULL:
        return None
    cdef Field out = Field()
    out.init(field)
    return out


cdef public api object pyarrow_wrap_schema(const shared_ptr[CSchema]& type):
    cdef Schema out = Schema()
    out.init_schema(type)
    return out


cdef public api object pyarrow_wrap_array(const shared_ptr[CArray]& sp_array):
    if sp_array.get() == NULL:
        raise ValueError('Array was NULL')

    cdef CDataType* data_type = sp_array.get().type().get()

    if data_type == NULL:
        raise ValueError('Array data type was NULL')

    cdef Array arr = _array_classes[data_type.id()]()
    arr.init(sp_array)
    return arr


cdef public api object pyarrow_wrap_tensor(
    const shared_ptr[CTensor]& sp_tensor):
    if sp_tensor.get() == NULL:
        raise ValueError('Tensor was NULL')

    cdef Tensor tensor = Tensor()
    tensor.init(sp_tensor)
    return tensor


cdef public api object pyarrow_wrap_column(const shared_ptr[CColumn]& ccolumn):
    cdef Column column = Column()
    column.init(ccolumn)
    return column


cdef public api object pyarrow_wrap_table(const shared_ptr[CTable]& ctable):
    cdef Table table = Table()
    table.init(ctable)
    return table


cdef public api object pyarrow_wrap_batch(
    const shared_ptr[CRecordBatch]& cbatch):
    cdef RecordBatch batch = RecordBatch()
    batch.init(cbatch)
    return batch
