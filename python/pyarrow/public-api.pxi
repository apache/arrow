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

# You cannot assign something to a dereferenced pointer in Cython thus these
# methods don't use Status to indicate a successful operation.


cdef api bint pyarrow_is_buffer(object buffer):
    return isinstance(buffer, Buffer)


cdef api shared_ptr[CBuffer] pyarrow_unwrap_buffer(object buffer):
    cdef Buffer buf
    if pyarrow_is_buffer(buffer):
        buf = <Buffer>(buffer)
        return buf.buffer

    return shared_ptr[CBuffer]()


cdef api object pyarrow_wrap_buffer(const shared_ptr[CBuffer]& buf):
    cdef Buffer result = Buffer.__new__(Buffer)
    result.init(buf)
    return result


cdef api object pyarrow_wrap_resizable_buffer(
        const shared_ptr[CResizableBuffer]& buf):
    cdef ResizableBuffer result = ResizableBuffer.__new__(ResizableBuffer)
    result.init_rz(buf)
    return result


cdef api bint pyarrow_is_data_type(object type_):
    return isinstance(type_, DataType)


cdef api shared_ptr[CDataType] pyarrow_unwrap_data_type(
        object data_type):
    cdef DataType type_
    if pyarrow_is_data_type(data_type):
        type_ = <DataType>(data_type)
        return type_.sp_type

    return shared_ptr[CDataType]()


cdef api object pyarrow_wrap_data_type(
        const shared_ptr[CDataType]& type):
    cdef DataType out

    if type.get() == NULL:
        return None

    if type.get().id() == _Type_DICTIONARY:
        out = DictionaryType.__new__(DictionaryType)
    elif type.get().id() == _Type_LIST:
        out = ListType.__new__(ListType)
    elif type.get().id() == _Type_STRUCT:
        out = StructType.__new__(StructType)
    elif type.get().id() == _Type_UNION:
        out = UnionType.__new__(UnionType)
    elif type.get().id() == _Type_TIMESTAMP:
        out = TimestampType.__new__(TimestampType)
    elif type.get().id() == _Type_FIXED_SIZE_BINARY:
        out = FixedSizeBinaryType.__new__(FixedSizeBinaryType)
    elif type.get().id() == _Type_DECIMAL:
        out = Decimal128Type.__new__(Decimal128Type)
    else:
        out = DataType.__new__(DataType)

    out.init(type)
    return out


cdef object pyarrow_wrap_metadata(
        const shared_ptr[const CKeyValueMetadata]& meta):
    cdef const CKeyValueMetadata* cmeta = meta.get()

    if cmeta == nullptr:
        return None

    result = OrderedDict()
    for i in range(cmeta.size()):
        result[cmeta.key(i)] = cmeta.value(i)

    return result


cdef shared_ptr[CKeyValueMetadata] pyarrow_unwrap_metadata(object meta):
    cdef vector[c_string] keys, values

    if isinstance(meta, dict):
        keys = map(tobytes, meta.keys())
        values = map(tobytes, meta.values())
        return make_shared[CKeyValueMetadata](keys, values)

    return shared_ptr[CKeyValueMetadata]()


cdef api bint pyarrow_is_field(object field):
    return isinstance(field, Field)


cdef api shared_ptr[CField] pyarrow_unwrap_field(object field):
    cdef Field field_
    if pyarrow_is_field(field):
        field_ = <Field>(field)
        return field_.sp_field

    return shared_ptr[CField]()


cdef api object pyarrow_wrap_field(const shared_ptr[CField]& field):
    if field.get() == NULL:
        return None
    cdef Field out = Field.__new__(Field)
    out.init(field)
    return out


cdef api bint pyarrow_is_schema(object schema):
    return isinstance(schema, Schema)


cdef api shared_ptr[CSchema] pyarrow_unwrap_schema(object schema):
    cdef Schema sch
    if pyarrow_is_schema(schema):
        sch = <Schema>(schema)
        return sch.sp_schema

    return shared_ptr[CSchema]()


cdef api object pyarrow_wrap_schema(const shared_ptr[CSchema]& schema):
    cdef Schema out = Schema.__new__(Schema)
    out.init_schema(schema)
    return out


cdef api bint pyarrow_is_array(object array):
    return isinstance(array, Array)


cdef api shared_ptr[CArray] pyarrow_unwrap_array(object array):
    cdef Array arr
    if pyarrow_is_array(array):
        arr = <Array>(array)
        return arr.sp_array

    return shared_ptr[CArray]()


cdef api object pyarrow_wrap_array(const shared_ptr[CArray]& sp_array):
    if sp_array.get() == NULL:
        raise ValueError('Array was NULL')

    cdef CDataType* data_type = sp_array.get().type().get()

    if data_type == NULL:
        raise ValueError('Array data type was NULL')

    klass = _array_classes[data_type.id()]

    cdef Array arr = klass.__new__(klass)
    arr.init(sp_array)
    return arr


cdef api object pyarrow_wrap_chunked_array(
        const shared_ptr[CChunkedArray]& sp_array):
    if sp_array.get() == NULL:
        raise ValueError('ChunkedArray was NULL')

    cdef CDataType* data_type = sp_array.get().type().get()

    if data_type == NULL:
        raise ValueError('ChunkedArray data type was NULL')

    cdef ChunkedArray arr = ChunkedArray.__new__(ChunkedArray)
    arr.init(sp_array)
    return arr


cdef api bint pyarrow_is_tensor(object tensor):
    return isinstance(tensor, Tensor)


cdef api shared_ptr[CTensor] pyarrow_unwrap_tensor(object tensor):
    cdef Tensor ten
    if pyarrow_is_tensor(tensor):
        ten = <Tensor>(tensor)
        return ten.sp_tensor

    return shared_ptr[CTensor]()


cdef api object pyarrow_wrap_tensor(
        const shared_ptr[CTensor]& sp_tensor):
    if sp_tensor.get() == NULL:
        raise ValueError('Tensor was NULL')

    cdef Tensor tensor = Tensor.__new__(Tensor)
    tensor.init(sp_tensor)
    return tensor


cdef api bint pyarrow_is_column(object column):
    return isinstance(column, Column)


cdef api shared_ptr[CColumn] pyarrow_unwrap_column(object column):
    cdef Column col
    if pyarrow_is_column(column):
        col = <Column>(column)
        return col.sp_column

    return shared_ptr[CColumn]()


cdef api object pyarrow_wrap_column(const shared_ptr[CColumn]& ccolumn):
    cdef Column column = Column.__new__(Column)
    column.init(ccolumn)
    return column


cdef api bint pyarrow_is_table(object table):
    return isinstance(table, Table)


cdef api shared_ptr[CTable] pyarrow_unwrap_table(object table):
    cdef Table tab
    if pyarrow_is_table(table):
        tab = <Table>(table)
        return tab.sp_table

    return shared_ptr[CTable]()


cdef api object pyarrow_wrap_table(const shared_ptr[CTable]& ctable):
    cdef Table table = Table.__new__(Table)
    table.init(ctable)
    return table


cdef api bint pyarrow_is_batch(object batch):
    return isinstance(batch, RecordBatch)


cdef api shared_ptr[CRecordBatch] pyarrow_unwrap_batch(object batch):
    cdef RecordBatch bat
    if pyarrow_is_batch(batch):
        bat = <RecordBatch>(batch)
        return bat.sp_batch

    return shared_ptr[CRecordBatch]()


cdef api object pyarrow_wrap_batch(
        const shared_ptr[CRecordBatch]& cbatch):
    cdef RecordBatch batch = RecordBatch.__new__(RecordBatch)
    batch.init(cbatch)
    return batch
