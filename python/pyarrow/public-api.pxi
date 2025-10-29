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
from pyarrow.includes.libarrow cimport (
    CArray, CDataType, CExpression,
    CField, CRecordBatch, CRecordBatchReader,
    CSchema, CTable, CTensor,
    CSparseCOOTensor, CSparseCSRMatrix,
    CSparseCSCMatrix, CSparseCSFTensor
)
# from pyarrow.includes.libarrow_dataset cimport (CDataset, CFragment,
#                                                 CPartitioning, CScanner)
# from pyarrow.includes.libarrow_fs cimport CFileSystem

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


# Workaround for Cython parsing bug
# https://github.com/cython/cython/issues/2143
ctypedef const CPyExtensionType* _CPyExtensionTypePtr


cdef api object pyarrow_wrap_data_type(
        const shared_ptr[CDataType]& type):
    cdef:
        const CExtensionType* ext_type
        const CPyExtensionType* cpy_ext_type
        DataType out

    if type.get() == NULL:
        return None

    if type.get().id() == _Type_DICTIONARY:
        out = DictionaryType.__new__(DictionaryType)
    elif type.get().id() == _Type_LIST:
        out = ListType.__new__(ListType)
    elif type.get().id() == _Type_LARGE_LIST:
        out = LargeListType.__new__(LargeListType)
    elif type.get().id() == _Type_LIST_VIEW:
        out = ListViewType.__new__(ListViewType)
    elif type.get().id() == _Type_LARGE_LIST_VIEW:
        out = LargeListViewType.__new__(LargeListViewType)
    elif type.get().id() == _Type_MAP:
        out = MapType.__new__(MapType)
    elif type.get().id() == _Type_FIXED_SIZE_LIST:
        out = FixedSizeListType.__new__(FixedSizeListType)
    elif type.get().id() == _Type_STRUCT:
        out = StructType.__new__(StructType)
    elif type.get().id() == _Type_SPARSE_UNION:
        out = SparseUnionType.__new__(SparseUnionType)
    elif type.get().id() == _Type_DENSE_UNION:
        out = DenseUnionType.__new__(DenseUnionType)
    elif type.get().id() == _Type_TIME32:
        out = Time32Type.__new__(Time32Type)
    elif type.get().id() == _Type_TIME64:
        out = Time64Type.__new__(Time64Type)
    elif type.get().id() == _Type_TIMESTAMP:
        out = TimestampType.__new__(TimestampType)
    elif type.get().id() == _Type_DURATION:
        out = DurationType.__new__(DurationType)
    elif type.get().id() == _Type_FIXED_SIZE_BINARY:
        out = FixedSizeBinaryType.__new__(FixedSizeBinaryType)
    elif type.get().id() == _Type_DECIMAL32:
        out = Decimal32Type.__new__(Decimal32Type)
    elif type.get().id() == _Type_DECIMAL64:
        out = Decimal64Type.__new__(Decimal64Type)
    elif type.get().id() == _Type_DECIMAL128:
        out = Decimal128Type.__new__(Decimal128Type)
    elif type.get().id() == _Type_DECIMAL256:
        out = Decimal256Type.__new__(Decimal256Type)
    elif type.get().id() == _Type_RUN_END_ENCODED:
        out = RunEndEncodedType.__new__(RunEndEncodedType)
    elif type.get().id() == _Type_EXTENSION:
        ext_type = <const CExtensionType*> type.get()
        cpy_ext_type = dynamic_cast[_CPyExtensionTypePtr](ext_type)
        extension_name = ext_type.extension_name()
        if cpy_ext_type != nullptr:
            return cpy_ext_type.GetInstance()
        elif extension_name == b"arrow.bool8":
            out = Bool8Type.__new__(Bool8Type)
        elif extension_name == b"arrow.fixed_shape_tensor":
            out = FixedShapeTensorType.__new__(FixedShapeTensorType)
        elif extension_name == b"arrow.opaque":
            out = OpaqueType.__new__(OpaqueType)
        elif extension_name == b"arrow.uuid":
            out = UuidType.__new__(UuidType)
        elif extension_name == b"arrow.json":
            out = JsonType.__new__(JsonType)
        else:
            out = BaseExtensionType.__new__(BaseExtensionType)
    else:
        out = DataType.__new__(DataType)

    out.init(type)
    return out


cdef object pyarrow_wrap_metadata(
        const shared_ptr[const CKeyValueMetadata]& meta):
    if meta.get() == nullptr:
        return None
    else:
        return KeyValueMetadata.wrap(meta)


cdef api bint pyarrow_is_metadata(object metadata):
    return isinstance(metadata, KeyValueMetadata)


cdef shared_ptr[const CKeyValueMetadata] pyarrow_unwrap_metadata(object meta):
    cdef shared_ptr[const CKeyValueMetadata] c_meta
    if pyarrow_is_metadata(meta):
        c_meta = (<KeyValueMetadata>meta).unwrap()
    return c_meta


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

    klass = get_array_class_from_type(sp_array.get().type())

    cdef Array arr = klass.__new__(klass)
    arr.init(sp_array)
    return arr


cdef api bint pyarrow_is_chunked_array(object array):
    return isinstance(array, ChunkedArray)


cdef api shared_ptr[CChunkedArray] pyarrow_unwrap_chunked_array(object array):
    cdef ChunkedArray arr
    if pyarrow_is_chunked_array(array):
        arr = <ChunkedArray>(array)
        return arr.sp_chunked_array

    return shared_ptr[CChunkedArray]()


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


cdef api bint pyarrow_is_scalar(object value):
    return isinstance(value, Scalar)


cdef api shared_ptr[CScalar] pyarrow_unwrap_scalar(object scalar):
    if pyarrow_is_scalar(scalar):
        return (<Scalar> scalar).unwrap()
    return shared_ptr[CScalar]()


cdef api object pyarrow_wrap_scalar(const shared_ptr[CScalar]& sp_scalar):
    if sp_scalar.get() == NULL:
        raise ValueError('Scalar was NULL')

    cdef CDataType* data_type = sp_scalar.get().type.get()

    if data_type == NULL:
        raise ValueError('Scalar data type was NULL')

    if data_type.id() == _Type_NA:
        return _NULL

    if data_type.id() not in _scalar_classes:
        raise ValueError('Scalar type not supported')

    klass = get_scalar_class_from_type(sp_scalar.get().type)

    cdef Scalar scalar = klass.__new__(klass)
    scalar.init(sp_scalar)
    return scalar


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


cdef api bint pyarrow_is_sparse_coo_tensor(object sparse_tensor):
    return isinstance(sparse_tensor, SparseCOOTensor)

cdef api shared_ptr[CSparseCOOTensor] pyarrow_unwrap_sparse_coo_tensor(
        object sparse_tensor):
    cdef SparseCOOTensor sten
    if pyarrow_is_sparse_coo_tensor(sparse_tensor):
        sten = <SparseCOOTensor>(sparse_tensor)
        return sten.sp_sparse_tensor

    return shared_ptr[CSparseCOOTensor]()

cdef api object pyarrow_wrap_sparse_coo_tensor(
        const shared_ptr[CSparseCOOTensor]& sp_sparse_tensor):
    if sp_sparse_tensor.get() == NULL:
        raise ValueError('SparseCOOTensor was NULL')

    cdef SparseCOOTensor sparse_tensor = SparseCOOTensor.__new__(
        SparseCOOTensor)
    sparse_tensor.init(sp_sparse_tensor)
    return sparse_tensor


cdef api bint pyarrow_is_sparse_csr_matrix(object sparse_tensor):
    return isinstance(sparse_tensor, SparseCSRMatrix)

cdef api shared_ptr[CSparseCSRMatrix] pyarrow_unwrap_sparse_csr_matrix(
        object sparse_tensor):
    cdef SparseCSRMatrix sten
    if pyarrow_is_sparse_csr_matrix(sparse_tensor):
        sten = <SparseCSRMatrix>(sparse_tensor)
        return sten.sp_sparse_tensor

    return shared_ptr[CSparseCSRMatrix]()

cdef api object pyarrow_wrap_sparse_csr_matrix(
        const shared_ptr[CSparseCSRMatrix]& sp_sparse_tensor):
    if sp_sparse_tensor.get() == NULL:
        raise ValueError('SparseCSRMatrix was NULL')

    cdef SparseCSRMatrix sparse_tensor = SparseCSRMatrix.__new__(
        SparseCSRMatrix)
    sparse_tensor.init(sp_sparse_tensor)
    return sparse_tensor


cdef api bint pyarrow_is_sparse_csc_matrix(object sparse_tensor):
    return isinstance(sparse_tensor, SparseCSCMatrix)

cdef api shared_ptr[CSparseCSCMatrix] pyarrow_unwrap_sparse_csc_matrix(
        object sparse_tensor):
    cdef SparseCSCMatrix sten
    if pyarrow_is_sparse_csc_matrix(sparse_tensor):
        sten = <SparseCSCMatrix>(sparse_tensor)
        return sten.sp_sparse_tensor

    return shared_ptr[CSparseCSCMatrix]()

cdef api object pyarrow_wrap_sparse_csc_matrix(
        const shared_ptr[CSparseCSCMatrix]& sp_sparse_tensor):
    if sp_sparse_tensor.get() == NULL:
        raise ValueError('SparseCSCMatrix was NULL')

    cdef SparseCSCMatrix sparse_tensor = SparseCSCMatrix.__new__(
        SparseCSCMatrix)
    sparse_tensor.init(sp_sparse_tensor)
    return sparse_tensor


cdef api bint pyarrow_is_sparse_csf_tensor(object sparse_tensor):
    return isinstance(sparse_tensor, SparseCSFTensor)

cdef api shared_ptr[CSparseCSFTensor] pyarrow_unwrap_sparse_csf_tensor(
        object sparse_tensor):
    cdef SparseCSFTensor sten
    if pyarrow_is_sparse_csf_tensor(sparse_tensor):
        sten = <SparseCSFTensor>(sparse_tensor)
        return sten.sp_sparse_tensor

    return shared_ptr[CSparseCSFTensor]()

cdef api object pyarrow_wrap_sparse_csf_tensor(
        const shared_ptr[CSparseCSFTensor]& sp_sparse_tensor):
    if sp_sparse_tensor.get() == NULL:
        raise ValueError('SparseCSFTensor was NULL')

    cdef SparseCSFTensor sparse_tensor = SparseCSFTensor.__new__(
        SparseCSFTensor)
    sparse_tensor.init(sp_sparse_tensor)
    return sparse_tensor


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


cdef api bint pyarrow_is_expression(object expr):
    return isinstance(expr, _pc().Expression)


cdef api shared_ptr[CExpression] pyarrow_unwrap_expression(object expr):
    from pyarrow.compute import Expression
    cdef Expression expression
    if pyarrow_is_expression(expr):
        expression = <Expression>(expr)
        return expression.unwrap()

    return shared_ptr[CExpression]()


cdef api object pyarrow_wrap_expression(const shared_ptr[CExpression]& cexpr):
    if cexpr.get() == NULL:
        raise ValueError('Expression was NULL')

    from pyarrow.compute import Expression
    cdef Expression expr = Expression.__new__(Expression)
    expr.init(cexpr)
    return expr


# cdef api bint pyarrow_is_filesystem(object fs):
#     return isinstance(fs, FileSystem)


# cdef api shared_ptr[CFileSystem] pyarrow_unwrap_filesystem(object fs):
#     cdef FileSystem filesystem
#     if pyarrow_is_filesystem(fs):
#         filesystem = <FileSystem>(fs)
#         return filesystem.unwrap()

#     return shared_ptr[CFileSystem]()


# cdef api object pyarrow_wrap_filesystem(const shared_ptr[CFileSystem]& cfs):
#     cdef FileSystem fs = FileSystem.__new__(FileSystem)
#     fs.init(cfs)
#     return fs


# cdef api bint pyarrow_is_dataset(object dataset):
#     return isinstance(dataset, Dataset)


# cdef api shared_ptr[CDataset] pyarrow_unwrap_dataset(object dataset):
#     cdef Dataset d
#     if pyarrow_is_dataset(dataset):
#         d = <Dataset>(dataset)
#         return d.unwrap()

#     return shared_ptr[CDataset]()


# cdef api object pyarrow_wrap_dataset(const shared_ptr[CDataset]& cdataset):
#     cdef Dataset dataset = Dataset.__new__(Dataset)
#     dataset.init(cdataset)
#     return dataset


# cdef api bint pyarrow_is_fragment(object frag):
#     return isinstance(frag, Fragment)


# cdef api shared_ptr[CFragment] pyarrow_unwrap_fragment(object frag):
#     cdef Fragment fragment
#     if pyarrow_is_dataset(frag):
#         fragment = <Fragment>(frag)
#         return fragment.unwrap()

#     return shared_ptr[CFragment]()


# cdef api object pyarrow_wrap_fragment(const shared_ptr[CFragment]& cfrag):
#     cdef Fragment frag = Fragment.__new__(Fragment)
#     frag.init(cfrag)
#     return frag


# cdef api bint pyarrow_is_partitioning(object part):
#     return isinstance(part, Partitioning)


# cdef api shared_ptr[CPartitioning] pyarrow_unwrap_dataset(object part):
#     cdef Partitioning partitioning
#     if pyarrow_is_partitioning(part):
#         partitioning = <Partitioning>(part)
#         return partitioning.unwrap()

#     return shared_ptr[CPartitioning]()


# cdef api object pyarrow_wrap_partitioning(const shared_ptr[CPartitioning]& cpart):
#     cdef Partitioning part = Partitioning.__new__(Partitioning)
#     part.init(cpart)
#     return part


# cdef api bint pyarrow_is_scanner(object scanner):
#     return isinstance(scanner, Scanner)


# cdef api shared_ptr[CScanner] pyarrow_unwrap_scanner(object scanner):
#     cdef Scanner s
#     if pyarrow_is_scanner(scanner):
#         s = <Scanner>(scanner)
#         return s.unwrap()

#     return shared_ptr[CScanner]()


# cdef api object pyarrow_wrap_scanner(const shared_ptr[CScanner]& cscanner):
#     cdef Scanner scanner = Scanner.__new__(Scanner)
#     scanner.init(cscanner)
#     return scanner


# cdef api bint pyarrow_is_record_batch_reader(object rdr):
#     return isinstance(rdr, RecordBatchReader)


# cdef api shared_ptr[CRecordBatchReader] pyarrow_unwrap_record_batch_reader(object rdr):
#     cdef RecordBatchReader record_batch_reader
#     if pyarrow_is_record_batch_reader(rdr):
#         record_batch_reader = <RecordBatchReader>(rdr)
#         return record_batch_reader.unwrap()

#     return shared_ptr[CRecordBatchReader]()


# cdef api object pyarrow_wrap_record_batch_reader(const shared_ptr[CRecordBatchReader]& crdr):
#     cdef RecordBatchReader rdr = RecordBatchReader.__new__(RecordBatchReader)
#     rdr.init(crdr)
#     return rdr
