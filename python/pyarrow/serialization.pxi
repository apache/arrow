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

from libcpp cimport bool as c_bool, nullptr
from libcpp.vector cimport vector as c_vector
from cpython.ref cimport PyObject
from cython.operator cimport dereference as deref

try:
    import cloudpickle as pickle
except ImportError:
    try:
        import cPickle as pickle
    except ImportError:
        import pickle

from pyarrow.lib cimport Buffer, NativeFile, check_status, _RecordBatchFileWriter

cdef extern from "arrow/python/python_to_arrow.h":

    cdef CStatus SerializeSequences(c_vector[PyObject*] sequences,
        int32_t recursion_depth, shared_ptr[CArray]* array_out,
        c_vector[PyObject*]& tensors_out)

    cdef shared_ptr[CRecordBatch] MakeBatch(shared_ptr[CArray] data)

    cdef extern PyObject *pyarrow_serialize_callback

    cdef extern PyObject *pyarrow_deserialize_callback

cdef extern from "arrow/python/arrow_to_python.h":

    cdef CStatus DeserializeList(shared_ptr[CArray] array, int32_t start_idx,
        int32_t stop_idx, PyObject* base,
        const c_vector[shared_ptr[CTensor]]& tensors, PyObject** out)

cdef class PythonObject:

    cdef:
        shared_ptr[CRecordBatch] batch
        c_vector[shared_ptr[CTensor]] tensors

    def __cinit__(self):
        pass


def is_named_tuple(cls):
    """Return True if cls is a namedtuple and False otherwise."""
    b = cls.__bases__
    if len(b) != 1 or b[0] != tuple:
        return False
    f = getattr(cls, "_fields", None)
    if not isinstance(f, tuple):
        return False
    return all(type(n) == str for n in f)


class SerializationException(Exception):
    def __init__(self, message, example_object):
        Exception.__init__(self, message)
        self.example_object = example_object


class DeserializationException(Exception):
    def __init__(self, message, type_id):
        Exception.__init__(self, message)
        self.type_id = type_id


# Types with special serialization handlers
type_to_type_id = dict()
whitelisted_types = dict()
types_to_pickle = set()
custom_serializers = dict()
custom_deserializers = dict()

def register_type(type, type_id, pickle=False, custom_serializer=None, custom_deserializer=None):
    """Add type to the list of types we can serialize.

    Args:
        type (type): The type that we can serialize.
        type_id: A string of bytes used to identify the type.
        pickle (bool): True if the serialization should be done with pickle.
            False if it should be done efficiently with Arrow.
        custom_serializer: This argument is optional, but can be provided to
            serialize objects of the class in a particular way.
        custom_deserializer: This argument is optional, but can be provided to
            deserialize objects of the class in a particular way.
    """
    type_to_type_id[type] = type_id
    whitelisted_types[type_id] = type
    if pickle:
        types_to_pickle.add(type_id)
    if custom_serializer is not None:
        custom_serializers[type_id] = custom_serializer
        custom_deserializers[type_id] = custom_deserializer

def serialization_callback(obj):
    if type(obj) not in type_to_type_id:
        raise SerializationException("pyarrow does not know how to "
                                     "serialize objects of type {}."
                                     .format(type(obj)),
                                     obj)
    type_id = type_to_type_id[type(obj)]
    if type_id in types_to_pickle:
        serialized_obj = {"data": pickle.dumps(obj), "pickle": True}
    elif type_id in custom_serializers:
        serialized_obj = {"data": custom_serializers[type_id](obj)}
    else:
        if is_named_tuple(type(obj)):
            serialized_obj = {}
            serialized_obj["_pa_getnewargs_"] = obj.__getnewargs__()
        elif hasattr(obj, "__dict__"):
            serialized_obj = obj.__dict__
        else:
            raise SerializationException("We do not know how to serialize "
                                         "the object '{}'".format(obj), obj)
    return dict(serialized_obj, **{"_pytype_": type_id})

def deserialization_callback(serialized_obj):
    type_id = serialized_obj["_pytype_"]

    if "pickle" in serialized_obj:
        # The object was pickled, so unpickle it.
        obj = pickle.loads(serialized_obj["data"])
    else:
        assert type_id not in types_to_pickle
        if type_id not in whitelisted_types:
            raise "error"
        type = whitelisted_types[type_id]
        if type_id in custom_deserializers:
            obj = custom_deserializers[type_id](serialized_obj["data"])
        else:
            # In this case, serialized_obj should just be the __dict__ field.
            if "_pa_getnewargs_" in serialized_obj:
                obj = type.__new__(type, *serialized_obj["_pa_getnewargs_"])
            else:
                obj = type.__new__(type)
                serialized_obj.pop("_pytype_")
                obj.__dict__.update(serialized_obj)
    return obj

pyarrow_serialize_callback = <PyObject*> serialization_callback

pyarrow_deserialize_callback = <PyObject*> deserialization_callback

# Main entry point for serialization
def serialize_sequence(object value):
    cdef int32_t recursion_depth = 0
    cdef PythonObject result = PythonObject()
    cdef c_vector[PyObject*] sequences
    cdef shared_ptr[CArray] array
    cdef c_vector[PyObject*] tensors
    cdef PyObject* tensor
    cdef shared_ptr[CTensor] out
    sequences.push_back(<PyObject*> value)
    check_status(SerializeSequences(sequences, recursion_depth, &array, tensors))
    result.batch = MakeBatch(array)
    num_tensors = 0
    for tensor in tensors:
        check_status(NdarrayToTensor(c_default_memory_pool(), <object> tensor, &out))
        result.tensors.push_back(out)
        num_tensors += 1
    return result, num_tensors

# Main entry point for deserialization
def deserialize_sequence(PythonObject value, object base):
    cdef PyObject* result
    check_status(DeserializeList(deref(value.batch).column(0), 0, deref(value.batch).num_rows(), <PyObject*> base, value.tensors, &result))
    return <object> result

def write_python_object(PythonObject value, int32_t num_tensors, NativeFile sink):
    cdef shared_ptr[OutputStream] stream
    sink.write_handle(&stream)
    cdef shared_ptr[CRecordBatchStreamWriter] writer
    cdef shared_ptr[CSchema] schema = deref(value.batch).schema()
    cdef shared_ptr[CRecordBatch] batch = value.batch
    cdef shared_ptr[CTensor] tensor
    cdef int32_t metadata_length
    cdef int64_t body_length

    with nogil:
        # write number of tensors
        check_status(stream.get().Write(<uint8_t*> &num_tensors, sizeof(int32_t)))

        check_status(CRecordBatchStreamWriter.Open(stream.get(), schema, &writer))
        check_status(deref(writer).WriteRecordBatch(deref(batch)))
        check_status(deref(writer).Close())

    for tensor in value.tensors:
        check_status(WriteTensor(deref(tensor), stream.get(), &metadata_length, &body_length))

def read_python_object(NativeFile source):
    cdef PythonObject result = PythonObject()
    cdef shared_ptr[RandomAccessFile] stream
    source.read_handle(&stream)
    cdef shared_ptr[CRecordBatchStreamReader] reader
    cdef shared_ptr[CTensor] tensor
    cdef int64_t offset
    cdef int64_t bytes_read
    cdef int32_t num_tensors
    
    with nogil:
        # read number of tensors
        check_status(stream.get().Read(sizeof(int32_t), &bytes_read, <uint8_t*> &num_tensors))

        check_status(CRecordBatchStreamReader.Open(<shared_ptr[InputStream]> stream, &reader))
        check_status(reader.get().ReadNextRecordBatch(&result.batch))

        check_status(deref(stream).Tell(&offset))

        for i in range(num_tensors):
            check_status(ReadTensor(offset, stream.get(), &tensor))
            result.tensors.push_back(tensor)
            check_status(deref(stream).Tell(&offset))

    return result
