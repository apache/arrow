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

from pyarrow.compat import pickle

cdef extern from "arrow/python/api.h" namespace 'arrow::py':

    CStatus SerializePythonSequence(
        PyObject* sequence,
        shared_ptr[CRecordBatch]* batch_out,
        c_vector[shared_ptr[CTensor]]* tensors_out)

    CStatus DeserializePythonSequence(
        shared_ptr[CRecordBatch] batch,
        c_vector[shared_ptr[CTensor]] tensors,
        PyObject* base,
        PyObject** out)

cdef extern from "arrow/python/api.h" namespace 'arrow::py' nogil:

    cdef CStatus WriteSerializedPythonSequence(
        shared_ptr[CRecordBatch] batch,
        c_vector[shared_ptr[CTensor]] tensors,
        OutputStream* dst)
    
    cdef CStatus ReadSerializedPythonSequence(
        shared_ptr[RandomAccessFile] src,
        shared_ptr[CRecordBatch]* batch_out,
        c_vector[shared_ptr[CTensor]]* tensors_out)

cdef extern from "arrow/python/python_to_arrow.h":

    cdef extern PyObject *pyarrow_serialize_callback

    cdef extern PyObject *pyarrow_deserialize_callback


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
def serialize_sequence(object value, NativeFile sink):
    cdef shared_ptr[OutputStream] stream
    sink.write_handle(&stream)

    cdef shared_ptr[CRecordBatch] batch
    cdef c_vector[shared_ptr[CTensor]] tensors

    check_status(SerializePythonSequence(<PyObject*> value, &batch, &tensors))

    with nogil:
        check_status(WriteSerializedPythonSequence(batch, tensors, stream.get()))

# Main entry point for deserialization
def deserialize_sequence(NativeFile source, object base):
    cdef shared_ptr[RandomAccessFile] stream
    source.read_handle(&stream)

    cdef shared_ptr[CRecordBatch] batch
    cdef c_vector[shared_ptr[CTensor]] tensors

    with nogil:
        check_status(ReadSerializedPythonSequence(stream, &batch, &tensors))

    cdef PyObject* result
    check_status(DeserializePythonSequence(batch, tensors, <PyObject*> base, &result))
    return <object> result
