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

from cpython.ref cimport PyObject

from pyarrow.compat import pickle

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

def register_type(type, type_id, pickle=False,
                  custom_serializer=None, custom_deserializer=None):
    """Add type to the list of types we can serialize.

    Parameters
    ----------
    type :type
        The type that we can serialize.
    type_id : bytes
        A string of bytes used to identify the type.
    pickle : bool
        True if the serialization should be done with pickle.
        False if it should be done efficiently with Arrow.
    custom_serializer : callable
        This argument is optional, but can be provided to
        serialize objects of the class in a particular way.
    custom_deserializer : callable
        This argument is optional, but can be provided to
        deserialize objects of the class in a particular way.
    """
    type_to_type_id[type] = type_id
    whitelisted_types[type_id] = type
    if pickle:
        types_to_pickle.add(type_id)
    if custom_serializer is not None:
        custom_serializers[type_id] = custom_serializer
        custom_deserializers[type_id] = custom_deserializer

def _serialization_callback(obj):
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

def _deserialization_callback(serialized_obj):
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

set_serialization_callbacks(<PyObject*> _serialization_callback,
                            <PyObject*> _deserialization_callback)

def serialize_sequence(object value, NativeFile sink):
    """Serialize a Python sequence to a file.

    Parameters
    ----------
    value: object
        Python object for the sequence that is to be serialized.
    sink: NativeFile
        File the sequence will be written to.
    """
    cdef shared_ptr[OutputStream] stream
    sink.write_handle(&stream)

    cdef shared_ptr[CRecordBatch] batch
    cdef vector[shared_ptr[CTensor]] tensors

    with nogil:
        check_status(SerializePythonSequence(<PyObject*> value, &batch, &tensors))
        check_status(WriteSerializedPythonSequence(batch, tensors, stream.get()))

def deserialize_sequence(NativeFile source, object base):
    """Deserialize a Python sequence from a file.

    Parameters
    ----------
    source: NativeFile
        File to read the sequence from.
    base: object
        This object will be the base object of all the numpy arrays
        contained in the sequence.

    Returns
    -------
    object
        Python object for the deserialized sequence.
    """
    cdef shared_ptr[RandomAccessFile] stream
    source.read_handle(&stream)

    cdef shared_ptr[CRecordBatch] batch
    cdef vector[shared_ptr[CTensor]] tensors
    cdef PyObject* result

    with nogil:
        check_status(ReadSerializedPythonSequence(stream, &batch, &tensors))
        check_status(DeserializePythonSequence(batch, tensors, <PyObject*> base, &result))

    return <object> result
