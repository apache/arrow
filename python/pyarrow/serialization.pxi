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


class SerializationCallbackError(ArrowSerializationError):
    def __init__(self, message, example_object):
        ArrowSerializationError.__init__(self, message)
        self.example_object = example_object


class DeserializationCallbackError(ArrowSerializationError):
    def __init__(self, message, type_id):
        ArrowSerializationError.__init__(self, message)
        self.type_id = type_id


cdef class SerializationContext:
    cdef:
        object type_to_type_id
        object whitelisted_types
        object types_to_pickle
        object custom_serializers
        object custom_deserializers

    def __init__(self):
        # Types with special serialization handlers
        self.type_to_type_id = dict()
        self.whitelisted_types = dict()
        self.types_to_pickle = set()
        self.custom_serializers = dict()
        self.custom_deserializers = dict()

    def register_type(self, type_, type_id, pickle=False,
                      custom_serializer=None, custom_deserializer=None):
        """EXPERIMENTAL: Add type to the list of types we can serialize.

        Parameters
        ----------
        type_ : TypeType
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
        self.type_to_type_id[type_] = type_id
        self.whitelisted_types[type_id] = type_
        if pickle:
            self.types_to_pickle.add(type_id)
        if custom_serializer is not None:
            self.custom_serializers[type_id] = custom_serializer
            self.custom_deserializers[type_id] = custom_deserializer

    def _serialize_callback(self, obj):
        found = False
        for type_ in type(obj).__mro__:
            if type_ in self.type_to_type_id:
                found = True
                break

        if not found:
            raise SerializationCallbackError(
                "pyarrow does not know how to "
                "serialize objects of type {}.".format(type(obj)), obj
            )

        # use the closest match to type(obj)
        type_id = self.type_to_type_id[type_]
        if type_id in self.types_to_pickle:
            serialized_obj = {"data": pickle.dumps(obj), "pickle": True}
        elif type_id in self.custom_serializers:
            serialized_obj = {"data": self.custom_serializers[type_id](obj)}
        else:
            if is_named_tuple(type_):
                serialized_obj = {}
                serialized_obj["_pa_getnewargs_"] = obj.__getnewargs__()
            elif hasattr(obj, "__dict__"):
                serialized_obj = obj.__dict__
            else:
                msg = "We do not know how to serialize " \
                      "the object '{}'".format(obj)
                raise SerializationCallbackError(msg, obj)
        return dict(serialized_obj, **{"_pytype_": type_id})

    def _deserialize_callback(self, serialized_obj):
        type_id = serialized_obj["_pytype_"]

        if "pickle" in serialized_obj:
            # The object was pickled, so unpickle it.
            obj = pickle.loads(serialized_obj["data"])
        else:
            assert type_id not in self.types_to_pickle
            if type_id not in self.whitelisted_types:
                msg = "Type ID " + str(type_id) + " not registered in " \
                      "deserialization callback"
                raise DeserializationCallbackError(msg, type_id)
            type_ = self.whitelisted_types[type_id]
            if type_id in self.custom_deserializers:
                obj = self.custom_deserializers[type_id](
                    serialized_obj["data"])
            else:
                # In this case, serialized_obj should just be
                # the __dict__ field.
                if "_pa_getnewargs_" in serialized_obj:
                    obj = type_.__new__(
                        type_, *serialized_obj["_pa_getnewargs_"])
                else:
                    obj = type_.__new__(type_)
                    serialized_obj.pop("_pytype_")
                    obj.__dict__.update(serialized_obj)
        return obj


_default_serialization_context = SerializationContext()


cdef class SerializedPyObject:
    """
    Arrow-serialized representation of Python object
    """
    cdef:
        CSerializedPyObject data

    cdef readonly:
        object base

    property total_bytes:

        def __get__(self):
            cdef CMockOutputStream mock_stream
            with nogil:
                check_status(WriteSerializedObject(self.data, &mock_stream))

            return mock_stream.GetExtentBytesWritten()

    def write_to(self, sink):
        """
        Write serialized object to a sink
        """
        cdef shared_ptr[OutputStream] stream
        get_writer(sink, &stream)
        self._write_to(stream.get())

    cdef _write_to(self, OutputStream* stream):
        with nogil:
            check_status(WriteSerializedObject(self.data, stream))

    def deserialize(self, SerializationContext context=None):
        """
        Convert back to Python object
        """
        cdef PyObject* result

        if context is None:
            context = _default_serialization_context

        with nogil:
            check_status(DeserializeObject(context, self.data,
                                           <PyObject*> self.base, &result))

        # PyObject_to_object is necessary to avoid a memory leak;
        # also unpack the list the object was wrapped in in serialize
        return PyObject_to_object(result)[0]

    def to_buffer(self, nthreads=1):
        """
        Write serialized data as Buffer
        """
        cdef Buffer output = allocate_buffer(self.total_bytes)
        sink = FixedSizeBufferWriter(output)
        if nthreads > 1:
            sink.set_memcopy_threads(nthreads)
        self.write_to(sink)
        return output


def serialize(object value, SerializationContext context=None):
    """EXPERIMENTAL: Serialize a Python sequence

    Parameters
    ----------
    value: object
        Python object for the sequence that is to be serialized.
    context : SerializationContext
        Custom serialization and deserialization context, uses a default
        context with some standard type handlers if not specified

    Returns
    -------
    serialized : SerializedPyObject
    """
    cdef SerializedPyObject serialized = SerializedPyObject()
    wrapped_value = [value]

    if context is None:
        context = _default_serialization_context

    with nogil:
        check_status(SerializeObject(context, wrapped_value, &serialized.data))
    return serialized


def serialize_to(object value, sink, SerializationContext context=None):
    """EXPERIMENTAL: Serialize a Python sequence to a file.

    Parameters
    ----------
    value: object
        Python object for the sequence that is to be serialized.
    sink: NativeFile or file-like
        File the sequence will be written to.
    context : SerializationContext
        Custom serialization and deserialization context, uses a default
        context with some standard type handlers if not specified
    """
    serialized = serialize(value, context)
    serialized.write_to(sink)


def read_serialized(source, base=None):
    """EXPERIMENTAL: Read serialized Python sequence from file-like object

    Parameters
    ----------
    source: NativeFile
        File to read the sequence from.
    base: object
        This object will be the base object of all the numpy arrays
        contained in the sequence.

    Returns
    -------
    serialized : the serialized data
    """
    cdef shared_ptr[RandomAccessFile] stream
    get_reader(source, &stream)

    cdef SerializedPyObject serialized = SerializedPyObject()
    serialized.base = base
    with nogil:
        check_status(ReadSerializedObject(stream.get(), &serialized.data))

    return serialized


def deserialize_from(source, object base, SerializationContext context=None):
    """EXPERIMENTAL: Deserialize a Python sequence from a file.

    Parameters
    ----------
    source: NativeFile
        File to read the sequence from.
    base: object
        This object will be the base object of all the numpy arrays
        contained in the sequence.
    context : SerializationContext
        Custom serialization and deserialization context

    Returns
    -------
    object
        Python object for the deserialized sequence.
    """
    serialized = read_serialized(source, base=base)
    return serialized.deserialize(context)


def deserialize(obj, SerializationContext context=None):
    """
    EXPERIMENTAL: Deserialize Python object from Buffer or other Python object
    supporting the buffer protocol

    Parameters
    ----------
    obj : pyarrow.Buffer or Python object supporting buffer protocol
    context : SerializationContext
        Custom serialization and deserialization context

    Returns
    -------
    deserialized : object
    """
    source = BufferReader(obj)
    return deserialize_from(source, obj, context)
