import sys

import pyarrow as pa

class BufferClass(object):
    pass

def serialize_buffer_class(obj):
    return pa.frombuffer(b"hello")

def deserialize_buffer_class(serialized_obj):
    return serialized_obj

pa._default_serialization_context.register_type(
    BufferClass, "BufferClass",
    custom_serializer=serialize_buffer_class,
    custom_deserializer=deserialize_buffer_class)

with open(sys.argv[1], "r") as f:
    data = f.read()
    pa.deserialize(data)
