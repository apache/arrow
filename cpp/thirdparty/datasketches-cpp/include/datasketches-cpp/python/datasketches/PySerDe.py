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

from _datasketches import PyObjectSerDe

import struct

# This file provides several Python SerDe implementation examples.
#
# Each implementation must extend the PyObjectSerDe class and define
# three methods:
#   * get_size(item) returns an int of the number of bytes needed to
#     serialize the given item
#   * to_bytes(item) returns a bytes object representing a serialized
#     version of the given item
#   * from_bytes(data, offset) takes a bytes object (data) and an offset
#     indicating where in the data array to start reading. The method
#     returns a tuple with the newly reconstructed object and the
#     total number of bytes beyond the offset read from the input data.

# Implements a simple string-encoding scheme where a string is
# written as <num_bytes> <string_contents>, with no null termination.
# This format allows pre-allocating each string, at the cost of
# additional storage. Using this format, the serialized string consumes
# 4 + len(item) bytes.
class PyStringsSerDe(PyObjectSerDe):
  def get_size(self, item):
    return int(4 + len(item))

  def to_bytes(self, item: str):
    b = bytearray()
    b.extend(len(item).to_bytes(4, 'little'))
    b.extend(map(ord,item))
    return bytes(b)

  def from_bytes(self, data: bytes, offset: int):
    num_chars = int.from_bytes(data[offset:offset+3], 'little')
    if (num_chars < 0 or num_chars > offset + len(data)):
        raise IndexError(f'num_chars read must be non-negative and not larger than the buffer. Found {num_chars}')
    str = data[offset+4:offset+4+num_chars].decode()
    return (str, 4+num_chars)

# Implements an integer-encoding scheme where each integer is written
# as a 32-bit (4 byte) little-endian value.
class PyIntsSerDe(PyObjectSerDe):
  def get_size(self, item):
    return int(4)

  def to_bytes(self, item):
    return struct.pack('i', item)

  def from_bytes(self, data: bytes, offset: int):
    val = struct.unpack_from('i', data, offset)[0]
    return (val, 4)


class PyLongsSerDe(PyObjectSerDe):
  def get_size(self, item):
    return int(8)

  def to_bytes(self, item):
    return struct.pack('l', item)

  def from_bytes(self, data: bytes, offset: int):
    val = struct.unpack_from('l', data, offset)[0]
    return (val, 8)


class PyFloatsSerDe(PyObjectSerDe):
  def get_size(self, item):
    return int(4)

  def to_bytes(self, item):
    return struct.pack('f', item)

  def from_bytes(self, data: bytes, offset: int):
    val = struct.unpack_from('f', data, offset)[0]
    return (val, 4)


class PyDoublesSerDe(PyObjectSerDe):
  def get_size(self, item):
    return int(8)

  def to_bytes(self, item):
    return struct.pack('d', item)

  def from_bytes(self, data: bytes, offset: int):
    val = struct.unpack_from('d', data, offset)[0]
    return (val, 8)
