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

from typing import Any

import pyarrow as pa

from buffer import PyArrowBuffer
from dataframe_protocol import (
    Column,
    ColumnBuffers,
    DtypeKind,
)

class PyArrowColumn(Column):
    """
    A column object, with only the methods and properties required by the
    interchange protocol defined.
    A column can contain one or more chunks. Each chunk can contain up to three
    buffers - a data buffer, a mask buffer (depending on null representation),
    and an offsets buffer (if variable-size binary; e.g., variable-length
    strings).
    Note: this Column object can only be produced by ``__dataframe__``, so
          doesn't need its own version or ``__column__`` protocol.
    """

    def __init__(self, column: pa.Array, allow_copy: bool = True) -> None:
        """
        Note: doesn't deal with extension arrays yet, just assume a regular
        Series/ndarray for now.
        """
        pass

    def size(self) -> int:
        """
        Size of the column, in elements.
        """
        pass

    @property
    def offset(self) -> int:
        """
        Offset of first element. Always zero.
        """
        pass

    @property
    def dtype(self) -> tuple[DtypeKind, int, str, str]:
        """
        Dtype description as a tuple ``(kind, bit-width, format string, endianness)``.
        Bit-width : the number of bits as an integer
        Format string : data type description format string in Apache Arrow C
                        Data Interface format.
        Endianness : current only native endianness (``=``) is supported
        Notes:
            - Kind specifiers are aligned with DLPack where possible (hence the
              jump to 20, leave enough room for future extension)
            - Masks must be specified as boolean with either bit width 1 (for bit
              masks) or 8 (for byte masks).
            - Dtype width in bits was preferred over bytes
            - Endianness isn't too useful, but included now in case in the future
              we need to support non-native endianness
            - Went with Apache Arrow format strings over NumPy format strings
              because they're more complete from a dataframe perspective
            - Format strings are mostly useful for datetime specification, and
              for categoricals.
            - For categoricals, the format string describes the type of the
              categorical in the data buffer. In case of a separate encoding of
              the categorical (e.g. an integer to string mapping), this can
              be derived from ``self.describe_categorical``.
            - Data types not included: complex, Arrow-style null, binary, decimal,
              and nested (list, struct, map, union) dtypes.
        """
        pass

    def _dtype_from_arrowdtype(self, dtype) -> tuple[DtypeKind, int, str, str]:
        """
        See `self.dtype` for details.
        """
        # Note: 'c' (complex) not handled yet (not in array spec v1).
        #       'b', 'B' (bytes), 'S', 'a', (old-style string) 'V' (void) not handled
        #       datetime and timedelta both map to datetime (is timedelta handled?)

        pass

    @property
    def describe_categorical(self):
        """
        If the dtype is categorical, there are two options:
        - There are only values in the data buffer.
        - There is a separate non-categorical Column encoding for categorical values.
        Raises TypeError if the dtype is not categorical
        Content of returned dict:
            - "is_ordered" : bool, whether the ordering of dictionary indices is
                             semantically meaningful.
            - "is_dictionary" : bool, whether a dictionary-style mapping of
                                categorical values to other objects exists
            - "categories" : Column representing the (implicit) mapping of indices to
                             category values (e.g. an array of cat1, cat2, ...).
                             None if not a dictionary-style categorical.
        """
        pass

    @property
    def describe_null(self):
        pass

    @property
    def null_count(self) -> int:
        """
        Number of null elements. Should always be known.
        """
        pass

    @property
    def metadata(self) -> dict[str, pd.Index]:
        """
        Store specific metadata of the column.
        """
        pass

    def num_chunks(self) -> int:
        """
        Return the number of chunks the column consists of.
        """
        pass

    def get_chunks(self, n_chunks: int | None = None):
        """
        Return an iterator yielding the chunks.
        See `DataFrame.get_chunks` for details on ``n_chunks``.
        """
        pass

    def get_buffers(self) -> ColumnBuffers:
        """
        Return a dictionary containing the underlying buffers.
        The returned dictionary has the following contents:
            - "data": a two-element tuple whose first element is a buffer
                      containing the data and whose second element is the data
                      buffer's associated dtype.
            - "validity": a two-element tuple whose first element is a buffer
                          containing mask values indicating missing data and
                          whose second element is the mask value buffer's
                          associated dtype. None if the null representation is
                          not a bit or byte mask.
            - "offsets": a two-element tuple whose first element is a buffer
                         containing the offset values for variable-size binary
                         data (e.g., variable-length strings) and whose second
                         element is the offsets buffer's associated dtype. None
                         if the data buffer does not have an associated offsets
                         buffer.
        """
        pass

    def _get_data_buffer(
        self,
    ) -> tuple[PyArrowBuffer, Any]:  # Any is for self.dtype tuple
        """
        Return the buffer containing the data and the buffer's associated dtype.
        """
        pass

    def _get_validity_buffer(self) -> tuple[PyArrowBuffer, Any]:
        """
        Return the buffer containing the mask values indicating missing data and
        the buffer's associated dtype.
        Raises NoBufferPresent if null representation is not a bit or byte mask.
        """
        pass

    def _get_offsets_buffer(self) -> tuple[PyArrowBuffer, Any]:
        """
        Return the buffer containing the offset values for variable-size binary
        data (e.g., variable-length strings) and the buffer's associated dtype.
        Raises NoBufferPresent if the data buffer does not have an associated
        offsets buffer.
        """
        pass
