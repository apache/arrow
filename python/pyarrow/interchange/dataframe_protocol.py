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

"""
Copy of the spec from https://github.com/data-apis/dataframe-api
"""

from abc import (
    ABC,
    abstractmethod,
)
import enum
from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
    Sequence,
    Tuple,
)

import sys

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict


class DlpackDeviceType(enum.IntEnum):
    """Integer enum for device type codes matching DLPack."""

    CPU = 1
    CUDA = 2
    CPU_PINNED = 3
    OPENCL = 4
    VULKAN = 7
    METAL = 8
    VPI = 9
    ROCM = 10


class DtypeKind(enum.IntEnum):
    """
    Integer enum for data types.
    Attributes
    ----------
    INT : int
        Matches to signed integer data type.
    UINT : int
        Matches to unsigned integer data type.
    FLOAT : int
        Matches to floating point data type.
    BOOL : int
        Matches to boolean data type.
    STRING : int
        Matches to string data type (UTF-8 encoded).
    DATETIME : int
        Matches to datetime data type.
    CATEGORICAL : int
        Matches to categorical data type.
    """

    INT = 0
    UINT = 1
    FLOAT = 2
    BOOL = 20
    STRING = 21  # UTF-8
    DATETIME = 22
    CATEGORICAL = 23


Dtype = Tuple[DtypeKind, int, str, str]  # see Column.dtype


class ColumnNullType(enum.IntEnum):
    """
    Integer enum for null type representation.
    Attributes
    ----------
    NON_NULLABLE : int
        Non-nullable column.
    USE_NAN : int
        Use explicit float NaN value.
    USE_SENTINEL : int
        Sentinel value besides NaN.
    USE_BITMASK : int
        The bit is set/unset representing a null on a certain position.
    USE_BYTEMASK : int
        The byte is set/unset representing a null on a certain position.
    """

    NON_NULLABLE = 0
    USE_NAN = 1
    USE_SENTINEL = 2
    USE_BITMASK = 3
    USE_BYTEMASK = 4


class ColumnBuffers(TypedDict):
    # first element is a buffer containing the column data;
    # second element is the data buffer's associated dtype
    data: Tuple["Buffer", Dtype]

    # first element is a buffer containing mask values indicating missing data;
    # second element is the mask value buffer's associated dtype.
    # None if the null representation is not a bit or byte mask
    validity: Optional[Tuple["Buffer", Dtype]]

    # first element is a buffer containing the offset values for
    # variable-size binary data (e.g., variable-length strings);
    # second element is the offsets buffer's associated dtype.
    # None if the data buffer does not have an associated offsets buffer
    offsets: Optional[Tuple["Buffer", Dtype]]


class CategoricalDescription(TypedDict):
    # whether the ordering of dictionary indices is semantically meaningful
    is_ordered: bool
    # whether a dictionary-style mapping of categorical values to other objects
    # exists
    is_dictionary: bool
    # Python-level only (e.g. ``{int: str}``).
    # None if not a dictionary-style categorical.
    # categories: Optional[Column]


class Buffer(ABC):
    """
    Data in the buffer is guaranteed to be contiguous in memory.
    Note that there is no dtype attribute present, a buffer can be thought of
    as simply a block of memory. However, if the column that the buffer is
    attached to has a dtype that's supported by DLPack and ``__dlpack__`` is
    implemented, then that dtype information will be contained in the return
    value from ``__dlpack__``.
    This distinction is useful to support both data exchange via DLPack on a
    buffer and (b) dtypes like variable-length strings which do not have a
    fixed number of bytes per element.
    """

    @property
    @abstractmethod
    def bufsize(self) -> int:
        """
        Buffer size in bytes.
        """
        pass

    @property
    @abstractmethod
    def ptr(self) -> int:
        """
        Pointer to start of the buffer as an integer.
        """
        pass

    @abstractmethod
    def __dlpack__(self):
        """
        Produce DLPack capsule (see array API standard).
        Raises:
            - TypeError : if the buffer contains unsupported dtypes.
            - NotImplementedError : if DLPack support is not implemented
        Useful to have to connect to array libraries. Support optional because
        it's not completely trivial to implement for a Python-only library.
        """
        raise NotImplementedError("__dlpack__")

    @abstractmethod
    def __dlpack_device__(self) -> Tuple[DlpackDeviceType, Optional[int]]:
        """
        Device type and device ID for where the data in the buffer resides.
        Uses device type codes matching DLPack.
        Note: must be implemented even if ``__dlpack__`` is not.
        """
        pass


class Column(ABC):
    """
    A column object, with only the methods and properties required by the
    interchange protocol defined.
    A column can contain one or more chunks. Each chunk can contain up to three
    buffers - a data buffer, a mask buffer (depending on null representation),
    and an offsets buffer (if variable-size binary; e.g., variable-length
    strings).
    TBD: Arrow has a separate "null" dtype, and has no separate mask concept.
         Instead, it seems to use "children" for both columns with a bit mask,
         and for nested dtypes. Unclear whether this is elegant or confusing.
         This design requires checking the null representation explicitly.
         The Arrow design requires checking:
         1. the ARROW_FLAG_NULLABLE (for sentinel values)
         2. if a column has two children, combined with one of those children
            having a null dtype.
         Making the mask concept explicit seems useful. One null dtype would
         not be enough to cover both bit and byte masks, so that would mean
         even more checking if we did it the Arrow way.
    TBD: there's also the "chunk" concept here, which is implicit in Arrow as
         multiple buffers per array (= column here). Semantically it may make
         sense to have both: chunks were meant for example for lazy evaluation
         of data which doesn't fit in memory, while multiple buffers per column
         could also come from doing a selection operation on a single
         contiguous buffer.
         Given these concepts, one would expect chunks to be all of the same
         size (say a 10,000 row dataframe could have 10 chunks of 1,000 rows),
         while multiple buffers could have data-dependent lengths. Not an issue
         in pandas if one column is backed by a single NumPy array, but in
         Arrow it seems possible.
         Are multiple chunks *and* multiple buffers per column necessary for
         the purposes of this interchange protocol, or must producers either
         reuse the chunk concept for this or copy the data?
    Note: this Column object can only be produced by ``__dataframe__``, so
          doesn't need its own version or ``__column__`` protocol.
    """

    @abstractmethod
    def size(self) -> int:
        """
        Size of the column, in elements.
        Corresponds to DataFrame.num_rows() if column is a single chunk;
        equal to size of this current chunk otherwise.
        Is a method rather than a property because it may cause a (potentially
        expensive) computation for some dataframe implementations.
        """
        pass

    @property
    @abstractmethod
    def offset(self) -> int:
        """
        Offset of first element.
        May be > 0 if using chunks; for example for a column with N chunks of
        equal size M (only the last chunk may be shorter),
        ``offset = n * M``, ``n = 0 .. N-1``.
        """
        pass

    @property
    @abstractmethod
    def dtype(self) -> Dtype:
        """
        Dtype description as a tuple ``(kind, bit-width, format string,
        endianness)``.
        Bit-width : the number of bits as an integer
        Format string : data type description format string in Apache Arrow C
                        Data Interface format.
        Endianness : current only native endianness (``=``) is supported
        Notes:
            - Kind specifiers are aligned with DLPack where possible (hence the
              jump to 20, leave enough room for future extension)
            - Masks must be specified as boolean with either bit width 1 (for
              bit masks) or 8 (for byte masks).
            - Dtype width in bits was preferred over bytes
            - Endianness isn't too useful, but included now in case in the
              future we need to support non-native endianness
            - Went with Apache Arrow format strings over NumPy format strings
              because they're more complete from a dataframe perspective
            - Format strings are mostly useful for datetime specification, and
              for categoricals.
            - For categoricals, the format string describes the type of the
              categorical in the data buffer. In case of a separate encoding of
              the categorical (e.g. an integer to string mapping), this can
              be derived from ``self.describe_categorical``.
            - Data types not included: complex, Arrow-style null, binary,
              decimal, and nested (list, struct, map, union) dtypes.
        """
        pass

    @property
    @abstractmethod
    def describe_categorical(self) -> CategoricalDescription:
        """
        If the dtype is categorical, there are two options:
        - There are only values in the data buffer.
        - There is a separate non-categorical Column encoding categorical
          values.
        Raises TypeError if the dtype is not categorical
        Returns the dictionary with description on how to interpret the
        data buffer:
            - "is_ordered" : bool, whether the ordering of dictionary indices
                             is semantically meaningful.
            - "is_dictionary" : bool, whether a mapping of
                                categorical values to other objects exists
            - "categories" : Column representing the (implicit) mapping of
                             indices to category values (e.g. an array of
                             cat1, cat2, ...). None if not a dictionary-style
                             categorical.
        TBD: are there any other in-memory representations that are needed?
        """
        pass

    @property
    @abstractmethod
    def describe_null(self) -> Tuple[ColumnNullType, Any]:
        """
        Return the missing value (or "null") representation the column dtype
        uses, as a tuple ``(kind, value)``.
        Value : if kind is "sentinel value", the actual value. If kind is a bit
        mask or a byte mask, the value (0 or 1) indicating a missing value.
        None otherwise.
        """
        pass

    @property
    @abstractmethod
    def null_count(self) -> Optional[int]:
        """
        Number of null elements, if known.
        Note: Arrow uses -1 to indicate "unknown", but None seems cleaner.
        """
        pass

    @property
    @abstractmethod
    def metadata(self) -> Dict[str, Any]:
        """
        The metadata for the column. See `DataFrame.metadata` for more details.
        """
        pass

    @abstractmethod
    def num_chunks(self) -> int:
        """
        Return the number of chunks the column consists of.
        """
        pass

    @abstractmethod
    def get_chunks(self, n_chunks: Optional[int] = None) -> Iterable["Column"]:
        """
        Return an iterator yielding the chunks.
        See `DataFrame.get_chunks` for details on ``n_chunks``.
        """
        pass

    @abstractmethod
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


#    def get_children(self) -> Iterable[Column]:
#        """
#        Children columns underneath the column, each object in this iterator
#        must adhere to the column specification.
#        """
#        pass


class DataFrame(ABC):
    """
    A data frame class, with only the methods required by the interchange
    protocol defined.
    A "data frame" represents an ordered collection of named columns.
    A column's "name" must be a unique string.
    Columns may be accessed by name or by position.
    This could be a public data frame class, or an object with the methods and
    attributes defined on this DataFrame class could be returned from the
    ``__dataframe__`` method of a public data frame class in a library adhering
    to the dataframe interchange protocol specification.
    """

    version = 0  # version of the protocol

    @abstractmethod
    def __dataframe__(
        self, nan_as_null: bool = False, allow_copy: bool = True
    ) -> "DataFrame":
        """
        Construct a new exchange object, potentially changing the parameters.
        ``nan_as_null`` is a keyword intended for the consumer to tell the
        producer to overwrite null values in the data with ``NaN``.
        It is intended for cases where the consumer does not support the bit
        mask or byte mask that is the producer's native representation.
        ``allow_copy`` is a keyword that defines whether or not the library is
        allowed to make a copy of the data. For example, copying data would be
        necessary if a library supports strided buffers, given that this
        protocol specifies contiguous buffers.
        """
        pass

    @property
    @abstractmethod
    def metadata(self) -> Dict[str, Any]:
        """
        The metadata for the data frame, as a dictionary with string keys. The
        contents of `metadata` may be anything, they are meant for a library
        to store information that it needs to, e.g., roundtrip losslessly or
        for two implementations to share data that is not (yet) part of the
        interchange protocol specification. For avoiding collisions with other
        entries, please add name the keys with the name of the library
        followed by a period and the desired name, e.g, ``pandas.indexcol``.
        """
        pass

    @abstractmethod
    def num_columns(self) -> int:
        """
        Return the number of columns in the DataFrame.
        """
        pass

    @abstractmethod
    def num_rows(self) -> Optional[int]:
        # TODO: not happy with Optional, but need to flag it may be expensive
        #       why include it if it may be None - what do we expect consumers
        #       to do here?
        """
        Return the number of rows in the DataFrame, if available.
        """
        pass

    @abstractmethod
    def num_chunks(self) -> int:
        """
        Return the number of chunks the DataFrame consists of.
        """
        pass

    @abstractmethod
    def column_names(self) -> Iterable[str]:
        """
        Return an iterator yielding the column names.
        """
        pass

    @abstractmethod
    def get_column(self, i: int) -> Column:
        """
        Return the column at the indicated position.
        """
        pass

    @abstractmethod
    def get_column_by_name(self, name: str) -> Column:
        """
        Return the column whose name is the indicated name.
        """
        pass

    @abstractmethod
    def get_columns(self) -> Iterable[Column]:
        """
        Return an iterator yielding the columns.
        """
        pass

    @abstractmethod
    def select_columns(self, indices: Sequence[int]) -> "DataFrame":
        """
        Create a new DataFrame by selecting a subset of columns by index.
        """
        pass

    @abstractmethod
    def select_columns_by_name(self, names: Sequence[str]) -> "DataFrame":
        """
        Create a new DataFrame by selecting a subset of columns by name.
        """
        pass

    @abstractmethod
    def get_chunks(
        self, n_chunks: Optional[int] = None
    ) -> Iterable["DataFrame"]:
        """
        Return an iterator yielding the chunks.
        By default (None), yields the chunks that the data is stored as by the
        producer. If given, ``n_chunks`` must be a multiple of
        ``self.num_chunks()``, meaning the producer must subdivide each chunk
        before yielding it.
        Note that the producer must ensure that all columns are chunked the
        same way.
        """
        pass
