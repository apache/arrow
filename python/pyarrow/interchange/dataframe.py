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

import chunk
from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
    Sequence,
    Tuple,
    TypedDict,
)

import pyarrow as pa
import warnings

from column import PandasColumn
from dataframe_protocol import DataFrame as DataFrameXchg


class PyArrowTableXchg(DataFrameXchg):
    """
    A data frame class, with only the methods required by the interchange
    protocol defined.
    Instances of this (private) class are returned from
    ``pd.DataFrame.__dataframe__`` as objects with the methods and
    attributes defined on this class.
    """

    def __init__(
        self, df: pa.Table, nan_as_null: bool = False, allow_copy: bool = True
    ) -> None:
        """
        Constructor - an instance of this (private) class is returned from
        `pd.DataFrame.__dataframe__`.
        """
        self._df = df
        # ``nan_as_null`` is a keyword intended for the consumer to tell the
        # producer to overwrite null values in the data with ``NaN`` (or ``NaT``).
        # This currently has no effect; once support for nullable extension
        # dtypes is added, this value should be propagated to columns.
        self._nan_as_null = nan_as_null
        self._allow_copy = allow_copy

    def __dataframe__(
        self, nan_as_null: bool = False, allow_copy: bool = True
    ) -> PyArrowTableXchg:
        return PyArrowTableXchg(self._df, nan_as_null, allow_copy)

    @property
    def metadata(self) -> dict[str, Any]:
        # The metadata for the data frame, as a dictionary with string keys.
        # Add schema metadata here (pandas metadata, ot custom metadata)
        schema_metadata = {k.decode('utf8'): v.decode('utf8') for k, v in self.schema.metadata.items()}
        return schema_metadata

    def num_columns(self) -> int:
        return self.num_columns

    def num_rows(self) -> int:
        return self.num_rows

    def num_chunks(self) -> int:
        return self.column(0).num_chunks

    def column_names(self) -> Iterable[str]:
        return self.column_names

    def get_column(self, i: int) -> PyArrowColumn:
        return self.column(i)

    def get_column_by_name(self, name: str) -> PyArrowColumn:
        return self.column(name)

    def get_columns(self) -> Iterable[PyArrowColumn]:
        return self.columns

    def select_columns(self, indices: Sequence[int]) -> PyArrowTableFrameXchg:
        return self.select(indices)

    def select_columns_by_name(self, names: Sequence[str]) -> PyArrowTableFrameXchg:
        return self.select(names)

    def get_chunks(self, n_chunks: Optional[int] = None) -> Iterable[PyArrowTableFrameXchg]:
        """
        Return an iterator yielding the chunks.
        """
        if n_chunks:
            if n_chunks%self.num_chunks == 0:
                chunk_size = self.num_rows//n_chunks
                if self.num_rows%n_chunks != 0:
                    warnings.warn("Converting dataframe into smaller chunks")
                batches = self.to_batches(max_chunksize = chunk_size)
            else:
                warnings.warn("``n_chunks`` must be a multiple of ``self.num_chunks()``")
        else:
            batches = self.to_batches()
        
        iterator_tables = [pa.Table.from_batches([batch]) for batch in batches]
        return iterator_tables
