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

from __future__ import annotations
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

from pyarrow.interchange.column import PyArrowColumn
from pyarrow.interchange.dataframe_protocol import DataFrame as DataFrameXchg


class TableXchg(DataFrameXchg):
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
        `pa.Table.__dataframe__`.
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
    ) -> TableXchg:
        return TableXchg(self._df, nan_as_null, allow_copy)

    @property
    def metadata(self) -> dict[str, Any]:
        # The metadata for the data frame, as a dictionary with string keys.
        # Add schema metadata here (pandas metadata, ot custom metadata)
        schema_metadata = {k.decode('utf8'): v.decode('utf8')
                           for k, v in self._df.schema.metadata.items()}
        return schema_metadata

    def num_columns(self) -> int:
        return self._df.num_columns

    def num_rows(self) -> int:
        return self._df.num_rows

    def num_chunks(self) -> int:
        return self._df.column(0).num_chunks

    def column_names(self) -> Iterable[str]:
        return self._df.column_names

    def get_column(self, i: int) -> PyArrowColumn:
        return PyArrowColumn(self._df.column(i),
                             allow_copy=self._allow_copy)

    def get_column_by_name(self, name: str) -> PyArrowColumn:
        return PyArrowColumn(self._df.column(name),
                             allow_copy=self._allow_copy)

    def get_columns(self) -> Iterable[PyArrowColumn]:
        return [
            PyArrowColumn(col, allow_copy=self._allow_copy)
            for col in self._df.columns
        ]

    def select_columns(self, indices: Sequence[int]) -> TableXchg:
        return TableXchg(
            self._df.select(list(indices)), self._nan_as_null, self._allow_copy
        )

    def select_columns_by_name(self, names: Sequence[str]) -> TableXchg:
        return TableXchg(
            self._df.select(list(names)), self._nan_as_null, self._allow_copy
        )

    def get_chunks(self, n_chunks: Optional[int] = None) -> Iterable[TableXchg]:
        """
        Return an iterator yielding the chunks.
        """
        if n_chunks and n_chunks > 1:
            if n_chunks % self.num_chunks() == 0:
                chunk_size = self.num_rows() // n_chunks
                if self.num_rows() % n_chunks != 0:
                    chunk_size += 1
                batches = self._df.to_batches(max_chunksize=chunk_size)
                # In case when the size of the chunk is such that the resulting
                # list is one less chunk then n_chunks -> append an empty chunk
                if len(batches) == n_chunks - 1:
                    batches.append(pa.record_batch([]))
            else:
                warnings.warn(
                    "``n_chunks`` must be a multiple of ``self.num_chunks()``")
        else:
            batches = self._df.to_batches()

        iterator_tables = [TableXchg(
                pa.Table.from_batches([batch]), self._nan_as_null, self._allow_copy
            )
            for batch in batches
        ]
        return iterator_tables
