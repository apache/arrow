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

import pyarrow as pa

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
    def metadata(self) -> dict[str, Index]:
        # `index` isn't a regular column, and the protocol doesn't support row
        # labels - so we export it as Pandas-specific metadata here.
        pass

    def num_columns(self) -> int:
        pass

    def num_rows(self) -> int:
        pass

    def num_chunks(self) -> int:
        pass

    def column_names(self) -> Index:
        pass

    def get_column(self, i: int) -> PyArrowColumn:
        pass

    def get_column_by_name(self, name: str) -> PyArrowColumn:
        pass

    def get_columns(self) -> list[PyArrowColumn]:
        pass

    def select_columns(self, indices) -> PyArrowTableFrameXchg:
        pass

    def select_columns_by_name(self, names) -> PyArrowTableFrameXchg:
        pass

    def get_chunks(self, n_chunks=None):
        """
        Return an iterator yielding the chunks.
        """
        pass