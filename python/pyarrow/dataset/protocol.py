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
"""Protocol definitions for pyarrow.dataset

These provide the abstract interface for a dataset. Other libraries may implement
this interface to expose their data, without having to extend PyArrow's classes.

Applications and libraries that want to consume datasets should accept datasets
that implement these protocols, rather than requiring the specific
PyArrow classes.

See Extending PyArrow Datasets for more information:

https://arrow.apache.org/docs/python/integration/dataset.html
"""
from abc import abstractmethod, abstractproperty
from typing import Iterator, List, Optional

# TODO: remove once we drop support for Python 3.7
if sys.version_info >= (3, 8):
    from typing import Protocol, runtime_checkable
else:
    from typing_extensions import Protocol, runtime_checkable

from pyarrow.dataset import Expression
from pyarrow import Table, RecordBatchReader, Schema


@runtime_checkable
class Scanner(Protocol):
    """
    A scanner implementation for a dataset.

    This may be a scan of a whole dataset, or a scan of a single fragment.
    """
    @abstractmethod
    def count_rows(self) -> int:
        """
        Count the number of rows in this dataset.

        Implementors may provide optimized code paths that compute this from metadata.

        Returns
        -------
        int
            The number of rows in the dataset.
        """
        ...

    @abstractmethod
    def head(self, num_rows: int) -> Table:
        """
        Get the first ``num_rows`` rows of the dataset.

        Parameters
        ----------
        num_rows : int
            The number of rows to return.

        Returns
        -------
        Table
            A table containing the first ``num_rows`` rows of the dataset.
        """
        ...

    @abstractmethod
    def to_reader(self) -> RecordBatchReader:
        """
        Create a Record Batch Reader for this scan.

        This is used to read the data in chunks.

        Returns
        -------
        RecordBatchReader
        """
        ...


@runtime_checkable
class Scannable(Protocol):
    @abstractmethod
    def scanner(self, columns: Optional[List[str]] = None,
                filter: Optional[Expression] = None, batch_size: Optional[int] = None,
                use_threads: bool = True,
                **kwargs) -> Scanner:
        """Create a scanner for this dataset.

        Parameters
        ----------
        columns : List[str], optional
            Names of columns to include in the scan. If None, all columns are
            included.
        filter : Expression, optional
            Filter expression to apply to the scan. If None, no filter is applied.
        batch_size : int, optional
            The number of rows to include in each batch. If None, the default
            value is used. The default value is implementation specific.
        use_threads : bool, default True
            Whether to use multiple threads to read the rows. It is expected
            that consumers reading a whole dataset in one scanner will keep this
            as True, while consumers reading a single fragment per worker will
            typically set this to False.

        Notes
        -----
        The filters must be fully satisfied. If the dataset cannot satisfy the
        filter, it should raise an error.

        Only the following expressions are allowed in the filter:
        - Equality / inequalities (==, !=, <, >, <=, >=)
        - Conjunctions (and, or)
        - Field references (e.g. "a" or "a.b.c")
        - Literals (e.g. 1, 1.0, "a", True)
        - cast
        - is_null / not_null
        - isin
        - between
        - negation (not)

        """
        ...


@runtime_checkable
class Fragment(Scannable, Protocol):
    """A fragment of a dataset.

    This might be a partition, a file, a file chunk, etc.

    This class should be pickleable so that it can be used in a distributed scan."""
    ...


@runtime_checkable
class Dataset(Scannable, Protocol):
    @abstractmethod
    def get_fragments(
        self,
        filter: Optional[Expression] = None, **kwargs
    ) -> Iterator[Fragment]:
        """Get the fragments of this dataset.

        Parameters
        ----------
        filter : Expression, optional
            Filter expression to use to prune which fragments are selected.
            See Scannable.scanner for details on allowed filters. The filter is
            just used to prune which fragments are selected. It does not need to
            save the filter to apply to the scan. That is handled by the scanner.
        **kwargs : dict
            Additional arguments to pass to underlying implementation.
        """
        ...

    @abstractproperty
    def schema(self) -> Schema:
        """
        Get the schema of this dataset.

        Returns
        -------
        Schema
        """
        ...
