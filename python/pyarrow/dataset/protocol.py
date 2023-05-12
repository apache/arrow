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
"""
from abc import abstractmethod
from typing import Iterator, List, Optional, Protocol

from pyarrow.dataset import Expression
from pyarrow import Table, IntegerArray, RecordBatch, RecordBatchReader, Schema


class Scanner(Protocol):
    @abstractmethod
    def count_rows(self) -> int:
        ...
    
    @abstractmethod
    def head(self, num_rows: int) -> Table:
        ...

    @abstractmethod
    def take(self, indices: IntegerArray) -> Table:
        ...
    
    @abstractmethod
    def to_table(self) -> Table:
        ...
    
    @abstractmethod
    def to_batches(self) -> Iterator[RecordBatch]:
        ...

    @abstractmethod
    def to_reader(self) -> RecordBatchReader:
        ...


class Scannable(Protocol):
    @abstractmethod
    def scanner(self, columns: Optional[List[str]] = None,
                filter: Optional[Expression] = None, **kwargs) -> Scanner:
        ...
    
    @abstractmethod
    def schema(self) -> Schema:
        ...


class Fragment(Scannable):
    ...


class Dataset(Scannable):
    @abstractmethod
    def get_fragments(self, filter: Optional[Expression] = None) -> Iterator[Fragment]:
        ...
