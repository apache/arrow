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

import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import numpy as np

from pyarrow.lib import _Weakrefable
from scipy.sparse import coo_matrix, csr_matrix
from sparse import COO  # type: ignore[import-untyped, import-not-found]


class Tensor(_Weakrefable):

    @classmethod
    def from_numpy(cls, obj: np.ndarray,
                   dim_names: list[str] | None = None) -> Self: ...

    def to_numpy(self) -> np.ndarray: ...

    def equals(self, other: Tensor) -> bool: ...

    def dim_name(self, i: int) -> str: ...

    @property
    def dim_names(self) -> list[str]: ...

    @property
    def is_mutable(self) -> bool: ...

    @property
    def is_contiguous(self) -> bool: ...

    @property
    def ndim(self) -> int: ...

    @property
    def size(self) -> str: ...

    @property
    def shape(self) -> tuple[int, ...]: ...

    @property
    def strides(self) -> tuple[int, ...]: ...


class SparseCOOTensor(_Weakrefable):

    @classmethod
    def from_dense_numpy(cls, obj: np.ndarray,
                         dim_names: list[str] | None = None) -> Self: ...

    @classmethod
    def from_numpy(
        cls,
        data: np.ndarray,
        coords: np.ndarray,
        shape: tuple[int, ...],
        dim_names: list[str] | None = None,
    ) -> Self: ...

    @classmethod
    def from_scipy(cls, obj: csr_matrix,
                   dim_names: list[str] | None = None) -> Self: ...

    @classmethod
    def from_pydata_sparse(
        cls, obj: COO, dim_names: list[str] | None = None) -> Self: ...

    @classmethod
    def from_tensor(cls, obj: Tensor) -> Self: ...

    def to_numpy(self) -> tuple[np.ndarray, np.ndarray]: ...

    def to_scipy(self) -> coo_matrix: ...

    def to_pydata_sparse(self) -> COO: ...

    def to_tensor(self) -> Tensor: ...

    def equals(self, other: Self) -> bool: ...

    @property
    def is_mutable(self) -> bool: ...
    @property
    def ndim(self) -> int: ...
    @property
    def size(self) -> str: ...
    @property
    def shape(self) -> tuple[int, ...]: ...
    def dim_name(self, i: int) -> str: ...

    @property
    def dim_names(self) -> list[str]: ...
    @property
    def non_zero_length(self) -> int: ...
    @property
    def has_canonical_format(self) -> bool: ...


class SparseCSRMatrix(_Weakrefable):

    @classmethod
    def from_dense_numpy(cls, obj: np.ndarray,
                         dim_names: list[str] | None = None) -> Self: ...

    @classmethod
    def from_numpy(
        cls,
        data: np.ndarray,
        indptr: np.ndarray,
        indices: np.ndarray,
        shape: tuple[int, ...],
        dim_names: list[str] | None = None,
    ) -> Self: ...

    @classmethod
    def from_scipy(cls, obj: csr_matrix,
                   dim_names: list[str] | None = None) -> Self: ...

    @classmethod
    def from_tensor(cls, obj: Tensor) -> Self: ...

    def to_numpy(self) -> tuple[np.ndarray, np.ndarray, np.ndarray]: ...

    def to_scipy(self) -> csr_matrix: ...

    def to_tensor(self) -> Tensor: ...

    def equals(self, other: Self) -> bool: ...

    @property
    def is_mutable(self) -> bool: ...
    @property
    def ndim(self) -> int: ...
    @property
    def size(self) -> str: ...
    @property
    def shape(self) -> tuple[int, ...]: ...
    def dim_name(self, i: int) -> str: ...

    @property
    def dim_names(self) -> list[str]: ...
    @property
    def non_zero_length(self) -> int: ...


class SparseCSCMatrix(_Weakrefable):

    @classmethod
    def from_dense_numpy(cls, obj: np.ndarray,
                         dim_names: list[str] | None = None) -> Self: ...

    @classmethod
    def from_numpy(
        cls,
        data: np.ndarray,
        indptr: np.ndarray,
        indices: np.ndarray,
        shape: tuple[int, ...],
        dim_names: list[str] | None = None,
    ) -> Self: ...

    @classmethod
    def from_scipy(cls, obj: csr_matrix,
                   dim_names: list[str] | None = None) -> Self: ...

    @classmethod
    def from_tensor(cls, obj: Tensor) -> Self: ...

    def to_numpy(self) -> tuple[np.ndarray, np.ndarray, np.ndarray]: ...

    def to_scipy(self) -> csr_matrix: ...

    def to_tensor(self) -> Tensor: ...

    def equals(self, other: Self) -> bool: ...

    @property
    def is_mutable(self) -> bool: ...
    @property
    def ndim(self) -> int: ...
    @property
    def size(self) -> str: ...
    @property
    def shape(self) -> tuple[int, ...]: ...
    def dim_name(self, i: int) -> str: ...

    @property
    def dim_names(self) -> list[str]: ...
    @property
    def non_zero_length(self) -> int: ...


class SparseCSFTensor(_Weakrefable):

    @classmethod
    def from_dense_numpy(cls, obj: np.ndarray,
                         dim_names: list[str] | None = None) -> Self: ...

    @classmethod
    def from_numpy(
        cls,
        data: np.ndarray,
        indptr: np.ndarray,
        indices: np.ndarray,
        shape: tuple[int, ...],
        axis_order: list[int] | None = None,
        dim_names: list[str] | None = None,
    ) -> Self: ...

    @classmethod
    def from_tensor(cls, obj: Tensor) -> Self: ...

    def to_numpy(self) -> tuple[np.ndarray, np.ndarray, np.ndarray]: ...

    def to_tensor(self) -> Tensor: ...

    def equals(self, other: Self) -> bool: ...

    @property
    def is_mutable(self) -> bool: ...
    @property
    def ndim(self) -> int: ...
    @property
    def size(self) -> str: ...
    @property
    def shape(self) -> tuple[int, ...]: ...
    def dim_name(self, i: int) -> str: ...

    @property
    def dim_names(self) -> list[str]: ...
    @property
    def non_zero_length(self) -> int: ...


__all__ = [
    "Tensor",
    "SparseCOOTensor",
    "SparseCSRMatrix",
    "SparseCSCMatrix",
    "SparseCSFTensor",
]
