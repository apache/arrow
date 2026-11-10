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
# mypy: warn-unused-ignores

from collections.abc import Hashable
from typing import TYPE_CHECKING

import numpy as np
import pytest

from typing_extensions import CapsuleType

import pyarrow as pa

from _check import TYPE_CHECKING_INVALID_USAGE, assert_type, check

if TYPE_CHECKING:
    from scipy.sparse import coo_array, csc_array, csr_array  # noqa: F401

DENSE = np.array([[1, 0, 2], [0, 3, 0]])


def test_tensor() -> None:
    tensor = pa.Tensor.from_numpy(DENSE, dim_names=("x", "y"))
    check(assert_type(tensor, pa.Tensor), pa.Tensor)
    check(assert_type(tensor.dim_names, list[str]), list)

    from_dlpack = pa.Tensor.from_dlpack(DENSE)
    check(assert_type(from_dlpack, pa.Tensor), pa.Tensor)
    check(assert_type(tensor.__dlpack__(max_version=(1, 0)), CapsuleType),
          CapsuleType)
    check(assert_type(tensor.__dlpack_device__(), tuple[int, int]), tuple)
    # __dlpack_device__() output must be accepted back as `device`
    round_trip = pa.Tensor.from_dlpack(
        tensor, device=tensor.__dlpack_device__())
    check(assert_type(round_trip, pa.Tensor), pa.Tensor)


def test_sparse_from_numpy() -> None:
    dim_names = ("x", "y")
    coo = pa.SparseCOOTensor.from_numpy(
        np.array([[1, 2, 3]]).T, np.array([[0, 0], [0, 2], [1, 1]]),
        (2, 3), dim_names)
    check(assert_type(coo, pa.SparseCOOTensor), pa.SparseCOOTensor)
    check(assert_type(coo.dim_names, tuple[str, ...]), tuple)

    coo = pa.SparseCOOTensor.from_numpy(
        np.array([[1, 2, 3]]).T, np.array([[0, 0], [0, 2], [1, 1]]),
        [2, 3], dim_names)
    check(assert_type(coo, pa.SparseCOOTensor), pa.SparseCOOTensor)

    csr = pa.SparseCSRMatrix.from_numpy(
        np.array([[1, 2, 3]]).T, np.array([0, 2, 3]), np.array([0, 2, 1]),
        (2, 3), dim_names)
    check(assert_type(csr, pa.SparseCSRMatrix), pa.SparseCSRMatrix)
    check(assert_type(csr.dim_names, tuple[str, ...]), tuple)

    csc = pa.SparseCSCMatrix.from_numpy(
        np.array([[1, 3, 2]]).T, np.array([0, 1, 2, 3]),
        np.array([0, 1, 0]), (2, 3), dim_names)
    check(assert_type(csc, pa.SparseCSCMatrix), pa.SparseCSCMatrix)
    check(assert_type(csc.dim_names, tuple[str, ...]), tuple)

    csf = pa.SparseCSFTensor.from_numpy(
        np.array([[1, 2, 3]]).T, [np.array([0, 2, 3])],
        [np.array([0, 1]), np.array([0, 2, 1])], (2, 3),
        axis_order=(0, 1), dim_names=dim_names)
    check(assert_type(csf, pa.SparseCSFTensor), pa.SparseCSFTensor)
    check(assert_type(csf.dim_names, tuple[str, ...]), tuple)

    csf = pa.SparseCSFTensor.from_numpy(
        np.array([[1, 2, 3]]).T, (np.array([0, 2, 3]),),
        (np.array([0, 1]), np.array([0, 2, 1])), (2, 3))
    check(assert_type(csf, pa.SparseCSFTensor), pa.SparseCSFTensor)


def test_sparse_scipy() -> None:
    sparse = pytest.importorskip("scipy.sparse")

    coo = pa.SparseCOOTensor.from_scipy(sparse.coo_array(DENSE))
    check(assert_type(coo.to_scipy(), "coo_array"), sparse.coo_array)

    csr = pa.SparseCSRMatrix.from_scipy(sparse.csr_array(DENSE))
    check(assert_type(csr.to_scipy(), "csr_array"), sparse.csr_array)

    csc = pa.SparseCSCMatrix.from_scipy(sparse.csc_array(DENSE))
    check(assert_type(csc.to_scipy(), "csc_array"), sparse.csc_array)


def test_invalid_usage() -> None:
    tensor = pa.Tensor.from_numpy(DENSE)
    if TYPE_CHECKING_INVALID_USAGE:
        pa.Tensor.from_numpy(DENSE, dim_names=[1, 2])  # type: ignore[list-item]
        _: Hashable = tensor  # type: ignore[assignment]  # ty: ignore[invalid-assignment]  # noqa: E501
