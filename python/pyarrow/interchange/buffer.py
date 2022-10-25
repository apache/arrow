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

from dataframe_protocol import (
    Buffer,
    DlpackDeviceType,
)
import numpy as np

class PyArrowBuffer(Buffer):
    """
    Data in the buffer is guaranteed to be contiguous in memory.
    """

    def __init__(self, x: np.ndarray, allow_copy: bool = True) -> None:
        """
        Handle only regular columns (= numpy arrays) for now.
        """
        pass

    @property
    def bufsize(self) -> int:
        """
        Buffer size in bytes.
        """
        pass

    @property
    def ptr(self) -> int:
        """
        Pointer to start of the buffer as an integer.
        """
        pass

    def __dlpack__(self):
        """
        Represent this structure as DLPack interface.
        """
        pass

    def __dlpack_device__(self) -> tuple[DlpackDeviceType, int | None]:
        """
        Device type and device ID for where the data in the buffer resides.
        """
        pass

    def __repr__(self) -> str:
        return (
            "PyArrowBuffer("
            + str(
                {
                    "bufsize": self.bufsize,
                    "ptr": self.ptr,
                    "device": self.__dlpack_device__()[0].name,
                }
            )
            + ")"
        )