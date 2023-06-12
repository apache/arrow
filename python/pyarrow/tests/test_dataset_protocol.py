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
"""Test that PyArrow datasets conform to the protocol."""
import pyarrow.dataset.protocol as protocol
import pyarrow.dataset as ds


def test_dataset_protocol():
    assert isinstance(ds.Dataset, protocol.Dataset)
    assert isinstance(ds.Fragment, protocol.Fragment)

    assert isinstance(ds.Dataset, protocol.Scannable)
    assert isinstance(ds.Fragment, protocol.Scannable)

    assert isinstance(ds.Scanner, protocol.Scanner)
