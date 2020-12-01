# -*- coding: utf-8 -*-
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

import unittest

import pyarrow
import arrow_pyarrow_integration_testing


class TestCase(unittest.TestCase):
    def test_primitive_python(self):
        """
        Python -> Rust -> Python
        """
        old_allocated = pyarrow.total_allocated_bytes()
        a = pyarrow.array([1, 2, 3])
        b = arrow_pyarrow_integration_testing.double(a)
        self.assertEqual(b, pyarrow.array([2, 4, 6]))
        del a
        del b
        # No leak of C++ memory
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())

    def test_primitive_rust(self):
        """
        Rust -> Python -> Rust
        """
        old_allocated = pyarrow.total_allocated_bytes()

        def double(array):
            array = array.to_pylist()
            return pyarrow.array([x * 2 if x is not None else None for x in array])

        is_correct = arrow_pyarrow_integration_testing.double_py(double)
        self.assertTrue(is_correct)
        # No leak of C++ memory
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())

    def test_string_python(self):
        """
        Python -> Rust -> Python
        """
        old_allocated = pyarrow.total_allocated_bytes()
        a = pyarrow.array(["a", None, "ccc"])
        b = arrow_pyarrow_integration_testing.substring(a, 1)
        self.assertEqual(b, pyarrow.array(["", None, "cc"]))
        del a
        del b
        # No leak of C++ memory
        self.assertEqual(old_allocated, pyarrow.total_allocated_bytes())
