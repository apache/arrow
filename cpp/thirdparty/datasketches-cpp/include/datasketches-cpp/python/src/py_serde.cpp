/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <cstring>
#include "memory_operations.hpp"

#include "py_serde.hpp"

#include <pybind11/pybind11.h>

namespace py = pybind11;

void init_serde(py::module& m) {
  py::class_<datasketches::py_object_serde, datasketches::PyObjectSerDe /* <--- trampoline*/>(m, "PyObjectSerDe")
    .def(py::init<>())
    .def("get_size", &datasketches::py_object_serde::get_size, py::arg("item"),
        "Returns the size in bytes of an item")
    .def("to_bytes", &datasketches::py_object_serde::to_bytes, py::arg("item"),
        "Retuns a bytes object with a serialized version of an item")
    .def("from_bytes", &datasketches::py_object_serde::from_bytes, py::arg("data"), py::arg("offset"),
        "Reads a bytes object starting from the given offest and returns a tuple of the reconstructed "
        "object and the number of additional bytes read")
    ;
}    

namespace datasketches {
  size_t py_object_serde::size_of_item(const py::object& item) const {
    return get_size(item);
  }

  size_t py_object_serde::serialize(void* ptr, size_t capacity, const py::object* items, unsigned num) const {
    size_t bytes_written = 0;
    py::gil_scoped_acquire acquire;
    for (unsigned i = 0; i < num; ++i) {
      std::string bytes = to_bytes(items[i]); // implicit cast from py::bytes
      check_memory_size(bytes_written + bytes.size(), capacity);
      memcpy(ptr, bytes.c_str(), bytes.size());
      ptr = static_cast<char*>(ptr) + bytes.size();
      bytes_written += bytes.size();
    }
    py::gil_scoped_release release;
    return bytes_written;
  }

  size_t py_object_serde::deserialize(const void* ptr, size_t capacity, py::object* items, unsigned num) const {
    size_t bytes_read = 0;
    unsigned i = 0;
    bool failure = false;
    bool error_from_python = false;
    py::gil_scoped_acquire acquire;

    // copy data into bytes only once
    py::bytes bytes(static_cast<const char*>(ptr), capacity);
    for (; i < num && !failure; ++i) {
      py::tuple bytes_and_len;
      try {
        bytes_and_len = from_bytes(bytes, bytes_read);
      } catch (py::error_already_set &e) {
        failure = true;
        error_from_python = true;
        break;
      }

      size_t length = py::cast<size_t>(bytes_and_len[1]);
      if (bytes_read + length > capacity) {
        bytes_read += length; // use this value to report the error
        failure = true;
        break;
      }
      
      new (&items[i]) py::object(py::cast<py::object>(bytes_and_len[0]));
      ptr = static_cast<const char*>(ptr) + length;
      bytes_read += length;
    }

    if (failure) {
      // clean up what we've allocated
      for (unsigned j = 0; j < i; ++j) {
        items[j].dec_ref();
      }

      if (error_from_python) {
        throw py::value_error("Error reading value in from_bytes");
      } else {
        // this next call will throw
        check_memory_size(bytes_read, capacity);
      }
    }

    py::gil_scoped_release release;
    return bytes_read;
  }


} // namespace datasketches