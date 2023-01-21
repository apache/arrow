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

#include <pybind11/pybind11.h>

#include "cpc_sketch.hpp"
#include "cpc_union.hpp"
#include "cpc_common.hpp"
#include "common_defs.hpp"

namespace py = pybind11;

void init_cpc(py::module &m) {
  using namespace datasketches;

  py::class_<cpc_sketch>(m, "cpc_sketch")
    .def(py::init<uint8_t, uint64_t>(), py::arg("lg_k")=cpc_constants::DEFAULT_LG_K, py::arg("seed")=DEFAULT_SEED)
    .def(py::init<const cpc_sketch&>())
    .def("__str__", &cpc_sketch::to_string,
         "Produces a string summary of the sketch")
    .def("to_string", &cpc_sketch::to_string,
         "Produces a string summary of the sketch")
    .def<void (cpc_sketch::*)(uint64_t)>("update", &cpc_sketch::update, py::arg("datum"),
         "Updates the sketch with the given 64-bit integer value")
    .def<void (cpc_sketch::*)(double)>("update", &cpc_sketch::update, py::arg("datum"),
         "Updates the sketch with the given 64-bit floating point")
    .def<void (cpc_sketch::*)(const std::string&)>("update", &cpc_sketch::update, py::arg("datum"),
         "Updates the sketch with the given string")
    .def("is_empty", &cpc_sketch::is_empty,
         "Returns True if the sketch is empty, otherwise Dalse")
    .def("get_estimate", &cpc_sketch::get_estimate,
         "Estimate of the distinct count of the input stream")
    .def("get_lower_bound", &cpc_sketch::get_lower_bound, py::arg("kappa"),
         "Returns an approximate lower bound on the estimate for kappa values in {1, 2, 3}, roughly corresponding to standard deviations")
    .def("get_upper_bound", &cpc_sketch::get_upper_bound, py::arg("kappa"),
         "Returns an approximate upper bound on the estimate for kappa values in {1, 2, 3}, roughly corresponding to standard deviations")
    .def(
        "serialize",
        [](const cpc_sketch& sk) {
          auto bytes = sk.serialize();
          return py::bytes(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        },
        "Serializes the sketch into a bytes object"
    )
    .def_static(
        "deserialize",
        [](const std::string& bytes) { return cpc_sketch::deserialize(bytes.data(), bytes.size()); },
        py::arg("bytes"),
        "Reads a bytes object and returns the corresponding cpc_sketch"
    );

  py::class_<cpc_union>(m, "cpc_union")
    .def(py::init<uint8_t, uint64_t>(), py::arg("lg_k"), py::arg("seed")=DEFAULT_SEED)
    .def(py::init<const cpc_union&>())
    .def("update", (void (cpc_union::*)(const cpc_sketch&)) &cpc_union::update, py::arg("sketch"),
         "Updates the union with the provided CPC sketch")
    .def("get_result", &cpc_union::get_result,
         "Returns a CPC sketch with the result of the union")
    ;
}
