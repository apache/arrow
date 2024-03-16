// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "arrow/pretty_print.h"

#include <sstream>

namespace arrow::matlab::tabular {

    namespace {
        arrow::PrettyPrintOptions make_pretty_print_options() {
            auto opts = arrow::PrettyPrintOptions::Defaults();
            opts.skip_new_lines = true;
            opts.array_delimiters.open = "";
            opts.array_delimiters.close = "";
            opts.chunked_array_delimiters.open = "";
            opts.chunked_array_delimiters.close = "";
            return opts;
        }
    }

    template <typename TabularType>
    arrow::Result<std::string> get_row_as_string(const std::shared_ptr<TabularType>& tabular_object, const int64_t matlab_row_index) {
        std::stringstream ss;
        const int64_t row_index = matlab_row_index - 1;
        if (row_index >= tabular_object->num_rows() || row_index < 0) {
            ss << "Invalid Row Index: " << matlab_row_index;
            return arrow::Status::Invalid(ss.str());
        }

        const auto opts = make_pretty_print_options();
        const auto num_columns = tabular_object->num_columns();
        const auto& columns = tabular_object->columns();

        for (int32_t i = 0; i < num_columns; ++i) {
            const auto& column = columns[i];
            const auto type_id = column->type()->id();
            if (arrow::is_primitive(type_id) || arrow::is_string(type_id)) {
                auto slice = column->Slice(row_index, 1);
                ARROW_RETURN_NOT_OK(arrow::PrettyPrint(*slice, opts, &ss));
            } else if (type_id == arrow::Type::type::STRUCT) {
                // Use <Struct> as a placeholder since we don't have a good
                // way to display StructArray elements horizontally on screen.
                ss << "<Struct>";
            } else if (type_id == arrow::Type::type::LIST) {
                // Use <List> as a placeholder since we don't have a good
                // way to display ListArray elements horizontally on screen.
                ss << "<List>";
            } else {
                return arrow::Status::NotImplemented("Datatype " + column->type()->ToString() + "is not currently supported for display.");
            }

            if (i + 1 < num_columns) {
                // Only add the delimiter if there is at least 
                // one more element to print. 
                ss << " | ";
            }
        }
        return ss.str();
    }
}