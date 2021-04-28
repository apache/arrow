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

namespace gandiva {
    /// Function Holder for SQL 'to_char'

    template<typename T>
    class GANDIVA_EXPORT ToCharHolder : public FunctionHolder {

    public:
        ~ToCharHolder() override = default;

        static Status Make(const FunctionNode& node, std::shared_ptr<ToCharHolder>* holder);

        static Status Make(const std::string& java_pattern, std::shared_ptr<ToCharHolder>* holder);

        const char* Format(T number, int32_t number_type) {
            return "";
        }

    private:

    explicit ToCharHolder(const char* pattern, int32_t pattern_size)
            : pattern_(pattern), pattern_size_(pattern_size) {
        maximumFractionDigits_ = Setup();
    }

        // Sets the format's metadata, such as the maximum number of decimal digits and if
        // the patterns contains a dollar sign.
        int32_t Setup() {
            int32_t ret = 0;
            bool is_decimal_part = false;
            has_dollar_sign_ = false;
            for (size_t i = 0; i < pattern_size_; ++i) {
                if (pattern_[i] == '$') {
                    has_dollar_sign_ = true;
                }

                if (pattern_[i] == '.') {
                    is_decimal_part = true;
                    continue;
                }

                if (is_decimal_part) {
                    ret++;
                }
            }

            return ret;
        }

        const char* pattern_;
        size_t pattern_size_;
        int32_t maximumFractionDigits_;
        bool has_dollar_sign_;

    };
}