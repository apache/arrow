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

#include <cstdint>

#include "arrow/vendored/fast_float/fast_float.h"

#include "gandiva/function_holder.h"
#include "gandiva/node.h"


namespace gandiva{

class GANDIVA_EXPORT DecimalFormatHolder : public FunctionHolder{
  public:
    ~DecimalFormatHolder() override = default;

    static Status Make(const FunctionNode& node, std::shared_ptr<DecimalFormatHolder>* holder);

    static Status Make(const std::string& decimal_format, std::shared_ptr<DecimalFormatHolder>* holder);

    arrow_vendored::fast_float::from_chars_result Parse(
        const char* number, int32_t number_size, double &answer){
      using arrow_vendored::fast_float::from_chars;
      using arrow_vendored::fast_float::chars_format;

      if (has_dolar_sign_){
        number++;
        number_size--;
      }

      std::string res;

      for (int i = 0; i < number_size; ++i) {
        if (number[i] != ','){
          res.push_back(number[i]);
        }
      }

      const char* res_ptr = res.c_str();
      auto result = from_chars(res_ptr, res_ptr + res.size(), answer, chars_format::fixed);

      return result;
    }

  private:
    explicit DecimalFormatHolder(const char* pattern, int32_t pattern_size) :
        pattern_(pattern), pattern_size_(pattern_size){
      maximumFractionDigits_ = Setup();
    }

    // Setups all classes variables and returns the max quantity of fraction digits for
    // this format.
    int32_t Setup(){
      int32_t ret=0;
      const char* aux = pattern_;
      bool is_decimal_part = false;
      has_dolar_sign_= false;
      for (int i = 0; i < pattern_size_; ++i) {
        if (*aux == '$'){
          has_dolar_sign_= true;
        }

        if (*aux == '.'){
          decimal_start_idx_ = i;
          is_decimal_part = true;
        }

        if (is_decimal_part){
          ret++;
        }

        aux++;
      }

      return ret;
    }

    const char* pattern_;
    int32_t pattern_size_;
    int32_t maximumFractionDigits_;
    int32_t decimal_start_idx_;
    bool has_dolar_sign_;
};

}  // namespace gandiva