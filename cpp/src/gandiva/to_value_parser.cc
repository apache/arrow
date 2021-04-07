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

#include "to_value_parser.h"

const char* remove_comma(const char *first, const char *last){
  char *aux = const_cast<char *>(first);
  while (aux != last){
    if (*(aux+1) == ','){
      free(aux+1);
      aux = aux + 2;
    }
    else{
      aux ++;
    }
  }
  return first;
}


namespace gandiva{
    template<typename T>
    const char *from_chars(int64_t context, const char* first, int32_t len,
                           T &value) noexcept {
      using arrow_vendored::fast_float::chars_format;
      using arrow_vendored::fast_float::from_chars_result;
      using arrow_vendored::fast_float::from_chars;

      if (*(first+len) == ',' || len == 0){
        std::string err =
          "Failed to cast the string " + std::string(first, len) + ", invalid format";
          gdv_fn_context_set_error_msg(context, err.c_str());
      }
      first = remove_comma(first,first+len);

      if (std::is_same<T,int32_t>::value){
        value = reinterpret_cast<float>(value);
      }else if (std::is_same<T,int64_t>::value){
        value = reinterpret_cast<double>(value);
      }

      from_chars_result answer =
          from_chars(first, first+len, value, chars_format::general);

      if (std::is_same<T,int32_t>::value){
        value = reinterpret_cast<int32_t>(value);
      }else if (std::is_same<T,int64_t>::value){
        value = reinterpret_cast<int64_t>(value);
      }

      if(answer.ec != std::errc()) {
        std::string err =
            "Failed to cast the string " + std::string(first, len) + " to a number";
        gdv_fn_context_set_error_msg(context, err.c_str());
      }
      return answer.ptr;
    }
}

