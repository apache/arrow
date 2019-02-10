/*
 * Copyright 2018 Kenta Murata <mrkn@mrkn.jp>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "red_arrow.hpp"

namespace red_arrow {

VALUE mArrow;
VALUE cRecordBatch;
ID id_BigDecimal;

}  // namespace rb

extern "C" void Init_arrow() {
  red_arrow::mArrow = rb_const_get_at(rb_cObject, rb_intern("Arrow"));
  red_arrow::cRecordBatch = rb_const_get_at(red_arrow::mArrow, rb_intern("RecordBatch"));
  rb_define_method(red_arrow::cRecordBatch, "raw_records",
                   reinterpret_cast<VALUE(*)(ANYARGS)>(red_arrow::record_batch_raw_records), -1);

  red_arrow::id_BigDecimal = rb_intern("BigDecimal");
}
