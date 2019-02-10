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

#ifndef RED_ARROW_HPP
#define RED_ARROW_HPP 1

#include <arrow/api.h>

#include <arrow-glib/arrow-glib.hpp>
#include <rbgobject.h>

namespace red_arrow {

extern VALUE mArrow;
extern VALUE cRecordBatch;

extern ID id_BigDecimal;

VALUE record_batch_raw_records(int argc, VALUE* argv, VALUE obj);

}  // namesapce red_arrow

#endif  /* RED_ARROW_HPP */
