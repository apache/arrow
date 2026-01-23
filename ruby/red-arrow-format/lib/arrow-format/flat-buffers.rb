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

require_relative "org/apache/arrow/flatbuf/binary"
require_relative "org/apache/arrow/flatbuf/block"
require_relative "org/apache/arrow/flatbuf/bool"
require_relative "org/apache/arrow/flatbuf/date"
require_relative "org/apache/arrow/flatbuf/date_unit"
require_relative "org/apache/arrow/flatbuf/decimal"
require_relative "org/apache/arrow/flatbuf/dictionary_encoding"
require_relative "org/apache/arrow/flatbuf/dictionary_batch"
require_relative "org/apache/arrow/flatbuf/duration"
require_relative "org/apache/arrow/flatbuf/fixed_size_binary"
require_relative "org/apache/arrow/flatbuf/floating_point"
require_relative "org/apache/arrow/flatbuf/footer"
require_relative "org/apache/arrow/flatbuf/int"
require_relative "org/apache/arrow/flatbuf/interval"
require_relative "org/apache/arrow/flatbuf/interval_unit"
require_relative "org/apache/arrow/flatbuf/large_binary"
require_relative "org/apache/arrow/flatbuf/large_list"
require_relative "org/apache/arrow/flatbuf/large_utf8"
require_relative "org/apache/arrow/flatbuf/list"
require_relative "org/apache/arrow/flatbuf/map"
require_relative "org/apache/arrow/flatbuf/message"
require_relative "org/apache/arrow/flatbuf/null"
require_relative "org/apache/arrow/flatbuf/precision"
require_relative "org/apache/arrow/flatbuf/record_batch"
require_relative "org/apache/arrow/flatbuf/schema"
require_relative "org/apache/arrow/flatbuf/struct_"
require_relative "org/apache/arrow/flatbuf/time"
require_relative "org/apache/arrow/flatbuf/time_unit"
require_relative "org/apache/arrow/flatbuf/timestamp"
require_relative "org/apache/arrow/flatbuf/union"
require_relative "org/apache/arrow/flatbuf/union_mode"
require_relative "org/apache/arrow/flatbuf/utf8"

module ArrowFormat
  FB = Org::Apache::Arrow::Flatbuf
end
