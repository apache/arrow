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

class TestFixedSizeListArray < Test::Unit::TestCase
  include Helper::Buildable

  sub_test_case(".new") do
    def setup
      @array_length = 3
      @list_elem_size = 4
      @value_array = build_uint8_array([10, 20, 30, 40, nil, nil, nil, nil, 50, 60, 70, 80])
      @null_bitmap = Arrow::Buffer.new([0b101].pack("C*"))
      @null_count = 1
    end

    def test_new_array
      array = Arrow::FixedSizeListArray.new(@value_array, @list_elem_size, @null_bitmap, @null_count)
      assert_equal([@array_length, Arrow::UInt8DataType.new, @list_elem_size, @value_array],
                   [array.length, array.value_data_type.field.data_type, array.value_data_type.list_size, array.values])
    end

    def new_data_type
      data_type = Arrow::FixedSizeListDataType.new(Arrow::UInt8DataType.new, list_elem_size)

      array = Arrow::FixedSizeListArray.new(@value_array, data_type, @null_bitmap, @null_count)
      assert_equal([@array_length, Arrow::UInt8DataType.new, @list_elem_size, @value_array],
                   [array.length, array.value_data_type.field.data_type, array.value_data_type.list_size, array.values])
    end
  end
end
