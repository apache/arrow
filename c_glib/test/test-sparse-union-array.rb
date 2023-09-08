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

class TestSparseUnionArray < Test::Unit::TestCase
  include Helper::Buildable

  sub_test_case(".new") do
    sub_test_case("default") do
      def setup
        type_ids = build_int8_array([0, 1, 0, 1, 0])
        fields = [
          build_int16_array([1, nil, nil, nil, 5]),
          build_string_array([nil, "b", nil, "d", nil]),
        ]
        @array = Arrow::SparseUnionArray.new(type_ids, fields)
      end

      def test_value_data_type
        fields = [
          Arrow::Field.new("0", Arrow::Int16DataType.new),
          Arrow::Field.new("1", Arrow::StringDataType.new),
        ]
        assert_equal(Arrow::SparseUnionDataType.new(fields, [0, 1]),
                     @array.value_data_type)
      end

      def test_field
        assert_equal([
                       build_int16_array([1, nil, nil, nil, 5]),
                       build_string_array([nil, "b", nil, "d", nil]),
                     ],
                     [
                       @array.get_field(0),
                       @array.get_field(1),
                     ])
      end
    end

    sub_test_case("DataType") do
      def setup
        data_type_fields = [
          Arrow::Field.new("number", Arrow::Int16DataType.new),
          Arrow::Field.new("text", Arrow::StringDataType.new),
        ]
        type_codes = [11, 13]
        @data_type = Arrow::SparseUnionDataType.new(data_type_fields, type_codes)
        type_ids = build_int8_array([11, 13, 11, 13, 11])
        fields = [
          build_int16_array([1, nil, nil, nil, 5]),
          build_string_array([nil, "b", nil, "d", nil]),
        ]
        @array = Arrow::SparseUnionArray.new(@data_type, type_ids, fields)
      end

      def test_value_data_type
        assert_equal(@data_type,
                     @array.value_data_type)
      end

      def test_type_code
        assert_equal(11, @array.get_type_code(0))
      end

      def test_child_id
        assert_equal(0, @array.get_child_id(0))
      end

      def test_field
        assert_equal([
                       build_int16_array([1, nil, nil, nil, 5]),
                       build_string_array([nil, "b", nil, "d", nil]),
                     ],
                     [
                       @array.get_field(0),
                       @array.get_field(1),
                     ])
      end
    end
  end
end
