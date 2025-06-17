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

class TestFixedSizeListDataType < Test::Unit::TestCase
  sub_test_case(".new") do
    def test_field
      list_size = 5
      field_name = "bool_field"
      field = Arrow::Field.new("bool_field", Arrow::BooleanDataType.new)
      data_type = Arrow::FixedSizeListDataType.new(field, list_size)
      assert_equal([field, list_size], [data_type.field, data_type.list_size])
    end

    def test_data_type
      value_type = Arrow::BooleanDataType.new
      list_size = 5
      data_type = Arrow::FixedSizeListDataType.new(value_type, list_size)
      field = Arrow::Field.new("item", value_type)
      assert_equal([field, list_size], [data_type.field, data_type.list_size])
    end
  end

  sub_test_case("instance_methods") do
    def setup
      @list_size = 5
      @value_type = Arrow::BooleanDataType.new
      @data_type = Arrow::FixedSizeListDataType.new(@value_type, @list_size)
    end

    def test_name
      assert_equal("fixed_size_list", @data_type.name);
    end

    def test_to_s
      assert_equal("fixed_size_list<item: bool>[5]", @data_type.to_s)
    end

    def test_list_size
      assert_equal(@list_size, @data_type.list_size)
    end

    def test_field
      field = Arrow::Field.new("item", @value_type)
      assert_equal(field, @data_type.field)
    end
  end
end
