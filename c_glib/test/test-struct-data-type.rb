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

class TestStructDataType < Test::Unit::TestCase
  def setup
    @enabled_field_data_type = Arrow::BooleanDataType.new
    @message_field_data_type = Arrow::StringDataType.new
    @field_data_types = [
      @enabled_field_data_type,
      @message_field_data_type,
    ]
    @enabled_field = Arrow::Field.new("enabled", @enabled_field_data_type)
    @message_field = Arrow::Field.new("message", @message_field_data_type)
    @fields = [@enabled_field, @message_field]
    @data_type = Arrow::StructDataType.new(@fields)
  end

  def test_type
    assert_equal(Arrow::Type::STRUCT, @data_type.id)
  end

  def test_to_s
    assert_equal("struct<enabled: bool, message: string>",
                 @data_type.to_s)
  end

  def test_n_fields
    assert_equal(2, @data_type.n_fields)
  end

  def test_fields
    assert_equal(@fields.zip(@field_data_types),
                 @data_type.fields.collect {|field| [field, field.data_type]})
  end

  sub_test_case("#get_field") do
    def test_found
      assert_equal(@fields[1], @data_type.get_field(1))
    end

    def test_negative
      assert_equal(@fields[-1], @data_type.get_field(-1))
    end

    def test_over
      assert_equal(nil, @data_type.get_field(2))
    end

    def test_data_type
      field = @data_type.get_field(0)
      assert_equal([
                     @fields[0],
                     @field_data_types[0],
                   ],
                   [
                     field,
                     field.data_type,
                   ])
    end
  end

  sub_test_case("#get_field_by_name") do
    def test_found
      assert_equal(@enabled_field,
                   @data_type.get_field_by_name("enabled"))
    end

    def test_not_found
      assert_equal(nil,
                   @data_type.get_field_by_name("nonexistent"))
    end

    def test_data_type
      field = @data_type.get_field_by_name("enabled")
      assert_equal([
                     @enabled_field,
                     @enabled_field_data_type,
                   ],
                   [
                     field,
                     field.data_type,
                   ])
    end
  end

  sub_test_case("#get_field_index") do
    def test_found
      assert_equal(@fields.index(@enabled_field),
                   @data_type.get_field_index("enabled"))
    end

    def test_not_found
      assert_equal(-1,
                   @data_type.get_field_index("nonexistent"))
    end
  end
end
