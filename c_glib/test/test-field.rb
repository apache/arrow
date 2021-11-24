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

class TestField < Test::Unit::TestCase
  include Helper::Omittable

  def setup
    @data_type = Arrow::BooleanDataType.new
    @field = Arrow::Field.new("enabled", @data_type)
    @field_with_metadata = @field.with_metadata("key1" => "value1",
                                                "key2" => "value2")
  end

  def test_export
    require_gi_bindings(3, 4, 8)
    c_abi_schema = @field.export
    assert_equal(@field,
                 Arrow::Field.import(c_abi_schema))
  end

  def test_equal
    assert_equal(Arrow::Field.new("enabled", Arrow::BooleanDataType.new),
                 Arrow::Field.new("enabled", Arrow::BooleanDataType.new))
  end

  def test_name
    assert_equal("enabled", @field.name)
  end

  def test_data_type
    assert_equal(@data_type.to_s,
                 @field.data_type.to_s)
  end

  def test_nullable?
    assert do
      @field.nullable?
    end
  end

  def test_to_s
    assert_equal("enabled: bool", @field_with_metadata.to_s)
  end

  sub_test_case("#to_string_metadata") do
    def test_true
      assert_equal(<<-FIELD.chomp, @field_with_metadata.to_string_metadata(true))
enabled: bool
-- metadata --
key1: value1
key2: value2
      FIELD
    end

    def test_false
      assert_equal(<<-FIELD.chomp, @field_with_metadata.to_string_metadata(false))
enabled: bool
      FIELD
    end
  end

  sub_test_case("#has_metadata?") do
    def test_existent
      assert do
        @field_with_metadata.has_metadata?
      end
    end

    def test_nonexistent
      assert do
        not @field.has_metadata?
      end
    end
  end

  sub_test_case("#metadata") do
    def test_existent
      assert_equal({
                     "key1" => "value1",
                     "key2" => "value2",
                   },
                   @field_with_metadata.metadata)
    end

    def test_nonexistent
      assert_nil(@field.metadata)
    end
  end

  def test_with_metadata
    field = @field_with_metadata.with_metadata("key3" => "value3")
    assert_equal({"key3" => "value3"},
                 field.metadata)
  end

  def test_with_merged_metadata
    field = @field_with_metadata.with_merged_metadata("key1" => "new-value1",
                                                      "key3" => "value3")
    assert_equal({
                   "key1" => "new-value1",
                   "key2" => "value2",
                   "key3" => "value3",
                 },
                 field.metadata)
  end

  def test_remove_metadata
    field = @field_with_metadata.remove_metadata
    assert_nil(field.metadata)
  end
end
