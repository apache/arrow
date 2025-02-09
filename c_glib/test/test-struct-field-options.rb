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

class TestStructFieldOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::StructFieldOptions.new
  end

  def test_default
    assert_equal("", @options.field_ref)
  end

  def test_set_string
    @options.field_ref = "foo"
    assert_equal("foo", @options.field_ref)
  end

  def test_set_symbol
    @options.field_ref = :foo
    assert_equal("foo", @options.field_ref)
  end

  def test_set_dot_path
    @options.field_ref = ".foo.bar"
    assert_equal(".foo.bar", @options.field_ref)
  end

  def test_set_invalid
    message = "[struct-field-options][set-field-ref]: " +
              "Invalid: Dot path '[foo]' contained an unterminated index"
    assert_raise(Arrow::Error::Invalid.new(message)) do
      @options.field_ref = "[foo]"
    end
  end

  def test_struct_field_function
    fields = [
      Arrow::Field.new("score", Arrow::Int8DataType.new),
      Arrow::Field.new("enabled", Arrow::BooleanDataType.new),
    ]
    structs = [
      {
        "score" => -29,
        "enabled" => true,
      },
      {
        "score" => 2,
        "enabled" => false,
      },
      nil,
    ]
    args = [
      Arrow::ArrayDatum.new(build_struct_array(fields, structs)),
    ]
    @options.field_ref = "score"
    struct_field_function = Arrow::Function.find("struct_field")
    assert_equal(build_int8_array([-29, 2, nil]),
                 struct_field_function.execute(args, @options).value)
  end
end
