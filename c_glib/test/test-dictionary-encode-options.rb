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

class TestDictionaryEncodeOptions < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @options = Arrow::DictionaryEncodeOptions.new
  end

  def test_null_encoding_behavior_property
    assert_equal(Arrow::DictionaryEncodeNullEncodingBehavior::MASK, @options.null_encoding_behavior)
    @options.null_encoding_behavior = :encode
    assert_equal(Arrow::DictionaryEncodeNullEncodingBehavior::ENCODE, 
                 @options.null_encoding_behavior)
  end

  def test_dictionary_encode_function_with_encode
    args = [
      Arrow::ArrayDatum.new(build_string_array(["a", "b", nil, "a", "b"])),
    ]
    @options.null_encoding_behavior = :encode
    dictionary_encode_function = Arrow::Function.find("dictionary_encode")
    result = dictionary_encode_function.execute(args, @options).value
    assert_equal(Arrow::DictionaryDataType.new(Arrow::Int32DataType.new,
                                               Arrow::StringDataType.new,
                                               false),
                 result.value_data_type)
    assert_equal(build_int32_array([0, 1, 2, 0, 1]), result.indices)
  end
end
