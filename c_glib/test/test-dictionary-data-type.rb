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

class TestDictionaryDataType < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @index_data_type = Arrow::Int32DataType.new
    @dictionary = build_string_array(["C", "C++", "Ruby"])
    @ordered = true
    @data_type = Arrow::DictionaryDataType.new(@index_data_type,
                                               @dictionary,
                                               @ordered)
  end

  def test_type
    assert_equal(Arrow::Type::DICTIONARY, @data_type.id)
  end

  def test_to_s
    assert_equal("dictionary<values=string, indices=int32, ordered=1>",
                 @data_type.to_s)
  end

  def test_bit_width
    assert_equal(32, @data_type.bit_width)
  end

  def test_index_data_type
    assert_equal(@index_data_type, @data_type.index_data_type)
  end

  def test_dictionary
    assert_equal(@dictionary, @data_type.dictionary)
  end

  def test_ordered?
    assert do
      @data_type.ordered?
    end
  end
end
