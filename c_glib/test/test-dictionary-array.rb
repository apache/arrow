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

class TestDictionaryArray < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @index_data_type = Arrow::Int32DataType.new
    @dictionary = build_string_array(["C", "C++", "Ruby"])
    @ordered = false
    @data_type = Arrow::DictionaryDataType.new(@index_data_type,
                                               @dictionary,
                                               @ordered)
  end

  sub_test_case(".new") do
    def test_new
      indices = build_int32_array([0, 2, 2, 1, 0])
      dictionary_array = Arrow::DictionaryArray.new(@data_type, indices)
      assert_equal(<<-STRING.chomp, dictionary_array.to_s)

-- is_valid: all not null
-- dictionary: ["C", "C++", "Ruby"]
-- indices: [0, 2, 2, 1, 0]
      STRING
    end
  end

  sub_test_case("instance methods") do
    def setup
      super
      @indices = build_int32_array([0, 2, 2, 1, 0])
      @dictionary_array = Arrow::DictionaryArray.new(@data_type, @indices)
    end

    def test_indices
      assert_equal(@indices, @dictionary_array.indices)
    end

    def test_dictionary
      assert_equal(@dictionary, @dictionary_array.dictionary)
    end

    def test_dictionary_data_type
      assert_equal(@data_type,
                   @dictionary_array.dictionary_data_type)
    end
  end
end
