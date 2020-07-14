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

class TestDictinaryArrayBuilder < Test::Unit::TestCase
  include Helper::Buildable

  def setup
    @values = [
                *%w(foo bar foo),
                nil,
                *%w(foo baz bar baz baz)
              ]
  end

  sub_test_case("StringDictionaryArrayBuilder") do
    sub_test_case("constructed from empty") do
      def setup
        super

        @dictionary = %w(foo bar baz)
        dictionary_array = build_string_array(@dictionary)
        @indices = @values.map {|x| x ? @dictionary.index(x) : nil }
        indices_array = build_int8_array(@indices)
        @data_type = Arrow::DictionaryDataType.new(indices_array.value_data_type,
                                                   dictionary_array.value_data_type,
                                                   false)
        @expected_array = Arrow::DictionaryArray.new(@data_type,
                                                     indices_array,
                                                     dictionary_array)
        @builder = Arrow::StringDictionaryArrayBuilder.new
        @values.each do |value|
          if value
            @builder.append_string(value)
          else
            @builder.append_null
          end
        end
      end

      test("append_string") do
        dictionary_array = build_string_array([*@dictionary, "qux"])
        indices_array = build_int8_array([*@indices, 3])
        expected_array = Arrow::DictionaryArray.new(@data_type,
                                                    indices_array,
                                                    dictionary_array)

        @builder.append_string("qux")
        assert do
          expected_array == @builder.finish
        end
      end

      test("append_array") do
        dictionary_array = build_string_array([*@dictionary, "qux"])
        indices_array = build_int8_array([*@indices, 3, 0, nil, 2])
        expected_array = Arrow::DictionaryArray.new(@data_type,
                                                    indices_array,
                                                    dictionary_array)

        @builder.append_string_array(build_string_array(["qux", "foo", nil, "baz"]))
        assert do
          expected_array == @builder.finish
        end
      end

      test("dictionary_length") do
        assert do
          @dictionary.length == @builder.dictionary_length
        end
      end

      test("finish") do
        assert do
          @expected_array == @builder.finish
        end
      end
    end

    sub_test_case("constructed with memo values") do
      def setup
        super

        @dictionary = %w(qux foo bar baz)
        dictionary_array = build_string_array(@dictionary)
        indices = @values.map {|x| x ? @dictionary.index(x) : nil }
        indices_array = build_int8_array(indices)
        data_type = Arrow::DictionaryDataType.new(indices_array.value_data_type,
                                                  dictionary_array.value_data_type,
                                                  false)
        @expected_array = Arrow::DictionaryArray.new(data_type,
                                                     indices_array,
                                                     dictionary_array)

        @builder = Arrow::StringDictionaryArrayBuilder.new
        @builder.insert_memo_values(dictionary_array)
        @values.each do |value|
          if value
            @builder.append_string(value)
          else
            @builder.append_null
          end
        end
      end

      test("dictionary_length") do
        assert do
          @dictionary.length == @builder.dictionary_length
        end
      end

      test("finish") do
        assert do
          @expected_array == @builder.finish
        end
      end
    end
  end
end
