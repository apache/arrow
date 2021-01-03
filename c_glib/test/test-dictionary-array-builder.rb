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

  sub_test_case("BinaryDictionaryArrayBuilder") do
    sub_test_case("constructed from empty") do
      def setup
        super

        @dictionary = %w(foo bar baz)
        @dictionary_array = build_binary_array(@dictionary)
        @indices = @values.map {|x| x ? @dictionary.index(x) : nil }
        @indices_array = build_int8_array(@indices)
        @data_type = Arrow::DictionaryDataType.new(@indices_array.value_data_type,
                                                   @dictionary_array.value_data_type,
                                                   false)
        @expected_array = Arrow::DictionaryArray.new(@data_type,
                                                     @indices_array,
                                                     @dictionary_array)
        @builder = Arrow::BinaryDictionaryArrayBuilder.new
        @values.each do |value|
          if value
            @builder.append_value_bytes(value)
          else
            @builder.append_null
          end
        end
      end

      test("append_value") do
        dictionary_array = build_binary_array([*@dictionary, "qux"])
        indices_array = build_int8_array([*@indices, 3])
        expected_array = Arrow::DictionaryArray.new(@data_type,
                                                    indices_array,
                                                    dictionary_array)

        @builder.append_value("qux")
        assert_equal(expected_array, @builder.finish)
      end

      test("append_value_bytes") do
        dictionary_array = build_binary_array([*@dictionary, "qux"])
        indices_array = build_int8_array([*@indices, 3])
        expected_array = Arrow::DictionaryArray.new(@data_type,
                                                    indices_array,
                                                    dictionary_array)

        @builder.append_value_bytes("qux")
        assert_equal(expected_array, @builder.finish)
      end

      test("append_array") do
        dictionary_array = build_binary_array([*@dictionary, "qux"])
        indices_array = build_int8_array([*@indices, 3, 0, nil, 2])
        expected_array = Arrow::DictionaryArray.new(@data_type,
                                                    indices_array,
                                                    dictionary_array)

        @builder.append_array(build_binary_array(["qux", "foo", nil, "baz"]))
        assert_equal(expected_array, @builder.finish)
      end

      test("append_indices") do
        @builder.insert_memo_values(build_binary_array(["qux"]))
        dictionary_array = build_binary_array([*@dictionary, "qux"])
        indices_array = build_int8_array([*@indices, 1, 2, nil, 3, 0, 1, 2, 1, 3, 0])
        expected_array = Arrow::DictionaryArray.new(@data_type,
                                                    indices_array,
                                                    dictionary_array)

        @builder.append_indices([1, 2, 1, 3, 0],
                                [true, true, false, true, true])
        @builder.append_indices([1, 2, 1, 3, 0])
        assert_equal(expected_array, @builder.finish)
      end

      test("append_nulls") do
        dictionary_array = build_binary_array([])
        indices_array = build_int8_array([nil, nil, nil])
        data_type = Arrow::DictionaryDataType.new(indices_array.value_data_type,
                                                  dictionary_array.value_data_type,
                                                  false)
        expected_array = Arrow::DictionaryArray.new(data_type,
                                                    indices_array,
                                                    dictionary_array)
        builder = Arrow::BinaryDictionaryArrayBuilder.new
        builder.append_nulls(3)
        assert_equal(expected_array,
                     builder.finish)
      end

      test("append_empty_values") do
        dictionary_array = build_binary_array(["hello"])
        indices_array = build_int8_array([0, 0, 0, 0])
        data_type = Arrow::DictionaryDataType.new(indices_array.value_data_type,
                                                  dictionary_array.value_data_type,
                                                  false)
        expected_array = Arrow::DictionaryArray.new(data_type,
                                                    indices_array,
                                                    dictionary_array)
        builder = Arrow::BinaryDictionaryArrayBuilder.new
        builder.append_value("hello")
        builder.append_empty_value
        builder.append_empty_values(2)
        assert_equal(expected_array,
                     builder.finish)
      end

      test("dictionary_length") do
        assert_equal(@dictionary.length, @builder.dictionary_length)
      end

      test("finish") do
        assert_equal(@expected_array,
                     @builder.finish)
      end

      test("finish_delta") do
        assert_equal([
                       true,
                       @indices_array,
                       @dictionary_array,
                     ],
                     @builder.finish_delta)
      end

      test("reset") do
        expected_array = Arrow::DictionaryArray.new(@data_type,
                                                    build_int8_array([]),
                                                    @dictionary_array)
        @builder.reset
        assert_equal({
                       dictionary_length: @dictionary.length,
                       array: expected_array,
                     },
                     {
                       dictionary_length: @builder.dictionary_length,
                       array: @builder.finish,
                     })
      end

      test("reset_full") do
        expected_array = Arrow::DictionaryArray.new(@data_type,
                                                    build_int8_array([]),
                                                    build_binary_array([]))
        @builder.reset_full
        assert_equal({
                       dictionary_length: 0,
                       array: expected_array,
                     },
                     {
                       dictionary_length: @builder.dictionary_length,
                       array: @builder.finish,
                     })
      end
    end

    sub_test_case("constructed with memo values") do
      def setup
        super

        @dictionary = %w(qux foo bar baz)
        dictionary_array = build_binary_array(@dictionary)
        indices = @values.map {|x| x ? @dictionary.index(x) : nil }
        indices_array = build_int8_array(indices)
        data_type = Arrow::DictionaryDataType.new(indices_array.value_data_type,
                                                  dictionary_array.value_data_type,
                                                  false)
        @expected_array = Arrow::DictionaryArray.new(data_type,
                                                     indices_array,
                                                     dictionary_array)

        @builder = Arrow::BinaryDictionaryArrayBuilder.new
        @builder.insert_memo_values(dictionary_array)
        @values.each do |value|
          if value
            @builder.append_value_bytes(value)
          else
            @builder.append_null
          end
        end
      end

      test("dictionary_length") do
        assert_equal(@dictionary.length, @builder.dictionary_length)
      end

      test("finish") do
        assert_equal(@expected_array, @builder.finish)
      end
    end
  end

  sub_test_case("StringDictionaryArrayBuilder") do
    sub_test_case("constructed from empty") do
      def setup
        super

        @dictionary = %w(foo bar baz)
        @dictionary_array = build_string_array(@dictionary)
        @indices = @values.map {|x| x ? @dictionary.index(x) : nil }
        @indices_array = build_int8_array(@indices)
        @data_type = Arrow::DictionaryDataType.new(@indices_array.value_data_type,
                                                   @dictionary_array.value_data_type,
                                                   false)
        @expected_array = Arrow::DictionaryArray.new(@data_type,
                                                     @indices_array,
                                                     @dictionary_array)
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
        assert_equal(expected_array, @builder.finish)
      end

      test("append_array") do
        dictionary_array = build_string_array([*@dictionary, "qux"])
        indices_array = build_int8_array([*@indices, 3, 0, nil, 2])
        expected_array = Arrow::DictionaryArray.new(@data_type,
                                                    indices_array,
                                                    dictionary_array)

        @builder.append_array(build_string_array(["qux", "foo", nil, "baz"]))
        assert_equal(expected_array, @builder.finish)
      end

      test("append_indices") do
        @builder.insert_memo_values(build_string_array(["qux"]))
        dictionary_array = build_string_array([*@dictionary, "qux"])
        indices_array = build_int8_array([*@indices, 1, 2, nil, 3, 0, 1, 2, 1, 3, 0])
        expected_array = Arrow::DictionaryArray.new(@data_type,
                                                    indices_array,
                                                    dictionary_array)

        @builder.append_indices([1, 2, 1, 3, 0],
                                [true, true, false, true, true])
        @builder.append_indices([1, 2, 1, 3, 0])
        assert_equal(expected_array, @builder.finish)
      end

      test("append_nulls") do
        dictionary_array = build_string_array([])
        indices_array = build_int8_array([nil, nil, nil])
        data_type = Arrow::DictionaryDataType.new(indices_array.value_data_type,
                                                  dictionary_array.value_data_type,
                                                  false)
        expected_array = Arrow::DictionaryArray.new(data_type,
                                                    indices_array,
                                                    dictionary_array)
        builder = Arrow::StringDictionaryArrayBuilder.new
        builder.append_nulls(3)
        assert_equal(expected_array,
                     builder.finish)
      end

      test("append_empty_values") do
        dictionary_array = build_string_array(["hello"])
        indices_array = build_int8_array([0, 0, 0, 0])
        data_type = Arrow::DictionaryDataType.new(indices_array.value_data_type,
                                                  dictionary_array.value_data_type,
                                                  false)
        expected_array = Arrow::DictionaryArray.new(data_type,
                                                    indices_array,
                                                    dictionary_array)
        builder = Arrow::StringDictionaryArrayBuilder.new
        builder.append_string("hello")
        builder.append_empty_value
        builder.append_empty_values(2)
        assert_equal(expected_array,
                     builder.finish)
      end

      test("dictionary_length") do
        assert_equal(@dictionary.length, @builder.dictionary_length)
      end

      test("finish") do
        assert_equal(@expected_array,
                     @builder.finish)
      end

      test("finish_delta") do
        assert_equal([
                       true,
                       @indices_array,
                       @dictionary_array,
                     ],
                     @builder.finish_delta)
      end

      test("reset") do
        expected_array = Arrow::DictionaryArray.new(@data_type,
                                                    build_int8_array([]),
                                                    @dictionary_array)
        @builder.reset
        assert_equal({
                       dictionary_length: @dictionary.length,
                       array: expected_array,
                     },
                     {
                       dictionary_length: @builder.dictionary_length,
                       array: @builder.finish,
                     })
      end

      test("reset_full") do
        expected_array = Arrow::DictionaryArray.new(@data_type,
                                                    build_int8_array([]),
                                                    build_string_array([]))
        @builder.reset_full
        assert_equal({
                       dictionary_length: 0,
                       array: expected_array,
                     },
                     {
                       dictionary_length: @builder.dictionary_length,
                       array: @builder.finish,
                     })
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
        assert_equal(@dictionary.length, @builder.dictionary_length)
      end

      test("finish") do
        assert_equal(@expected_array, @builder.finish)
      end
    end
  end
end
