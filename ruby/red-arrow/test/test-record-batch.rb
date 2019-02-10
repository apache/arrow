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

class RecordBatchTest < Test::Unit::TestCase
  sub_test_case(".new") do
    def setup
      @schema = Arrow::Schema.new(visible: :boolean,
                                  count: :uint32)
    end

    test("[Schema, records]") do
      records = [
        {visible: true, count: 1},
        nil,
        [false, 3],
      ]
      record_batch = Arrow::RecordBatch.new(@schema, records)
      assert_equal([
                     {"visible" => true,  "count" => 1},
                     {"visible" => nil,   "count" => nil},
                     {"visible" => false, "count" => 3},
                   ],
                   record_batch.each_record.collect(&:to_h))
    end

    test("[Schema, columns]") do
      columns = {
        visible: [true, nil, false],
        count: [1, 2, nil],
      }
      record_batch = Arrow::RecordBatch.new(@schema, columns)
      assert_equal([
                     {"visible" => true,  "count" => 1},
                     {"visible" => nil,   "count" => 2},
                     {"visible" => false, "count" => nil},
                   ],
                   record_batch.each_record.collect(&:to_h))
    end

    test("[Schema, n_rows, columns]") do
      columns = [
        Arrow::BooleanArray.new([true, nil, false]),
        Arrow::UInt32Array.new([1, 2, nil]),
      ]
      n_rows = columns[0].length
      record_batch = Arrow::RecordBatch.new(@schema, n_rows, columns)
      assert_equal([
                     {"visible" => true,  "count" => 1},
                     {"visible" => nil,   "count" => 2},
                     {"visible" => false, "count" => nil},
                   ],
                   record_batch.each_record.collect(&:to_h))
    end
  end

  sub_test_case("instance methods") do
    def setup
      @schema = Arrow::Schema.new(count: :uint32)
      @counts = Arrow::UInt32Array.new([1, 2, 4, 8])
      @record_batch = Arrow::RecordBatch.new(@schema, @counts.length, [@counts])
    end

    sub_test_case("#each") do
      test("default") do
        records = []
        @record_batch.each do |record|
          records << [record, record.index]
        end
        assert_equal([
                       [0, 0],
                       [1, 1],
                       [2, 2],
                       [3, 3],
                     ],
                     records.collect {|record, i| [record.index, i]})
      end

      test("reuse_record: true") do
        records = []
        @record_batch.each(reuse_record: true) do |record|
          records << [record, record.index]
        end
        assert_equal([
                       [3, 0],
                       [3, 1],
                       [3, 2],
                       [3, 3],
                     ],
                     records.collect {|record, i| [record.index, i]})
      end
    end

    test("#to_table") do
      assert_equal(Arrow::Table.new(@schema, [@counts]),
                   @record_batch.to_table)
    end

    sub_test_case("#==") do
      test("Arrow::RecordBatch") do
        assert do
          @record_batch == @record_batch
        end
      end

      test("not Arrow::RecordBatch") do
        assert do
          not (@record_batch == 29)
        end
      end
    end

    sub_test_case("#raw_records") do
      def setup
        @string_values = ["apple", "orange", "watermelon", nil, "タコ"]
        @string_array = Arrow::StringArray.new(@string_values)

        @uint32_values = [1, 2, 4, 8, 16]
        @uint32_array = Arrow::UInt32Array.new(@uint32_values)

        @double_values = [10.1, 11.2, 12.3, 13.4, 14.4]
        @double_array = Arrow::DoubleArray.new(@double_values)

        @decimal128_values = ['123.45', '234.56', nil, '345.67', "456.78"]
        @decimal128_array = Arrow::Decimal128Array.new(
          Arrow::Decimal128DataType.new(8, 2), @decimal128_values)

        @date32_values = [
          Date.new(1993,  2, 24),
          Date.new(1996, 12, 25),
          Date.new(2013,  2, 24),
          Date.new(2020, 12, 25),
          nil
        ]
        epoch_date = Date.new(1970, 1, 1)
        @date32_array = Arrow::Date32Array.new(
          @date32_values.map {|dt| dt && (dt - epoch_date).to_i }
        )

        @date64_values = [
          nil,
          DateTime.new(1993,  2, 23, 15, 0, 0),
          DateTime.new(1996, 12, 24, 15, 0, 0),
          DateTime.new(2013,  2, 23, 15, 0, 0),
          DateTime.new(2020, 12, 24, 15, 0, 0)
        ]
        @date64_array = Arrow::Date64Array.new(
          @date64_values.map {|dt| dt && dt.to_time.gmtime.to_i * 1000 }
        )

        jst = '+09:00'
        @timestamp_values = [
          Time.new(1993,  2, 24, 0, 0, 0, jst).gmtime,
          Time.new(1996, 12, 25, 0, 0, 0, jst).gmtime,
          nil,
          Time.new(2013,  2, 24, 0, 0, 0, jst).gmtime,
          Time.new(2020, 12, 25, 0, 0, 0, jst).gmtime
        ]
        @timestamp_sec_array = Arrow::TimestampArray.new(
          :second,
          @timestamp_values.map {|ts| ts && ts.to_i }
        )
        @timestamp_msec_array = Arrow::TimestampArray.new(
          :milli,
          @timestamp_values.map {|ts| ts && (ts.to_r * 1_000).to_i }
        )
        @timestamp_usec_array = Arrow::TimestampArray.new(
          :micro,
          @timestamp_values.map {|ts| ts && (ts.to_r * 1_000_000).to_i }
        )

        @list_values = [
          [true, false],
          nil,
          [false, true, false, false],
          [true],
          [false, true, true]
        ]
        @list_array = Arrow::ListArray.new(
          # FIXME: `name:` should be "item".  I don't know why it is.
          # If the value of `name` is changed, the following error is occurred:
          #
          #     Arrow::Error::Invalid: [record-batch][new]: Invalid: Column 9
          #     type not match schema: list<item: bool> vs list<visible: bool>
          Arrow::ListDataType.new(name: "item", type: :boolean),
          @list_values
        )

        @struct_values = [
          [true, 3.14, 'a'],
          nil,
          [false, 2.71, 'c'],
          nil,
          [true, Float::INFINITY, 'z'],
        ]
        @struct_array = Arrow::StructArray.new(
          Arrow::StructDataType.new(
            visible: :boolean,
            value: :double,
            mark: :string
          ),
          @struct_values
        )

        dense_union_type_ids = [ 0, 1, 1, 0, 2 ]
        dense_union_offsets = [ 0, 0, 1, 1, 0 ]
        dense_union_children = [
          Arrow::Int32Array.new([42, -42]),
          Arrow::StringArray.new(%w[foo ほげ]),
          Arrow::Decimal128Array.new(
            Arrow::Decimal128DataType.new(8, 2),
            ['3.14']
          )
        ]
        # TODO: we should test with non-continuous type codes, but we cannot
        # because arrow-glib hasn't support to make an array of a union type
        # with non-continuous type codes.
        @dense_union_array = Arrow::DenseUnionArray.new(
          Arrow::Int8Array.new(dense_union_type_ids),
          Arrow::Int32Array.new(dense_union_offsets),
          dense_union_children
        )

        sparse_union_type_ids = [ 0, 1, 2, 1, 0 ]
        sparse_union_children = [
          Arrow::Int32Array.new([42, nil, nil, nil, -42]),
          Arrow::StringArray.new([nil, 'foo', nil, 'ほげ', nil]),
          Arrow::Decimal128Array.new(
            Arrow::Decimal128DataType.new(8, 2),
            [nil, nil, '3.14', nil, nil]
          )
        ]
        @sparse_union_array = Arrow::SparseUnionArray.new(
          Arrow::Int8Array.new(sparse_union_type_ids),
          sparse_union_children
        )

        @dict_vocab = Arrow::StringArray.new(['foo', 'bar', 'baz'])
        @dict_indices = [0, 1, 2, 1, 0]
        @dict_array = Arrow::DictionaryArray.new(
          Arrow::DictionaryDataType.new(:int8, @dict_vocab, true),
          Arrow::Int8Array.new(@dict_indices)
        )

        @schema = Arrow::Schema.new(
          string: :string,
          uint32: :uint32,
          double: :double,
          decimal128: @decimal128_array.value_data_type,
          date32: :date32,
          date64: :date64,
          timestamp_sec: @timestamp_sec_array.value_data_type,
          timestamp_msec: @timestamp_msec_array.value_data_type,
          timestamp_usec: @timestamp_usec_array.value_data_type,
          list: @list_array.value_data_type,
          struct: @struct_array.value_data_type,
          dense_union: @dense_union_array.value_data_type,
          sparse_union: @sparse_union_array.value_data_type,
          dict: @dict_array.value_data_type
        )

        @record_batch = Arrow::RecordBatch.new(
          @schema, @string_values.length,
          [
            @string_array,
            @uint32_array,
            @double_array,
            @decimal128_array,
            @date32_array,
            @date64_array,
            @timestamp_sec_array,
            @timestamp_msec_array,
            @timestamp_usec_array,
            @list_array,
            @struct_array,
            @dense_union_array,
            @sparse_union_array,
            @dict_array
          ]
        )

        struct_field_names = @struct_array.value_data_type.fields.map(&:name)
        @expected_columnar_result = [
          @string_values,
          @uint32_values,
          @double_values,
          @decimal128_values.map {|d| d && Arrow::Decimal128.new(d) },
          @date32_values,
          @date64_values,
          @timestamp_values,
          @timestamp_values,
          @timestamp_values,
          @list_values.map {|x| x && Arrow::BooleanArray.new(x) },
          @struct_values.map {|x| struct_field_names.zip(x || []).to_h },
          dense_union_type_ids.map.with_index {|tid, i|
            offset = dense_union_offsets[i]
            dense_union_children[tid][offset]
          },
          sparse_union_type_ids.map.with_index {|tid, i|
            sparse_union_children[tid][i]
          },
          @dict_indices
        ]
      end

      test("default") do
        raw_records = @record_batch.raw_records
        assert_equal(@expected_columnar_result.transpose, raw_records)
      end

      test("convert_decimal: true") do
        @expected_columnar_result[3] = @decimal128_values.map {|x| x && BigDecimal(x) }
        @expected_columnar_result[11][-1] = BigDecimal('3.14')
        @expected_columnar_result[12][2] = BigDecimal('3.14')

        raw_records = @record_batch.raw_records(convert_decimal: true)
        assert_equal(@expected_columnar_result.transpose, raw_records)
      end
    end
  end
end
