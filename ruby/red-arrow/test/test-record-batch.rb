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
        @decimal_type = Arrow::Decimal128DataType.new(8, 2)
        # FIXME: `name:` should be "item".  I don't know why it is.
        # If the value of `name` is changed, the following error is occurred:
        #
        #     Arrow::Error::Invalid: [record-batch][new]: Invalid: Column 9
        #     type not match schema: list<item: bool> vs list<visible: bool>
        @list_type = Arrow::ListDataType.new(name: "item", type: :boolean)
        @struct_type = Arrow::StructDataType.new(
          visible: :boolean,
          value: :double,
          mark: :string
        )
        @schema = Arrow::Schema.new(
          name: :string,
          count: :uint32,
          weight: :double,
          price: @decimal_type,
          date32: :date32,
          date64: :date64,
          timestamp_sec: Arrow::TimestampDataType.new(:second),
          timestamp_msec: Arrow::TimestampDataType.new(:milli),
          timestamp_usec: Arrow::TimestampDataType.new(:micro),
          flags: @list_type,
          struct: @struct_type
        )

        @names = Arrow::StringArray.new(["apple", "orange", "watermelon", "octopus"])
        @counts = Arrow::UInt32Array.new([1, 2, 4, 8])
        @weights = Arrow::DoubleArray.new([10.1, 11.2, 12.3, 13.4])
        @prices = Arrow::Decimal128Array.new(
          @decimal_type,
          [
            Arrow::Decimal128.new("123.45"),
            Arrow::Decimal128.new("234.56"),
            nil,
            Arrow::Decimal128.new("345.67")
          ])

        @date32_expected = [
          Date.new(1993,  2, 24),
          Date.new(1996, 12, 25),
          Date.new(2013,  2, 24),
          Date.new(2020, 12, 25)
        ]
        epoch_date = Date.new(1970, 1, 1)
        @date32 = Arrow::Date32Array.new(
          @date32_expected.map {|dt| (dt - epoch_date).to_i }
        )

        @date64_expected = [
          DateTime.new(1993,  2, 23, 15, 0, 0),
          DateTime.new(1996, 12, 24, 15, 0, 0),
          DateTime.new(2013,  2, 23, 15, 0, 0),
          DateTime.new(2020, 12, 24, 15, 0, 0)
        ]
        @date64 = Arrow::Date64Array.new(
          @date64_expected.map {|dt| dt.to_time.gmtime.to_i * 1000 }
        )

        jst = '+09:00'
        @timestamp_expected = [
          Time.new(1993,  2, 24, 0, 0, 0, jst).gmtime,
          Time.new(1996, 12, 25, 0, 0, 0, jst).gmtime,
          Time.new(2013,  2, 24, 0, 0, 0, jst).gmtime,
          Time.new(2020, 12, 25, 0, 0, 0, jst).gmtime
        ]
        @timestamp_sec = Arrow::TimestampArray.new(
          :second,
          @timestamp_expected.map(&:to_i)
        )
        @timestamp_msec = Arrow::TimestampArray.new(
          :milli,
          @timestamp_expected.map {|ts| (ts.to_r * 1_000).to_i }
        )
        @timestamp_usec = Arrow::TimestampArray.new(
          :micro,
          @timestamp_expected.map {|ts| (ts.to_r * 1_000_000).to_i }
        )

        @list_raw_values = [
          [true, false],
          nil,
          [false, true, false, false],
          [true]
        ]
        @list = Arrow::ListArray.new(@list_type, @list_raw_values)

        @struct_raw_values = [
          [true, 3.14, 'a'],
          nil,
          [false, 2.71, 'c'],
          [true, Float::INFINITY, 'z'],
        ]
        @struct = Arrow::StructArray.new(
          @struct_type,
          @struct_raw_values
        )

        @record_batch = Arrow::RecordBatch.new(
          @schema, @counts.length,
          [
            @names,
            @counts,
            @weights,
            @prices,
            @date32,
            @date64,
            @timestamp_sec,
            @timestamp_msec,
            @timestamp_usec,
            @list,
            @struct
          ]
        )
      end

      test("default") do
        raw_records = @record_batch.raw_records
        expected = [
          ["apple",      1, 10.1, Arrow::Decimal128.new("123.45")],
          ["orange",     2, 11.2, Arrow::Decimal128.new("234.56")],
          ["watermelon", 4, 12.3, nil],
          ["octopus",    8, 13.4, Arrow::Decimal128.new("345.67")],
        ]
        @date32_expected.each_with_index {|x, i| expected[i] << x }
        @date64_expected.each_with_index {|x, i| expected[i] << x }
        @timestamp_expected.each_with_index {|x, i| expected[i] << x }
        @timestamp_expected.each_with_index {|x, i| expected[i] << x }
        @timestamp_expected.each_with_index {|x, i| expected[i] << x }
        @list_raw_values.each_with_index {|x, i|
          expected[i] << (x ? Arrow::BooleanArray.new(x) : nil)
        }
        struct_names = @struct_type.fields.map(&:name)
        @struct_raw_values.each_with_index {|x, i|
          expected[i] << struct_names.zip(x || []).to_h
        }
        assert_equal(expected, raw_records)
      end

      test("convert_decimal: true") do
        raw_records = @record_batch.raw_records(convert_decimal: true)
        expected = [
          ["apple",      1, 10.1, BigDecimal("123.45")],
          ["orange",     2, 11.2, BigDecimal("234.56")],
          ["watermelon", 4, 12.3, nil],
          ["octopus",    8, 13.4, BigDecimal("345.67")],
        ]
        @date32_expected.each_with_index {|x, i| expected[i] << x }
        @date64_expected.each_with_index {|x, i| expected[i] << x }
        @timestamp_expected.each_with_index {|x, i| expected[i] << x }
        @timestamp_expected.each_with_index {|x, i| expected[i] << x }
        @timestamp_expected.each_with_index {|x, i| expected[i] << x }
        @list_raw_values.each_with_index {|x, i|
          expected[i] << (x ? Arrow::BooleanArray.new(x) : nil)
        }
        struct_names = @struct_type.fields.map(&:name)
        @struct_raw_values.each_with_index {|x, i|
          expected[i] << struct_names.zip(x || []).to_h
        }
        assert_equal(expected, raw_records)
      end
    end
  end
end
