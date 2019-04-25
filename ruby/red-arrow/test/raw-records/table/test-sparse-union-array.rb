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

require_relative '../helper/sparse-union-array'

class RawRecordsTableSparseUnionArrayTest < Test::Unit::TestCase
  include RawRecordsSparseUnionArrayHelper

  def make_table(type, records)
    record_batch = build_record_batch(type, records)
    Arrow::Table.new(record_batch.schema, [record_batch])
  end

  test("NullArray") do
    records = [
      [{"0" => nil}],
      [nil],
    ]
    table = make_table(:null, records)
    assert_equal(records, table.raw_records)
  end

  test("BooleanArray") do
    records = [
      [{"0" => true}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:boolean, records)
    assert_equal(records, table.raw_records)
  end

  test("Int8Array") do
    records = [
      [{"0" => -(2 ** 7)}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:int8, records)
    assert_equal(records, table.raw_records)
  end

  test("UInt8Array") do
    records = [
      [{"0" => (2 ** 8) - 1}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:uint8, records)
    assert_equal(records, table.raw_records)
  end

  test("Int16Array") do
    records = [
      [{"0" => -(2 ** 15)}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:int16, records)
    assert_equal(records, table.raw_records)
  end

  test("UInt16Array") do
    records = [
      [{"0" => (2 ** 16) - 1}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:uint16, records)
    assert_equal(records, table.raw_records)
  end

  test("Int32Array") do
    records = [
      [{"0" => -(2 ** 31)}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:int32, records)
    assert_equal(records, table.raw_records)
  end

  test("UInt32Array") do
    records = [
      [{"0" => (2 ** 32) - 1}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:uint32, records)
    assert_equal(records, table.raw_records)
  end

  test("Int64Array") do
    records = [
      [{"0" => -(2 ** 63)}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:int64, records)
    assert_equal(records, table.raw_records)
  end

  test("UInt64Array") do
    records = [
      [{"0" => (2 ** 64) - 1}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:uint64, records)
    assert_equal(records, table.raw_records)
  end

  test("FloatArray") do
    records = [
      [{"0" => -1.0}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:float, records)
    assert_equal(records, table.raw_records)
  end

  test("DoubleArray") do
    records = [
      [{"0" => -1.0}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:double, records)
    assert_equal(records, table.raw_records)
  end

  test("BinaryArray") do
    records = [
      [{"0" => "\xff".b}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:binary, records)
    assert_equal(records, table.raw_records)
  end

  test("StringArray") do
    records = [
      [{"0" => "Ruby"}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:string, records)
    assert_equal(records, table.raw_records)
  end

  test("Date32Array") do
    records = [
      [{"0" => Date.new(1960, 1, 1)}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:date32, records)
    assert_equal(records, table.raw_records)
  end

  test("Date64Array") do
    records = [
      [{"0" => DateTime.new(1960, 1, 1, 2, 9, 30)}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table(:date64, records)
    assert_equal(records, table.raw_records)
  end

  sub_test_case("TimestampArray") do
    test("second") do
      records = [
        [{"0" => Time.parse("1960-01-01T02:09:30Z")}],
        [nil],
        [{"1" => nil}],
      ]
      table = make_table({
                           type: :timestamp,
                           unit: :second,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("milli") do
      records = [
        [{"0" => Time.parse("1960-01-01T02:09:30.123Z")}],
        [nil],
        [{"1" => nil}],
      ]
      table = make_table({
                           type: :timestamp,
                           unit: :milli,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("micro") do
      records = [
        [{"0" => Time.parse("1960-01-01T02:09:30.123456Z")}],
        [nil],
        [{"1" => nil}],
      ]
      table = make_table({
                           type: :timestamp,
                           unit: :micro,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("nano") do
      records = [
        [{"0" => Time.parse("1960-01-01T02:09:30.123456789Z")}],
        [nil],
        [{"1" => nil}],
      ]
      table = make_table({
                           type: :timestamp,
                           unit: :nano,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end
  end

  sub_test_case("Time32Array") do
    test("second") do
      records = [
        [{"0" => 60 * 10}], # 00:10:00
        [nil],
        [{"1" => nil}],
      ]
      table = make_table({
                           type: :time32,
                           unit: :second,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("milli") do
      records = [
        [{"0" => (60 * 10) * 1000 + 123}], # 00:10:00.123
        [nil],
        [{"1" => nil}],
      ]
      table = make_table({
                           type: :time32,
                           unit: :milli,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end
  end

  sub_test_case("Time64Array") do
    test("micro") do
      records = [
        [{"0" => (60 * 10) * 1_000_000 + 123_456}], # 00:10:00.123456
        [nil],
        [{"1" => nil}],
      ]
      table = make_table({
                           type: :time64,
                           unit: :micro,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end

    test("nano") do
      records = [
        # 00:10:00.123456789
        [{"0" => (60 * 10) * 1_000_000_000 + 123_456_789}],
        [nil],
        [{"1" => nil}],
      ]
      table = make_table({
                           type: :time64,
                           unit: :nano,
                         },
                         records)
      assert_equal(records, table.raw_records)
    end
  end

  test("Decimal128Array") do
    records = [
      [{"0" => BigDecimal("92.92")}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table({
                         type: :decimal128,
                         precision: 8,
                         scale: 2,
                       },
                       records)
    assert_equal(records, table.raw_records)
  end

  test("ListArray") do
    records = [
      [{"0" => [true, nil, false]}],
      [nil],
      [{"1" => nil}],
    ]
    table = make_table({
                         type: :list,
                         field: {
                           name: :sub_element,
                           type: :boolean,
                         },
                       },
                       records)
    assert_equal(records, table.raw_records)
  end

  test("StructArray") do
    records = [
      [{"0" => {"sub_field" => true}}],
      [nil],
      [{"1" => nil}],
      [{"0" => {"sub_field" => nil}}],
    ]
    table = make_table({
                         type: :struct,
                         fields: [
                           {
                             name: :sub_field,
                             type: :boolean,
                           },
                         ],
                       },
                       records)
    assert_equal(records, table.raw_records)
  end

  test("SparseUnionArray") do
    omit("Need to add support for SparseUnionArrayBuilder")
    records = [
      [{"0" => {"field1" => true}}],
      [nil],
      [{"1" => nil}],
      [{"0" => {"field2" => nil}}],
    ]
    table = make_table({
                         type: :sparse_union,
                         fields: [
                           {
                             name: :field1,
                             type: :boolean,
                           },
                           {
                             name: :field2,
                             type: :uint8,
                           },
                         ],
                         type_codes: [0, 1],
                       },
                       records)
    assert_equal(records, table.raw_records)
  end

  test("DenseUnionArray") do
    omit("Need to add support for DenseUnionArrayBuilder")
    records = [
      [{"0" => {"field1" => true}}],
      [nil],
      [{"1" => nil}],
      [{"0" => {"field2" => nil}}],
    ]
    table = make_table({
                         type: :dense_union,
                         fields: [
                           {
                             name: :field1,
                             type: :boolean,
                           },
                           {
                             name: :field2,
                             type: :uint8,
                           },
                         ],
                         type_codes: [0, 1],
                       },
                       records)
    assert_equal(records, table.raw_records)
  end

  test("DictionaryArray") do
    omit("Need to add support for DictionaryArrayBuilder")
    records = [
      [{"0" => "Ruby"}],
      [nil],
      [{"1" => nil}],
      [{"0" => "GLib"}],
    ]
    dictionary = Arrow::StringArray.new(["GLib", "Ruby"])
    table = make_table({
                         type: :dictionary,
                         index_data_type: :int8,
                         dictionary: dictionary,
                         ordered: true,
                       },
                       records)
    assert_equal(records, table.raw_records)
  end
end
