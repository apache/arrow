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

class RawRecordsRecordBatchDenseUnionArrayTest < Test::Unit::TestCase
  def fields(type, type_codes)
    field_description = {}
    if type.is_a?(Hash)
      field_description = field_description.merge(type)
    else
      field_description[:type] = type
    end
    {
      column: {
        type: :dense_union,
        fields: [
          field_description.merge(name: "0"),
          field_description.merge(name: "1"),
        ],
        type_codes: type_codes,
      },
    }
  end

  # TODO: Use Arrow::RecordBatch.new(fields(type), records)
  def build_record_batch(type, records)
    type_codes = [0, 1]
    schema = Arrow::Schema.new(fields(type, type_codes))
    type_ids = []
    offsets = []
    arrays = schema.fields[0].data_type.fields.collect do |field|
      sub_schema = Arrow::Schema.new([field])
      sub_records = []
      records.each do |record|
        column = record[0]
        next if column.nil?
        next unless column.key?(field.name)
        sub_records << [column[field.name]]
      end
      sub_record_batch = Arrow::RecordBatch.new(sub_schema,
                                                sub_records)
      sub_record_batch.columns[0]
    end
    records.each do |record|
      column = record[0]
      if column.nil?
        type_ids << nil
        offsets << 0
      elsif column.key?("0")
        type_id = type_codes[0]
        type_ids << type_id
        offsets << (type_ids.count(type_id) - 1)
      elsif column.key?("1")
        type_id = type_codes[1]
        type_ids << type_id
        offsets << (type_ids.count(type_id) - 1)
      end
    end
    union_array = Arrow::DenseUnionArray.new(schema.fields[0].data_type,
                                             Arrow::Int8Array.new(type_ids),
                                             Arrow::Int32Array.new(offsets),
                                             arrays)
    schema = Arrow::Schema.new(column: union_array.value_data_type)
    Arrow::RecordBatch.new(schema,
                           records.size,
                           [union_array])
  end

  test("NullArray") do
    records = [
      [{"0" => nil}],
      [nil],
    ]
    record_batch = build_record_batch(:null, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("BooleanArray") do
    records = [
      [{"0" => true}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:boolean, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("Int8Array") do
    records = [
      [{"0" => -(2 ** 7)}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:int8, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("UInt8Array") do
    records = [
      [{"0" => (2 ** 8) - 1}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:uint8, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("Int16Array") do
    records = [
      [{"0" => -(2 ** 15)}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:int16, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("UInt16Array") do
    records = [
      [{"0" => (2 ** 16) - 1}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:uint16, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("Int32Array") do
    records = [
      [{"0" => -(2 ** 31)}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:int32, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("UInt32Array") do
    records = [
      [{"0" => (2 ** 32) - 1}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:uint32, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("Int64Array") do
    records = [
      [{"0" => -(2 ** 63)}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:int64, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("UInt64Array") do
    records = [
      [{"0" => (2 ** 64) - 1}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:uint64, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("FloatArray") do
    records = [
      [{"0" => -1.0}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:float, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("DoubleArray") do
    records = [
      [{"0" => -1.0}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:double, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("BinaryArray") do
    records = [
      [{"0" => "\xff".b}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:binary, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("StringArray") do
    records = [
      [{"0" => "Ruby"}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:string, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("Date32Array") do
    records = [
      [{"0" => Date.new(1960, 1, 1)}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:date32, records)
    assert_equal(records, record_batch.raw_records)
  end

  test("Date64Array") do
    records = [
      [{"0" => DateTime.new(1960, 1, 1, 2, 9, 30)}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch(:date64, records)
    assert_equal(records, record_batch.raw_records)
  end

  sub_test_case("TimestampArray") do
    test("second") do
      records = [
        [{"0" => Time.parse("1960-01-01T02:09:30Z")}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch({
                                          type: :timestamp,
                                          unit: :second,
                                        },
                                        records)
      assert_equal(records, record_batch.raw_records)
    end

    test("milli") do
      records = [
        [{"0" => Time.parse("1960-01-01T02:09:30.123Z")}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch({
                                          type: :timestamp,
                                          unit: :milli,
                                        },
                                        records)
      assert_equal(records, record_batch.raw_records)
    end

    test("micro") do
      records = [
        [{"0" => Time.parse("1960-01-01T02:09:30.123456Z")}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch({
                                          type: :timestamp,
                                          unit: :micro,
                                        },
                                        records)
      assert_equal(records, record_batch.raw_records)
    end

    test("nano") do
      records = [
        [{"0" => Time.parse("1960-01-01T02:09:30.123456789Z")}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch({
                                          type: :timestamp,
                                          unit: :nano,
                                        },
                                        records)
      assert_equal(records, record_batch.raw_records)
    end
  end

  sub_test_case("Time32Array") do
    test("second") do
      records = [
        [{"0" => 60 * 10}], # 00:10:00
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch({
                                          type: :time32,
                                          unit: :second,
                                        },
                                        records)
      assert_equal(records, record_batch.raw_records)
    end

    test("milli") do
      records = [
        [{"0" => (60 * 10) * 1000 + 123}], # 00:10:00.123
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch({
                                          type: :time32,
                                          unit: :milli,
                                        },
                                        records)
      assert_equal(records, record_batch.raw_records)
    end
  end

  sub_test_case("Time64Array") do
    test("micro") do
      records = [
        [{"0" => (60 * 10) * 1_000_000 + 123_456}], # 00:10:00.123456
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch({
                                          type: :time64,
                                          unit: :micro,
                                        },
                                        records)
      assert_equal(records, record_batch.raw_records)
    end

    test("nano") do
      records = [
        # 00:10:00.123456789
        [{"0" => (60 * 10) * 1_000_000_000 + 123_456_789}],
        [nil],
        [{"1" => nil}],
      ]
      record_batch = build_record_batch({
                                          type: :time64,
                                          unit: :nano,
                                        },
                                        records)
      assert_equal(records, record_batch.raw_records)
    end
  end

  test("Decimal128Array") do
    records = [
      [{"0" => BigDecimal("92.92")}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch({
                                        type: :decimal128,
                                        precision: 8,
                                        scale: 2,
                                      },
                                      records)
    assert_equal(records, record_batch.raw_records)
  end

  test("ListArray") do
    records = [
      [{"0" => [true, nil, false]}],
      [nil],
      [{"1" => nil}],
    ]
    record_batch = build_record_batch({
                                        type: :list,
                                        field: {
                                          name: :sub_element,
                                          type: :boolean,
                                        },
                                      },
                                      records)
    assert_equal(records, record_batch.raw_records)
  end

  test("StructArray") do
    records = [
      [{"0" => {"sub_field" => true}}],
      [nil],
      [{"1" => nil}],
      [{"0" => {"sub_field" => nil}}],
    ]
    record_batch = build_record_batch({
                                        type: :struct,
                                        fields: [
                                          {
                                            name: :sub_field,
                                            type: :boolean,
                                          },
                                        ],
                                      },
                                      records)
    assert_equal(records, record_batch.raw_records)
  end

  test("SparseUnionArray") do
    omit("Need to add support for SparseUnionArrayBuilder")
    records = [
      [{"0" => {"field1" => true}}],
      [nil],
      [{"1" => nil}],
      [{"0" => {"field2" => nil}}],
    ]
    record_batch = build_record_batch({
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
    assert_equal(records, record_batch.raw_records)
  end

  test("DenseUnionArray") do
    omit("Need to add support for DenseUnionArrayBuilder")
    records = [
      [{"0" => {"field1" => true}}],
      [nil],
      [{"1" => nil}],
      [{"0" => {"field2" => nil}}],
    ]
    record_batch = build_record_batch({
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
    assert_equal(records, record_batch.raw_records)
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
    record_batch = build_record_batch({
                                        type: :dictionary,
                                        index_data_type: :int8,
                                        dictionary: dictionary,
                                        ordered: true,
                                      },
                                      records)
    assert_equal(records, record_batch.raw_records)
  end
end
