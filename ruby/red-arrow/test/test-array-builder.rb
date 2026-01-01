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

class ArrayBuilderTest < Test::Unit::TestCase
  sub_test_case(".build") do
    def assert_build(builder_class, raw_array)
      array = builder_class.build(raw_array)
      assert_equal(raw_array, array.to_a)
    end

    sub_test_case("generic builder") do
      test("strings") do
        assert_build(Arrow::ArrayBuilder,
                     ["Hello", nil, "World"])
      end

      test("symbols") do
        array = Arrow::ArrayBuilder.build([:hello, nil, :world])
        expected_builder = Arrow::StringDictionaryArrayBuilder.new
        assert_equal(expected_builder.build(["hello", nil, "world"]),
                     array)
      end

      test("boolean") do
        assert_build(Arrow::ArrayBuilder,
                     [true, nil, false])
      end

      test("positive integers") do
        assert_build(Arrow::ArrayBuilder,
                     [1, nil, 2, nil, 3])
      end

      test("negative integers") do
        assert_build(Arrow::ArrayBuilder,
                     [nil, -1, nil, -2, nil, -3])
      end

      test("times") do
        assert_build(Arrow::ArrayBuilder,
                     [Time.at(0), Time.at(1), Time.at(2)])
      end

      test("dates") do
        assert_build(Arrow::ArrayBuilder,
                     [Date.new(2018, 1, 4), Date.new(2018, 1, 5)])
      end

      test("datetimes") do
        assert_build(Arrow::ArrayBuilder,
                     [
                       DateTime.new(2018, 1, 4, 23, 18, 23),
                       DateTime.new(2018, 1, 5, 0, 23, 21),
                     ])
      end

      test("decimal + string") do
        raw_array = [BigDecimal("10.1"), "10.1"]
        array = Arrow::ArrayBuilder.build(raw_array)
        assert_equal(raw_array.collect(&:to_s), array.to_a)
      end

      test("NaN") do
        raw_array = [BigDecimal("10.1"), BigDecimal::NAN]
        array = Arrow::ArrayBuilder.build(raw_array)
        assert_equal(raw_array.collect(&:to_s), array.to_a)
      end

      test("Infinity") do
        raw_array = [BigDecimal("10.1"), BigDecimal::INFINITY]
        array = Arrow::ArrayBuilder.build(raw_array)
        assert_equal(raw_array.collect(&:to_s), array.to_a)
      end

      test("decimal128") do
        values = [
          BigDecimal("10.1"),
          BigDecimal("1.11"),
          BigDecimal("1"),
        ]
        array = Arrow::Array.new(values)
        data_type = Arrow::Decimal128DataType.new(3, 2)
        assert_equal({
                       data_type: data_type,
                       values: [
                         BigDecimal("10.1"),
                         BigDecimal("1.11"),
                         BigDecimal("1"),
                       ],
                     },
                     {
                       data_type: array.value_data_type,
                       values: array.to_a,
                     })
      end

      test("decimal256") do
        values = [
          BigDecimal("1" * 40 + ".1"),
          BigDecimal("1" * 38 + ".11"),
          BigDecimal("1" * 37),
        ]
        array = Arrow::Array.new(values)
        data_type = Arrow::Decimal256DataType.new(41, 2)
        assert_equal({
                       data_type: data_type,
                       values: [
                         BigDecimal("1" * 40 + ".1"),
                         BigDecimal("1" * 38 + ".11"),
                         BigDecimal("1" * 37),
                       ],
                     },
                     {
                       data_type: array.value_data_type,
                       values: array.to_a,
                     })
      end

      test("list<boolean>s") do
        assert_build(Arrow::ArrayBuilder,
                     [
                       [nil, true, false],
                       nil,
                       [false],
                     ])
      end

      test("list<string>s") do
        assert_build(Arrow::ArrayBuilder,
                     [
                       ["Hello", "World"],
                       ["Apache Arrow"],
                     ])
      end

      sub_test_case("nested integer list") do
        test("list<uint8>s") do
          values = [
            [0, 1, 2],
            [3, 4],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:uint8)
          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, 1, 2],
                           [3, 4],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<int8>s boundary") do
          values = [
            [0, GLib::MININT8],
            [GLib::MAXINT8],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:int8)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MININT8],
                           [GLib::MAXINT8],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<int16>s inferred from int8 underflow") do
          values = [
            [0, GLib::MININT8 - 1],
            [GLib::MAXINT8],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:int16)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MININT8 - 1],
                           [GLib::MAXINT8],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<int16>s inferred from int8 overflow") do
          values = [
            [0, GLib::MAXINT8 + 1],
            [GLib::MININT8],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:int16)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MAXINT8 + 1],
                           [GLib::MININT8],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<int16>s boundary") do
          values = [
            [0, GLib::MININT16],
            [GLib::MAXINT16],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:int16)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MININT16],
                           [GLib::MAXINT16],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<int32>s inferred from int16 underflow") do
          values = [
            [0, GLib::MININT16 - 1],
            [GLib::MAXINT16],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:int32)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MININT16 - 1],
                           [GLib::MAXINT16],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<int32>s inferred from int16 overflow") do
          values = [
            [0, GLib::MAXINT16 + 1],
            [GLib::MININT16],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:int32)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MAXINT16 + 1],
                           [GLib::MININT16],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<int32>s boundary") do
          values = [
            [0, GLib::MININT32],
            [GLib::MAXINT32],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:int32)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MININT32],
                           [GLib::MAXINT32],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<int64>s inferred from int32 underflow") do
          values = [
            [0, GLib::MININT32 - 1],
            [GLib::MAXINT32],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:int64)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MININT32 - 1],
                           [GLib::MAXINT32],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<int64>s inferred from int32 overflow") do
          values = [
            [0, GLib::MAXINT32 + 1],
            [GLib::MININT32],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:int64)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MAXINT32 + 1],
                           [GLib::MININT32],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("string fallback from nested int64 array overflow") do
          values = [
            [0, GLib::MAXINT64 + 1],
            [GLib::MININT64],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:string)

          assert_equal({
                         data_type: data_type,
                         values: [
                           ["0", "#{GLib::MAXINT64 + 1}"],
                           ["#{GLib::MININT64}"],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("string fallback from nested int64 array underflow") do
          values = [
            [0, GLib::MININT64 - 1],
            [GLib::MAXINT64],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:string)

          assert_equal({
                         data_type: data_type,
                         values: [
                           ["0", "#{GLib::MININT64 - 1}"],
                           ["#{GLib::MAXINT64}"],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<uint8>s boundary") do
          values = [
            [0, GLib::MAXUINT8],
            [GLib::MAXUINT8],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:uint8)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MAXUINT8],
                           [GLib::MAXUINT8],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<uint16>s") do
          values = [
            [0, GLib::MAXUINT8 + 1],
            [GLib::MAXUINT8],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:uint16)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MAXUINT8 + 1],
                           [GLib::MAXUINT8],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<uint16>s boundary") do
          values = [
            [0, GLib::MAXUINT16],
            [GLib::MAXUINT16],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:uint16)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MAXUINT16],
                           [GLib::MAXUINT16],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<uint32>s") do
          values = [
            [0, GLib::MAXUINT16 + 1],
            [GLib::MAXUINT16],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:uint32)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MAXUINT16 + 1],
                           [GLib::MAXUINT16],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<uint32>s boundary") do
          values = [
            [0, GLib::MAXUINT32],
            [GLib::MAXUINT32],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:uint32)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MAXUINT32],
                           [GLib::MAXUINT32],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("list<uint64>s") do
          values = [
            [0, GLib::MAXUINT32 + 1],
            [GLib::MAXUINT32],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:uint64)

          assert_equal({
                         data_type: data_type,
                         values: [
                           [0, GLib::MAXUINT32 + 1],
                           [GLib::MAXUINT32],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end

        test("string fallback from nested uint64 array overflow") do
          values = [
            [0, GLib::MAXUINT64 + 1],
            [GLib::MAXUINT64],
          ]
          array = Arrow::Array.new(values)
          data_type = Arrow::ListDataType.new(:string)

          assert_equal({
                         data_type: data_type,
                         values: [
                           ["0", "#{GLib::MAXUINT64 + 1}"],
                           ["#{GLib::MAXUINT64}"],
                         ],
                       },
                       {
                         data_type: array.value_data_type,
                         values: array.to_a,
                       })
        end
      end
    end

    sub_test_case("specific builder") do
      test("empty") do
        assert_build(Arrow::Int32ArrayBuilder,
                     [])
      end

      test("values") do
        assert_build(Arrow::Int32ArrayBuilder,
                     [1, -2])
      end

      test("values, nils") do
        assert_build(Arrow::Int32ArrayBuilder,
                     [1, -2, nil, nil])
      end

      test("values, nils, values") do
        assert_build(Arrow::Int32ArrayBuilder,
                     [1, -2, nil, nil, 3, -4])
      end

      test("values, nils, values, nils") do
        assert_build(Arrow::Int32ArrayBuilder,
                     [1, -2, nil, nil, 3, -4, nil, nil])
      end

      test("nils") do
        assert_build(Arrow::Int32ArrayBuilder,
                     [nil, nil])
      end

      test("nils, values") do
        assert_build(Arrow::Int32ArrayBuilder,
                     [nil, nil, 3, -4])
      end

      test("nils, values, nil") do
        assert_build(Arrow::Int32ArrayBuilder,
                     [nil, nil, 3, -4, nil, nil])
      end

      test("nils, values, nil, values") do
        assert_build(Arrow::Int32ArrayBuilder,
                     [nil, nil, 3, -4, nil, nil, 5, -6])
      end
    end
  end
end
