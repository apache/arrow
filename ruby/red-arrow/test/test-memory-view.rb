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

class MemoryViewTest < Test::Unit::TestCase
  def setup
    unless Fiddle.const_defined?(:MemoryView)
      omit("Fiddle::MemoryView is needed")
    end
    unless Fiddle::MemoryView.respond_to?(:export)
      omit("Fiddle::MemoryView.export is needed")
    end
  end

  def little_endian?
    [1].pack("s") == [1].pack("s<")
  end

  test("BooleanArray") do
    array = Arrow::BooleanArray.new([true] * 9)
    Fiddle::MemoryView.export(array) do |memory_view|
      if little_endian?
        template = "b"
      else
        template = "B"
      end
      assert_equal([
                     "#{template}8",
                     1,
                     2,
                     [(("1" * 9) + ("0" * 7))].pack("#{template}*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("Int8Array") do
    values = [-(2 ** 7), 0, (2 ** 7) - 1]
    array = Arrow::Int8Array.new(values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "c",
                     1,
                     values.size,
                     values.pack("c*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("Int16Array") do
    values = [-(2 ** 15), 0, (2 ** 15) - 1]
    array = Arrow::Int16Array.new(values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "s",
                     2,
                     2 * values.size,
                     values.pack("s*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("Int32Array") do
    values = [-(2 ** 31), 0, (2 ** 31) - 1]
    array = Arrow::Int32Array.new(values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "l",
                     4,
                     4 * values.size,
                     values.pack("l*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("Int64Array") do
    values = [-(2 ** 63), 0, (2 ** 63) - 1]
    array = Arrow::Int64Array.new(values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "q",
                     8,
                     8 * values.size,
                     values.pack("q*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("UInt8Array") do
    values = [0, (2 ** 8) - 1]
    array = Arrow::UInt8Array.new(values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "C",
                     1,
                     values.size,
                     values.pack("C*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("UInt16Array") do
    values = [0, (2 ** 16) - 1]
    array = Arrow::UInt16Array.new(values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "S",
                     2,
                     2 * values.size,
                     values.pack("S*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("UInt32Array") do
    values = [0, (2 ** 32) - 1]
    array = Arrow::UInt32Array.new(values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "L",
                     4,
                     4 * values.size,
                     values.pack("L*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("UInt64Array") do
    values = [(2 ** 64) - 1]
    array = Arrow::UInt64Array.new(values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "Q",
                     8,
                     8 * values.size,
                     values.pack("Q*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("FloatArray") do
    values = [-1.1, 0.0, 1.1]
    array = Arrow::FloatArray.new(values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "f",
                     4,
                     4 * values.size,
                     values.pack("f*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("DoubleArray") do
    values = [-1.1, 0.0, 1.1]
    array = Arrow::DoubleArray.new(values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "d",
                     8,
                     8 * values.size,
                     values.pack("d*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("FixedSizeBinaryArray") do
    values = ["\x01\x02", "\x03\x04", "\x05\x06"]
    data_type = Arrow::FixedSizeBinaryDataType.new(2)
    array = Arrow::FixedSizeBinaryArray.new(data_type, values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "C2",
                     2,
                     2 * values.size,
                     values.join("").b,
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("Date32Array") do
    n_days_since_epoch = 17406 # 2017-08-28
    values = [n_days_since_epoch]
    array = Arrow::Date32Array.new(values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "l",
                     4,
                     4 * values.size,
                     values.pack("l*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("Date64Array") do
    n_msecs_since_epoch = 1503878400000 # 2017-08-28T00:00:00Z
    values = [n_msecs_since_epoch]
    array = Arrow::Date64Array.new(values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "q",
                     8,
                     8 * values.size,
                     values.pack("q*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("Time32Array") do
    values = [1, 2, 3]
    array = Arrow::Time32Array.new(:milli, values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "l",
                     4,
                     4 * values.size,
                     values.pack("l*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("Time64Array") do
    values = [1, 2, 3]
    array = Arrow::Time64Array.new(:nano, values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "q",
                     8,
                     8 * values.size,
                     values.pack("q*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("TimestampArray") do
    values = [1, 2, 3]
    array = Arrow::TimestampArray.new(:micro, values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "q",
                     8,
                     8 * values.size,
                     values.pack("q*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("Decimal128Array") do
    values = [
      Arrow::Decimal128.new("10.1"),
      Arrow::Decimal128.new("11.1"),
      Arrow::Decimal128.new("10.2"),
    ]
    data_type = Arrow::Decimal128DataType.new(3, 1)
    array = Arrow::Decimal128Array.new(data_type, values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "q2",
                     16,
                     16 * values.size,
                     values.collect {|value| value.to_bytes.to_s}.join(""),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("Decimal256Array") do
    values = [
      Arrow::Decimal256.new("10.1"),
      Arrow::Decimal256.new("11.1"),
      Arrow::Decimal256.new("10.2"),
    ]
    data_type = Arrow::Decimal256DataType.new(3, 1)
    array = Arrow::Decimal256Array.new(data_type, values)
    Fiddle::MemoryView.export(array) do |memory_view|
      assert_equal([
                     "q4",
                     32,
                     32 * values.size,
                     values.collect {|value| value.to_bytes.to_s}.join(""),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end

  test("Buffer") do
    values = [0, nil, nil] * 3
    array = Arrow::Int8Array.new(values)
    buffer = array.null_bitmap
    Fiddle::MemoryView.export(buffer) do |memory_view|
      if little_endian?
        template = "b"
      else
        template = "B"
      end
      assert_equal([
                     "#{template}8",
                     1,
                     2,
                     ["100" * 3].pack("#{template}*"),
                   ],
                   [
                     memory_view.format,
                     memory_view.item_size,
                     memory_view.byte_size,
                     memory_view.to_s,
                   ])
    end
  end
end
