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

class TestArrayBuilder < Test::Unit::TestCase
  def test_boolean
    values = [false, true]
    assert_equal(ArrowFormat::BooleanArray.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_uint8
    values = [0, (2 ** 8) - 1]
    assert_equal(ArrowFormat::UInt8Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_int8
    values = [-(2 ** 7), (2 ** 7) - 1]
    assert_equal(ArrowFormat::Int8Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_uint16_min
    values = [0, 2 ** 8]
    assert_equal(ArrowFormat::UInt16Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_uint16_max
    values = [0, (2 ** 16) - 1]
    assert_equal(ArrowFormat::UInt16Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_int16_min
    values = [-(2 ** 7) - 1, 2 ** 7]
    assert_equal(ArrowFormat::Int16Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_int16_max
    values = [-(2 ** 15), (2 ** 15) - 1]
    assert_equal(ArrowFormat::Int16Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_uint32_min
    values = [0, 2 ** 16]
    assert_equal(ArrowFormat::UInt32Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_uint32_max
    values = [0, (2 ** 32) - 1]
    assert_equal(ArrowFormat::UInt32Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_int32_min
    values = [-(2 ** 15) - 1, 2 ** 15]
    assert_equal(ArrowFormat::Int32Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_int32_max
    values = [-(2 ** 31), (2 ** 31) - 1]
    assert_equal(ArrowFormat::Int32Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_uint64_min
    values = [0, 2 ** 32]
    assert_equal(ArrowFormat::UInt64Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_uint64_max
    values = [0, (2 ** 64) - 1]
    assert_equal(ArrowFormat::UInt64Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_int64_min
    values = [-(2 ** 31) - 1, 2 ** 31]
    assert_equal(ArrowFormat::Int64Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_int64_max
    values = [-(2 ** 63), (2 ** 63) - 1]
    assert_equal(ArrowFormat::Int64Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_float64
    values = [-Float::INFINITY, -0.0, +0.0, +Float::INFINITY]
    assert_equal(ArrowFormat::Float64Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_float64_mixed
    values = [-Float::INFINITY, 0, 0, +Float::INFINITY]
    assert_equal(ArrowFormat::Float64Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_utf8
    values = ["Hello", "World"]
    assert_equal(ArrowFormat::UTF8Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_utf8_mixed
    values = ["Hello", 100, "World"]
    assert_equal(ArrowFormat::UTF8Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_nil
    values = ["Hello", nil, "World"]
    assert_equal(ArrowFormat::UTF8Array.new(values),
                 ArrowFormat::Array.build(values))
  end

  def test_time
    values = [Time.at(0)]
    assert_equal(ArrowFormat::TimestampArray.new(:nanosecond, values),
                 ArrowFormat::Array.build(values))
  end

  def test_date
    values = [Date.new(2026, 7, 17)]
    assert_equal(ArrowFormat::Date32Array.new(values),
                 ArrowFormat::Array.build(values))
  end
end
