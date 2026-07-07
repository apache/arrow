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

class TestPrimitiveArray < Test::Unit::TestCase
  def test_no_null
    assert_equal([true, false],
                 ArrowFormat::BooleanArray.new([true, false]).to_a)
  end

  def test_null_multiple_bytes
    values = [true] * 8 + [nil, false]
    assert_equal(values,
                 ArrowFormat::BooleanArray.new(values).to_a)
  end

  def test_boolean
    assert_equal([true, nil, false],
                 ArrowFormat::BooleanArray.new([true, nil, false]).to_a)
  end

  def test_int8
    values = [-(2 ** 7), nil, (2 ** 7) - 1]
    assert_equal(values,
                 ArrowFormat::Int8Array.new(values).to_a)
  end

  def test_uint8
    values = [0, nil, (2 ** 8) - 1]
    assert_equal(values,
                 ArrowFormat::UInt8Array.new(values).to_a)
  end

  def test_int16
    values = [-(2 ** 15), nil, (2 ** 15) - 1]
    assert_equal(values,
                 ArrowFormat::Int16Array.new(values).to_a)
  end

  def test_uint16
    values = [0, nil, (2 ** 16) - 1]
    assert_equal(values,
                 ArrowFormat::UInt16Array.new(values).to_a)
  end

  def test_int32
    values = [-(2 ** 31), nil, (2 ** 31) - 1]
    assert_equal(values,
                 ArrowFormat::Int32Array.new(values).to_a)
  end

  def test_uint32
    values = [0, nil, (2 ** 32) - 1]
    assert_equal(values,
                 ArrowFormat::UInt32Array.new(values).to_a)
  end

  def test_int64
    values = [-(2 ** 63), nil, (2 ** 63) - 1]
    assert_equal(values,
                 ArrowFormat::Int64Array.new(values).to_a)
  end

  def test_uint64
    values = [0, nil, (2 ** 64) - 1]
    assert_equal(values,
                 ArrowFormat::UInt64Array.new(values).to_a)
  end

  def test_float32
    values = [-Float::INFINITY, -0.0, nil, +0.0, +Float::INFINITY]
    assert_equal(values,
                 ArrowFormat::Float32Array.new(values).to_a)
  end

  def test_float64
    values = [-Float::INFINITY, -0.0, nil, +0.0, +Float::INFINITY]
    assert_equal(values,
                 ArrowFormat::Float64Array.new(values).to_a)
  end
end
