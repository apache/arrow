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

class TestChunkedArray < Test::Unit::TestCase
  include Helper::Buildable

  def test_empty
    chunked_array = Arrow::ChunkedArray.new(Arrow::BooleanDataType.new)
    assert_equal(Arrow::BooleanDataType.new,
                 chunked_array.value_data_type)
  end

  def test_equal
    chunks1 = [
      build_boolean_array([true, false]),
      build_boolean_array([true]),
    ]
    chunks2 = [
      build_boolean_array([true]),
      build_boolean_array([false, true]),
    ]
    assert_equal(Arrow::ChunkedArray.new(chunks1),
                 Arrow::ChunkedArray.new(chunks2))
  end

  def test_value_data_type
    chunks = [
      build_boolean_array([true, false]),
      build_boolean_array([true]),
    ]
    assert_equal(Arrow::BooleanDataType.new,
                 Arrow::ChunkedArray.new(chunks).value_data_type)
  end

  def test_value_type
    chunks = [
      build_boolean_array([true, false]),
      build_boolean_array([true]),
    ]
    assert_equal(Arrow::Type::BOOLEAN,
                 Arrow::ChunkedArray.new(chunks).value_type)
  end

  def test_n_rows
    chunks = [
      build_boolean_array([true, false]),
      build_boolean_array([true]),
    ]
    chunked_array = Arrow::ChunkedArray.new(chunks)
    assert_equal(3, chunked_array.n_rows)
  end

  def test_n_nulls
    chunks = [
      build_boolean_array([true, nil, false]),
      build_boolean_array([nil, nil, true]),
    ]
    chunked_array = Arrow::ChunkedArray.new(chunks)
    assert_equal(3, chunked_array.n_nulls)
  end


  def test_n_chunks
    chunks = [
      build_boolean_array([true]),
      build_boolean_array([false]),
    ]
    chunked_array = Arrow::ChunkedArray.new(chunks)
    assert_equal(2, chunked_array.n_chunks)
  end

  def test_chunk
    chunks = [
      build_boolean_array([true, false]),
      build_boolean_array([false]),
    ]
    chunked_array = Arrow::ChunkedArray.new(chunks)
    assert_equal(2, chunked_array.get_chunk(0).length)
  end

  def test_chunks
    chunks = [
      build_boolean_array([true, false]),
      build_boolean_array([false]),
    ]
    chunked_array = Arrow::ChunkedArray.new(chunks)
    assert_equal([2, 1],
                 chunked_array.chunks.collect(&:length))
  end

  def test_slice
    chunks1 = [
      build_boolean_array([true, false, true]),
      build_boolean_array([false, true]),
    ]
    chunks2 = [
      build_boolean_array([false, true]),
      build_boolean_array([false]),
    ]
    chunked_array = Arrow::ChunkedArray.new(chunks1)
    sub_chunked_array = chunked_array.slice(1, 3)
    assert_equal(chunks2, sub_chunked_array.chunks)
  end

  def test_to_s
    chunks = [
      build_boolean_array([true, false]),
      build_boolean_array([true]),
    ]
    chunked_array = Arrow::ChunkedArray.new(chunks)
    assert_equal(<<-PRETTY_PRINT.chomp, chunked_array.to_s)
[
  [
    true,
    false
  ],
  [
    true
  ]
]
    PRETTY_PRINT
  end

  def test_combine
    chunks = [
      build_boolean_array([true]),
      build_boolean_array([false, nil]),
    ]
    chunked_array = Arrow::ChunkedArray.new(chunks)
    assert_equal(build_boolean_array([true, false, nil]),
                 chunked_array.combine)
  end

  def test_export_import
    chunks = [
      build_boolean_array([true, false, true]),
      build_boolean_array([false, nil]),
    ]
    original_chunked_array = Arrow::ChunkedArray.new(chunks)
    c_abi_array_stream = original_chunked_array.export
    assert_equal(original_chunked_array,
                 Arrow::ChunkedArray.import(c_abi_array_stream))
  end
end
