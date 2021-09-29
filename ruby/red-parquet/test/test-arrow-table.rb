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

class TestArrowTable < Test::Unit::TestCase
  def setup
    @count_field = Arrow::Field.new("count", :uint8)
    @visible_field = Arrow::Field.new("visible", :boolean)
    @label_field = Arrow::Field.new("label", :string)
    schema = Arrow::Schema.new([@count_field, @visible_field, @label_field])
    count_arrays = [
      Arrow::UInt8Array.new([1, 2]),
      Arrow::UInt8Array.new([4, 8, 16]),
      Arrow::UInt8Array.new([32, 64]),
      Arrow::UInt8Array.new([128]),
    ]
    visible_arrays = [
      Arrow::BooleanArray.new([true, false, nil]),
      Arrow::BooleanArray.new([true]),
      Arrow::BooleanArray.new([true, false]),
      Arrow::BooleanArray.new([nil]),
      Arrow::BooleanArray.new([nil]),
    ]
    label_arrays = [
      Arrow::StringArray.new(["a"]),
      Arrow::StringArray.new(["b", "c"]),
      Arrow::StringArray.new(["d", nil, nil]),
      Arrow::StringArray.new(["e", "f"]),
    ]
    @count_array = Arrow::ChunkedArray.new(count_arrays)
    @visible_array = Arrow::ChunkedArray.new(visible_arrays)
    @label_array = Arrow::ChunkedArray.new(label_arrays)
    @table = Arrow::Table.new(schema,
                              [@count_array, @visible_array, @label_array])

    @output = Tempfile.open(["red-parquet", ".parquet"])
    begin
      yield(@output)
    ensure
      @output.close!
    end
  end

  def test_save_load_path
    @table.save(@output.path)
    assert do
      @table.equal_metadata(Arrow::Table.load(@output.path), false)
    end
  end

  def test_save_load_buffer
    buffer = Arrow::ResizableBuffer.new(1024)
    @table.save(buffer, format: :parquet)
    assert do
      @table.equal_metadata(Arrow::Table.load(buffer, format: :parquet), false)
    end
  end

  def test_save_load_compression
    @table.save(@output.path, compression: :zstd)
    assert do
      @table.equal_metadata(Arrow::Table.load(@output.path), false)
    end
  end

  def test_save_load_compression_path
    @table.save(@output.path, compression: {"count" => :zstd})
    assert do
      @table.equal_metadata(Arrow::Table.load(@output.path), false)
    end
  end

  def test_save_load_dictionary
    @table.save(@output.path, dictionary: false)
    assert do
      @table.equal_metadata(Arrow::Table.load(@output.path), false)
    end
  end

  def test_save_load_dictionary_path
    @table.save(@output.path, dictionary: [["label", false]])
    assert do
      @table.equal_metadata(Arrow::Table.load(@output.path), false)
    end
  end
end
