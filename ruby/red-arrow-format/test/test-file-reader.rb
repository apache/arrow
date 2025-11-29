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

class TestFileReader < Test::Unit::TestCase
  def setup
    Dir.mktmpdir do |tmp_dir|
      table = Arrow::Table.new(uint8: Arrow::UInt8Array.new([1, 2, 3]))
      @path = File.join(tmp_dir, "data.arrow")
      table.save(@path)
      File.open(@path, "rb") do |input|
        @reader = ArrowFormat::FileReader.new(input)
        yield
        @reader = nil
      end
      GC.start
    end
  end

  def read
    @reader.to_a.collect do |record_batch|
      record_batch.to_h.tap do |hash|
        hash.each do |key, value|
          hash[key] = value.to_a
        end
      end
    end
  end

  def test_uint8
    assert_equal([
                   {"uint8" => [1, 2, 3]},
                 ],
                 read)
  end
end
