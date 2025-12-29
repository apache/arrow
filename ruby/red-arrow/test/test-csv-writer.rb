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

class CSVWriterTest < Test::Unit::TestCase
  sub_test_case("CSVWriteOptions") do
    def setup
      @options = Arrow::CSVWriteOptions.new
    end

    def test_delimiter
      assert_equal(",", @options.delimiter)
      @options.delimiter = ";"
      assert_equal(";", @options.delimiter)
    end
  end

  def test_write_table
    table = Arrow::Table.new({
      message: ["Start", nil, "Reboot"],
      count: [2, 9, 5],
    })

    options = Arrow::CSVWriteOptions.new
    options.delimiter = ";"

    buffer = Arrow::ResizableBuffer.new(0)
    Arrow::BufferOutputStream.open(buffer) do |output|
      Arrow::CSVWriter.open(output, table.schema, options) do |csv_writer|
        csv_writer.write_table(table)
      end
    end

    csv_output = buffer.data.to_s
    expected = <<~CSV
      "message";"count"
      "Start";2
      ;9
      "Reboot";5
    CSV
    assert_equal(expected, csv_output)
  end
end
