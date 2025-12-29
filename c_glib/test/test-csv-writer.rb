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

class TestCSVWriter < Test::Unit::TestCase
  include Helper::Buildable

  def test_write_record_batch
    message_data = ["Start", "Shutdown"]
    count_data = [2, 9]
    message_field = Arrow::Field.new("message", Arrow::StringDataType.new)
    count_field = Arrow::Field.new("count", Arrow::Int64DataType.new)
    schema = Arrow::Schema.new([message_field, count_field])

    buffer = Arrow::ResizableBuffer.new(0)
    output = Arrow::BufferOutputStream.new(buffer)
    begin
      csv_writer = Arrow::CSVWriter.new(output, schema, nil)
      begin
        record_batch = Arrow::RecordBatch.new(schema,
                                              message_data.size,
                                              [
                                                build_string_array(message_data),
                                                build_int64_array(count_data),
                                              ])
        csv_writer.write_record_batch(record_batch)
      ensure
        csv_writer.close
        assert do
          csv_writer.closed?
        end
      end
    ensure
      output.close
    end

    csv_output = buffer.data.to_s
    expected = <<~CSV
      "message","count"
      "Start",2
      "Shutdown",9
    CSV
    assert_equal(expected, csv_output)
  end

  def test_write_table
    message_data = ["Start", "Shutdown", "Reboot"]
    count_data = [2, 9, 5]
    message_field = Arrow::Field.new("message", Arrow::StringDataType.new)
    count_field = Arrow::Field.new("count", Arrow::Int64DataType.new)
    schema = Arrow::Schema.new([message_field, count_field])

    buffer = Arrow::ResizableBuffer.new(0)
    output = Arrow::BufferOutputStream.new(buffer)
    begin
      csv_writer = Arrow::CSVWriter.new(output, schema, nil)
      begin
        table = Arrow::Table.new(schema,
                                 [
                                   build_string_array(message_data),
                                   build_int64_array(count_data),
                                 ])
        csv_writer.write_table(table)
      ensure
        csv_writer.close
        assert do
          csv_writer.closed?
        end
      end
    ensure
      output.close
    end

    csv_output = buffer.data.to_s
    expected = <<~CSV
      "message","count"
      "Start",2
      "Shutdown",9
      "Reboot",5
    CSV
    assert_equal(expected, csv_output)
  end


  sub_test_case("options") do
    def setup
      @options = Arrow::CSVWriteOptions.new
    end

    def test_include_header
      assert do
        @options.include_header?
      end
      @options.include_header = false
      assert do
        not @options.include_header?
      end
    end

    def test_batch_size
      assert_equal(1024, @options.batch_size)
      @options.batch_size = 2048
      assert_equal(2048, @options.batch_size)
    end

    def test_delimiter
      assert_equal(44, @options.delimiter) # 44 is the ASCII code for comma
      @options.delimiter = ";".ord
      assert_equal(59, @options.delimiter) # 59 is the ASCII code for semicolon
    end

    def test_null_string
      assert_equal("", @options.null_string)
      @options.null_string = "NULL"
      assert_equal("NULL", @options.null_string)
    end

    def test_eol
      assert_equal("\n", @options.eol)
      @options.eol = "\r\n"
      assert_equal("\r\n", @options.eol)
    end

    def test_quoting_style
      assert_equal(Arrow::CSVQuotingStyle::NEEDED, @options.quoting_style)
      @options.quoting_style = Arrow::CSVQuotingStyle::ALL_VALID
      assert_equal(Arrow::CSVQuotingStyle::ALL_VALID, @options.quoting_style)
    end

    def test_quoting_header
      assert_equal(Arrow::CSVQuotingStyle::NEEDED, @options.quoting_header)
      @options.quoting_header = Arrow::CSVQuotingStyle::NONE
      assert_equal(Arrow::CSVQuotingStyle::NONE, @options.quoting_header)
    end

    def test_write_with_options
      message_data = ["Start", nil, "Reboot"]
      count_data = [2, 9, 5]
      message_field = Arrow::Field.new("message", Arrow::StringDataType.new)
      count_field = Arrow::Field.new("count", Arrow::Int64DataType.new)
      schema = Arrow::Schema.new([message_field, count_field])

      options = Arrow::CSVWriteOptions.new
      options.include_header = false
      options.delimiter = ";".ord
      options.quoting_style = Arrow::CSVQuotingStyle::NONE
      options.null_string = "NULL"

      buffer = Arrow::ResizableBuffer.new(0)
      output = Arrow::BufferOutputStream.new(buffer)
      begin
        csv_writer = Arrow::CSVWriter.new(output, schema, options)
        begin
          record_batch = Arrow::RecordBatch.new(schema,
                                                message_data.size,
                                                [
                                                  build_string_array(message_data),
                                                  build_int64_array(count_data),
                                                ])
          csv_writer.write_record_batch(record_batch)
        ensure
          csv_writer.close
        end
      ensure
        output.close
      end

      csv_output = buffer.data.to_s
      expected = <<~CSV
        Start;2
        NULL;9
        Reboot;5
      CSV
      assert_equal(expected, csv_output)
    end
  end
end
