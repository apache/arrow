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

class TestFeatherFileWriter < Test::Unit::TestCase
  include Helper::Buildable

  def test_append
    tempfile = Tempfile.open("arrow-feather-file-writer")
    output = Arrow::FileOutputStream.new(tempfile.path, false)
    begin
      writer = Arrow::FeatherFileWriter.new(output)
      begin
        writer.description = "Log"
        writer.n_rows = 3
        writer.append("message",
                      build_string_array(["Crash", "Error", "Shutdown"]))
        writer.append("is_critical",
                      build_boolean_array([true, true, false]))
      ensure
        writer.close
      end
    ensure
      output.close
    end

    input = Arrow::MemoryMappedInputStream.new(tempfile.path)
    begin
      reader = Arrow::FeatherFileReader.new(input)
      columns = reader.n_columns.times.collect do |i|
        [
          reader.get_column_name(i),
          reader.get_column_data(i).get_chunk(0),
        ]
      end
      assert_equal([
                     true,
                     "Log",
                     [
                       [
                         "message",
                         build_string_array(["Crash", "Error", "Shutdown"]),
                       ],
                       [
                         "is_critical",
                         build_boolean_array([true, true, false]),
                       ],
                     ],
                   ],
                   [
                     reader.has_description?,
                     reader.description,
                     columns,
                   ])
    ensure
      input.close
    end
  end

  def test_write
    messages = build_string_array(["Crash", "Error", "Shutdown"])
    is_criticals = build_boolean_array([true, true, false])
    table = build_table("message" => messages,
                        "is_critical" => is_criticals)

    tempfile = Tempfile.open("arrow-feather-file-writer")

    output = Arrow::FileOutputStream.new(tempfile.path, false)
    writer = Arrow::FeatherFileWriter.new(output)
    writer.n_rows = table.n_rows
    writer.write(table)
    writer.close
    output.close

    input = Arrow::MemoryMappedInputStream.new(tempfile.path)
    reader = Arrow::FeatherFileReader.new(input)
    assert_equal([table.n_rows, table],
                 [reader.n_rows, reader.read])
    input.close
  end
end
