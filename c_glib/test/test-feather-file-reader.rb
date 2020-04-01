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

class TestFeatherFileReader < Test::Unit::TestCase
  include Helper::Buildable

  def setup_file(table)
    tempfile = Tempfile.open("arrow-feather-file-reader")
    output = Arrow::FileOutputStream.new(tempfile.path, false)
    begin
      table.write_as_feather(output)
    ensure
      output.close
    end

    input = Arrow::MemoryMappedInputStream.new(tempfile.path)
    begin
      reader = Arrow::FeatherFileReader.new(input)
      yield(reader)
    ensure
      input.close
    end
  end

  test("#read") do
    table = build_table("message" => build_string_array(["Login"]),
                        "is_critical" => build_boolean_array([true]))
    setup_file(table) do |reader|
      assert do
        reader.version >= 2
      end
      assert_equal(table, reader.read)
    end
  end

  test("#read_indices") do
    table = build_table("message" => build_string_array(["Login"]),
                        "is_critical" => build_boolean_array([true]),
                        "host" => build_string_array(["www"]))
    setup_file(table) do |reader|
      assert_equal(build_table("message" => build_string_array(["Login"]),
                               "host" => build_string_array(["www"])),
                   reader.read_indices([2, 0]))
    end
  end

  test("#read_names") do
    table = build_table("message" => build_string_array(["Login"]),
                        "is_critical" => build_boolean_array([true]),
                        "host" => build_string_array(["www"]))
    setup_file(table) do |reader|
      assert_equal(build_table("message" => build_string_array(["Login"]),
                               "host" => build_string_array(["www"])),
                   reader.read_names(["message", "host"]))
    end
  end
end
