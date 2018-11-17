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

  def setup_file(data)
    tempfile = Tempfile.open("arrow-feather-file-reader")
    output = Arrow::FileOutputStream.new(tempfile.path, false)
    begin
      writer = Arrow::FeatherFileWriter.new(output)
      begin
        if data[:description]
          writer.description = data[:description]
        end
        writer.n_rows = data[:n_rows] || 0
        if data[:table]
          writer.write(data[:table])
        elsif data[:columns]
          data[:columns].each do |name, array|
            writer.append(name, array)
          end
        end
      ensure
        writer.close
      end
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

  sub_test_case("#description") do
    test("exist") do
      setup_file(:description => "Log") do |reader|
        assert_equal("Log", reader.description)
      end
    end

    test("not exist") do
      setup_file(:description => nil) do |reader|
        assert_nil(reader.description)
      end
    end
  end

  sub_test_case("#has_description?") do
    test("exist") do
      setup_file(:description => "Log") do |reader|
        assert do
          reader.has_description?
        end
      end
    end

    test("not exist") do
      setup_file(:description => nil) do |reader|
        assert do
          not reader.has_description?
        end
      end
    end
  end

  test("#version") do
    setup_file({}) do |reader|
      assert do
        reader.version >= 2
      end
    end
  end

  test("#n_rows") do
    setup_file(:n_rows => 3) do |reader|
      assert_equal(3, reader.n_rows)
    end
  end

  test("#n_columns") do
    columns = {
      "message" => build_string_array([]),
      "is_critical" => build_boolean_array([]),
    }
    setup_file(:columns => columns) do |reader|
      assert_equal(2, reader.n_columns)
    end
  end

  test("#get_column_name") do
    columns = {
      "message" => build_string_array([]),
      "is_critical" => build_boolean_array([]),
    }
    setup_file(:columns => columns) do |reader|
      assert_equal([
                     "message",
                     "is_critical",
                   ],
                   [
                     reader.get_column_name(0),
                     reader.get_column_name(1),
                   ])
    end
  end

  test("#get_column") do
    columns = {
      "message" => build_string_array([]),
      "is_critical" => build_boolean_array([]),
    }
    setup_file(:columns => columns) do |reader|
      assert_equal([
                     "message",
                     "is_critical",
                   ],
                   [
                     reader.get_column(0).name,
                     reader.get_column(1).name,
                   ])
    end
  end

  test("#columns") do
    columns = {
      "message" => build_string_array([]),
      "is_critical" => build_boolean_array([]),
    }
    setup_file(:columns => columns) do |reader|
      assert_equal([
                     "message",
                     "is_critical",
                   ],
                   reader.columns.collect(&:name))
    end
  end

  test("#read") do
    table = build_table("message" => build_string_array(["Login"]),
                        "is_critical" => build_boolean_array([true]))
    setup_file(:table => table) do |reader|
      assert_equal(table, reader.read)
    end
  end

  test("#read_indices") do
    table = build_table("message" => build_string_array(["Login"]),
                        "is_critical" => build_boolean_array([true]),
                        "host" => build_string_array(["www"]))
    setup_file(:table => table) do |reader|
      assert_equal(build_table("message" => build_string_array(["Login"]),
                               "host" => build_string_array(["www"])),
                   reader.read_indices([2, 0]))
    end
  end

  test("#read_names") do
    table = build_table("message" => build_string_array(["Login"]),
                        "is_critical" => build_boolean_array([true]),
                        "host" => build_string_array(["www"]))
    setup_file(:table => table) do |reader|
      assert_equal(build_table("message" => build_string_array(["Login"]),
                               "host" => build_string_array(["www"])),
                   reader.read_names(["host", "message"]))
    end
  end
end
