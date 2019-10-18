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

class TestCSVReader < Test::Unit::TestCase
  include Helper::Buildable
  include Helper::Omittable

  sub_test_case("#read") do
    def open_input(csv)
      buffer = Arrow::Buffer.new(csv)
      Arrow::BufferInputStream.new(buffer)
    end

    def test_default
      require_gi(1, 42, 0)
      table = Arrow::CSVReader.new(open_input(<<-CSV))
message,count
"Start",2
"Shutdown",9
      CSV
      columns = {
        "message" => build_string_array(["Start", "Shutdown"]),
        "count" => build_int64_array([2, 9]),
      }
      assert_equal(build_table(columns),
                   table.read)
    end

    sub_test_case("options") do
      def test_add_column_type
        options = Arrow::CSVReadOptions.new
        options.add_column_type("count", Arrow::UInt8DataType.new)
        options.add_column_type("valid", Arrow::BooleanDataType.new)
        table = Arrow::CSVReader.new(open_input(<<-CSV), options)
count,valid
2,1
9,0
        CSV
        columns = {
          "count" => build_uint8_array([2, 9]),
          "valid" => build_boolean_array([true, false]),
        }
        assert_equal(build_table(columns),
                     table.read)
      end

      def test_add_schema
        options = Arrow::CSVReadOptions.new
        fields = [
          Arrow::Field.new("count", Arrow::UInt8DataType.new),
          Arrow::Field.new("valid", Arrow::BooleanDataType.new),
        ]
        schema = Arrow::Schema.new(fields)
        options.add_schema(schema)
        table = Arrow::CSVReader.new(open_input(<<-CSV), options)
count,valid
2,1
9,0
        CSV
        columns = {
          "count" => build_uint8_array([2, 9]),
          "valid" => build_boolean_array([true, false]),
        }
        assert_equal(build_table(columns),
                     table.read)
      end

      def test_column_types
        require_gi_bindings(3, 3, 1)
        options = Arrow::CSVReadOptions.new
        options.add_column_type("count", Arrow::UInt8DataType.new)
        options.add_column_type("valid", Arrow::BooleanDataType.new)
        assert_equal({
                       "count" => Arrow::UInt8DataType.new,
                       "valid" => Arrow::BooleanDataType.new,
                     },
                     options.column_types)
      end

      def test_null_values
        options = Arrow::CSVReadOptions.new
        null_values = ["2", "5"]
        options.null_values = null_values
        assert_equal(null_values, options.null_values)

        table = Arrow::CSVReader.new(open_input(<<-CSV), options)
message,count
"Start",2
"Shutdown",9
"Restart",5
        CSV
        columns = {
          "message" => build_string_array(["Start", "Shutdown", "Restart"]),
          "count" => build_int64_array([nil, 9, nil]),
        }
        assert_equal(build_table(columns),
                     table.read)
      end

      def test_add_null_value
        options = Arrow::CSVReadOptions.new
        null_values = ["2", "5"]
        options.null_values = null_values
        options.add_null_value("9")
        assert_equal(null_values + ["9"], options.null_values)
      end

      def test_boolean_values
        options = Arrow::CSVReadOptions.new
        true_values = ["Start", "Restart"]
        options.true_values = true_values
        assert_equal(true_values, options.true_values)

        false_values = ["Shutdown"]
        options.false_values = false_values
        assert_equal(false_values, options.false_values)

        table = Arrow::CSVReader.new(open_input(<<-CSV), options)
message,count
"Start",2
"Shutdown",9
"Restart",5
        CSV
        columns = {
          "message" => build_boolean_array([true, false, true]),
          "count" => build_int64_array([2, 9, 5]),
        }
        assert_equal(build_table(columns),
                     table.read)
      end

      def test_add_true_value
        options = Arrow::CSVReadOptions.new
        true_values = ["Start", "Restart"]
        options.true_values = true_values
        options.add_true_value("Shutdown")
        assert_equal(true_values + ["Shutdown"], options.true_values)
      end

      def test_add_false_value
        options = Arrow::CSVReadOptions.new
        false_values = ["Start", "Restart"]
        options.false_values = false_values
        options.add_false_value("Shutdown")
        assert_equal(false_values + ["Shutdown"], options.false_values)
      end

      def test_allow_null_strings
        options = Arrow::CSVReadOptions.new
        options.null_values = ["Start", "Restart"]
        options.allow_null_strings = true
        table = Arrow::CSVReader.new(open_input(<<-CSV), options)
message,count
"Start",2
"Shutdown",9
"Restart",5
        CSV
        columns = {
          "message" => build_string_array([nil, "Shutdown", nil]),
          "count" => build_int64_array([2, 9, 5]),
        }
        assert_equal(build_table(columns),
                     table.read)
      end

      def test_n_skip_rows
        options = Arrow::CSVReadOptions.new
        options.n_skip_rows = 1
        table = Arrow::CSVReader.new(open_input(<<-CSV), options)
message1,message2
"Start1","Start2"
"Shutdown1","Shutdown2"
"Reboot1","Reboot2"
        CSV
        columns = {
          "Start1" => build_string_array(["Shutdown1", "Reboot1"]),
          "Start2" => build_string_array(["Shutdown2", "Reboot2"]),
        }
        assert_equal(build_table(columns),
                     table.read)
      end

      def test_column_names
        options = Arrow::CSVReadOptions.new
        column_names = ["message", "count"]
        options.column_names = column_names
        assert_equal(column_names, options.column_names)

        table = Arrow::CSVReader.new(open_input(<<-CSV), options)
"Start",2
"Shutdown",9
"Reboot",5
        CSV
        columns = {
          "message" => build_string_array(["Start", "Shutdown", "Reboot"]),
          "count" => build_int64_array([2, 9, 5]),
        }
        assert_equal(build_table(columns),
                     table.read)
      end

      def test_add_column_name
        options = Arrow::CSVReadOptions.new
        column_names = ["message", "count"]
        options.column_names = column_names
        options.add_column_name("score")
        assert_equal(column_names + ["score"], options.column_names)
      end

      def test_generate_column_names
        options = Arrow::CSVReadOptions.new
        options.generate_column_names = true

        table = Arrow::CSVReader.new(open_input(<<-CSV), options)
"Start",2
"Shutdown",9
"Reboot",5
        CSV
        columns = {
          "f0" => build_string_array(["Start", "Shutdown", "Reboot"]),
          "f1" => build_int64_array([2, 9, 5]),
        }
        assert_equal(build_table(columns),
                     table.read)
      end
    end
  end
end
