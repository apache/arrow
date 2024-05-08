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

class CSVLoaderTest < Test::Unit::TestCase
  include Helper::Fixture
  include Helper::Omittable

  def load_csv(input)
    Arrow::CSVLoader.load(input, skip_lines: /^#/)
  end

  sub_test_case(".load") do
    test("String: data: with header") do
      data = fixture_path("with-header-float.csv").read
      assert_equal(<<-TABLE, load_csv(data).to_s)
	  name	     score
	(utf8)	  (double)
0	alice 	 10.100000
1	bob   	 29.200000
2	chris 	 -1.300000
      TABLE
    end

    test("String: data: without header") do
      data = fixture_path("without-header-float.csv").read
      assert_equal(<<-TABLE, load_csv(data).to_s)
	     0	         1
	(utf8)	  (double)
0	alice 	 10.100000
1	bob   	 29.200000
2	chris 	 -1.300000
      TABLE
    end

    test("String: path: with header") do
      path = fixture_path("with-header-float.csv").to_s
      assert_equal(<<-TABLE, load_csv(path).to_s)
	  name	     score
	(utf8)	  (double)
0	alice 	 10.100000
1	bob   	 29.200000
2	chris 	 -1.300000
      TABLE
    end

    test("String: path: without header") do
      path = fixture_path("without-header-float.csv").to_s
      assert_equal(<<-TABLE, load_csv(path).to_s)
	     0	         1
	(utf8)	  (double)
0	alice 	 10.100000
1	bob   	 29.200000
2	chris 	 -1.300000
      TABLE
    end

    test("Pathname: with header") do
      path = fixture_path("with-header-float.csv")
      assert_equal(<<-TABLE, load_csv(path).to_s)
	  name	     score
	(utf8)	  (double)
0	alice 	 10.100000
1	bob   	 29.200000
2	chris 	 -1.300000
      TABLE
    end

    test("Pathname: without header") do
      path = fixture_path("without-header-float.csv")
      assert_equal(<<-TABLE, load_csv(path).to_s)
	     0	         1
	(utf8)	  (double)
0	alice 	 10.100000
1	bob   	 29.200000
2	chris 	 -1.300000
      TABLE
    end

    test("null: with double quote") do
      path = fixture_path("null-with-double-quote.csv").to_s
      assert_equal(<<-TABLE, load_csv(path).to_s)
	  name	 score
	(utf8)	(int8)
0	alice 	    10
1	bob   	(null)
2	chris 	    -1
      TABLE
    end

    test("null: without double quote") do
      path = fixture_path("null-without-double-quote.csv").to_s
      assert_equal(<<-TABLE, load_csv(path).to_s)
	  name	 score
	(utf8)	(int8)
0	alice 	    10
1	bob   	(null)
2	chris 	    -1
      TABLE
    end

    test("number: float, integer") do
      path = fixture_path("float-integer.csv").to_s
      assert_equal([2.9, 10, -1.1],
                   load_csv(path)[:score].to_a)
    end

    test("number: integer, float") do
      path = fixture_path("integer-float.csv").to_s
      assert_equal([10.0, 2.9, -1.1],
                   load_csv(path)[:score].to_a)
    end
  end

  sub_test_case("CSVReader") do
    def load_csv(data, **options)
      Arrow::CSVLoader.load(data, **options)
    end

    sub_test_case(":headers") do
      test("true") do
        values = Arrow::StringArray.new(["a", "b", "c"])
        assert_equal(Arrow::Table.new(value: values),
                     load_csv(<<-CSV, headers: true))
value
a
b
c
                     CSV
      end

      test(":first_line") do
        values = Arrow::StringArray.new(["a", "b", "c"])
        assert_equal(Arrow::Table.new(value: values),
                     load_csv(<<-CSV, headers: :first_line))
value
a
b
c
                     CSV
      end

      test("truthy") do
        values = Arrow::StringArray.new(["a", "b", "c"])
        assert_equal(Arrow::Table.new(value: values),
                     load_csv(<<-CSV, headers: 0))
value
a
b
c
                     CSV
      end

      test("Array of column names") do
        values = Arrow::StringArray.new(["a", "b", "c"])
        assert_equal(Arrow::Table.new(column: values),
                     load_csv(<<-CSV, headers: ["column"]))
a
b
c
                     CSV
      end

      test("false") do
        values = Arrow::StringArray.new(["a", "b", "c"])
        assert_equal(Arrow::Table.new(f0: values),
                     load_csv(<<-CSV, headers: false))
a
b
c
                     CSV
      end

      test("nil") do
        values = Arrow::StringArray.new(["a", "b", "c"])
        assert_equal(Arrow::Table.new(f0: values),
                     load_csv(<<-CSV, headers: nil))
a
b
c
                     CSV
      end

      test("string") do
        values = Arrow::StringArray.new(["a", "b", "c"])
        assert_equal(Arrow::Table.new(column: values),
                     load_csv(<<-CSV, headers: "column"))
a
b
c
                     CSV
      end
    end

    test(":column_types") do
      assert_equal(Arrow::Table.new(:count => Arrow::UInt16Array.new([1, 2, 4])),
                   load_csv(<<-CSV, column_types: {count: :uint16}))
count
1
2
4
                   CSV
    end

    test(":schema") do
      table = Arrow::Table.new(:count => Arrow::UInt16Array.new([1, 2, 4]))
      assert_equal(table,
                   load_csv(<<-CSV, schema: table.schema))
count
1
2
4
                   CSV
    end

    test(":encoding") do
      messages = [
        "\u3042", # U+3042 HIRAGANA LETTER A
        "\u3044", # U+3044 HIRAGANA LETTER I
        "\u3046", # U+3046 HIRAGANA LETTER U
      ]
      table = Arrow::Table.new(:message => Arrow::StringArray.new(messages))
      encoding = "cp932"
      assert_equal(table,
                   load_csv((["message"] + messages).join("\n").encode(encoding),
                            schema: table.schema,
                            encoding: encoding))
    end

    test(":encoding and :compression") do
      messages = [
        "\u3042", # U+3042 HIRAGANA LETTER A
        "\u3044", # U+3044 HIRAGANA LETTER I
        "\u3046", # U+3046 HIRAGANA LETTER U
      ]
      table = Arrow::Table.new(:message => Arrow::StringArray.new(messages))
      encoding = "cp932"
      csv = (["message"] + messages).join("\n").encode(encoding)
      assert_equal(table,
                   load_csv(Zlib::Deflate.deflate(csv),
                            schema: table.schema,
                            encoding: encoding,
                            compression: :gzip))
    end

    sub_test_case(":timestamp_parsers") do
      test(":iso8601") do
        require_glib(2, 58, 0)
        data_type = Arrow::TimestampDataType.new(:second,
                                                 GLib::TimeZone.new("UTC"))
        timestamps = [
          Time.iso8601("2024-03-16T23:54:12Z"),
          Time.iso8601("2024-03-16T23:54:13Z"),
          Time.iso8601("2024-03-16T23:54:14Z"),
        ]
        values = Arrow::TimestampArray.new(data_type, timestamps)
        assert_equal(Arrow::Table.new(value: values),
                     load_csv(<<-CSV, headers: true, timestamp_parsers: [:iso8601]))
value
#{timestamps[0].iso8601}
#{timestamps[1].iso8601}
#{timestamps[2].iso8601}
                     CSV
      end

      test("String") do
        timestamps = [
          Time.iso8601("2024-03-16T23:54:12Z"),
          Time.iso8601("2024-03-16T23:54:13Z"),
          Time.iso8601("2024-03-16T23:54:14Z"),
        ]
        values = Arrow::TimestampArray.new(:second, timestamps)
        format = "%Y-%m-%dT%H:%M:%S"
        assert_equal(Arrow::Table.new(value: values).schema,
                     load_csv(<<-CSV, headers: true, timestamp_parsers: [format]).schema)
value
#{timestamps[0].iso8601.chomp("Z")}
#{timestamps[1].iso8601.chomp("Z")}
#{timestamps[2].iso8601.chomp("Z")}
                     CSV
      end
    end
  end
end
