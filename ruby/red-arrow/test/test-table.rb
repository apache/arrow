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

class TableTest < Test::Unit::TestCase
  include Helper::Fixture

  def setup
    @count_field = Arrow::Field.new("count", :uint8)
    @visible_field = Arrow::Field.new("visible", :boolean)
    schema = Arrow::Schema.new([@count_field, @visible_field])
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
    @count_array = Arrow::ChunkedArray.new(count_arrays)
    @visible_array = Arrow::ChunkedArray.new(visible_arrays)
    @count_column = Arrow::Column.new(@count_field, @count_array)
    @visible_column = Arrow::Column.new(@visible_field, @visible_array)
    @table = Arrow::Table.new(schema, [@count_column, @visible_column])
  end

  test("#columns") do
    assert_equal(["count", "visible"],
                 @table.columns.collect(&:name))
  end

  sub_test_case("#slice") do
    test("Arrow::BooleanArray") do
      target_rows_raw = [nil, true, true, false, true, false, true, true]
      target_rows = Arrow::BooleanArray.new(target_rows_raw)
      assert_equal(<<-TABLE, @table.slice(target_rows).to_s)
	count	visible
0	    2	false  
1	    4	       
2	   16	true   
3	   64	       
4	  128	       
      TABLE
    end

    test("Array: boolean") do
      target_rows_raw = [nil, true, true, false, true, false, true, true]
      assert_equal(<<-TABLE, @table.slice(target_rows_raw).to_s)
	count	visible
0	    2	false  
1	    4	       
2	   16	true   
3	   64	       
4	  128	       
      TABLE
    end

    test("Integer: positive") do
      assert_equal(<<-TABLE, @table.slice(2).to_s)
	count	visible
0	    4	       
      TABLE
    end

    test("Integer: negative") do
      assert_equal(<<-TABLE, @table.slice(-1).to_s)
	count	visible
0	  128	       
      TABLE
    end

    test("Range: positive: include end") do
      assert_equal(<<-TABLE, @table.slice(2..4).to_s)
	count	visible
0	    4	       
1	    8	true   
2	   16	true   
      TABLE
    end

    test("Range: positive: exclude end") do
      assert_equal(<<-TABLE, @table.slice(2...4).to_s)
	count	visible
0	    4	       
1	    8	true   
      TABLE
    end

    test("Range: negative: include end") do
      assert_equal(<<-TABLE, @table.slice(-4..-2).to_s)
	count	visible
0	   16	true   
1	   32	false  
2	   64	       
      TABLE
    end

    test("Range: negative: exclude end") do
      assert_equal(<<-TABLE, @table.slice(-4...-2).to_s)
	count	visible
0	   16	true   
1	   32	false  
      TABLE
    end

    test("[from, to]: positive") do
      assert_equal(<<-TABLE, @table.slice(0, 2).to_s)
	count	visible
0	    1	true   
1	    2	false  
      TABLE
    end

    test("[from, to]: negative") do
      assert_equal(<<-TABLE, @table.slice(-4, 2).to_s)
	count	visible
0	   16	true   
1	   32	false  
      TABLE
    end

    sub_test_case("wrong argument") do
      test("no arguments") do
        message = "wrong number of arguments (given 0, expected 1..2)"
        assert_raise(ArgumentError.new(message)) do
          @table.slice
        end
      end

      test("too many arguments: with block") do
        message = "wrong number of arguments (given 3, expected 1..2)"
        assert_raise(ArgumentError.new(message)) do
          @table.slice(1, 2, 3)
        end
      end

      test("too many arguments: without block") do
        message = "wrong number of arguments (given 3, expected 0..2)"
        assert_raise(ArgumentError.new(message)) do
          @table.slice(1, 2, 3) {}
        end
      end
    end
  end

  sub_test_case("#[]") do
    test("[String]") do
      assert_equal(@count_column, @table["count"])
    end

    test("[Symbol]") do
      assert_equal(@visible_column, @table[:visible])
    end
  end

  sub_test_case("#merge") do
    sub_test_case("Hash") do
      test("add") do
        name_array = Arrow::StringArray.new(["a", "b", "c", "d", "e", "f", "g", "h"])
        assert_equal(<<-TABLE, @table.merge(:name => name_array).to_s)
	count	visible	name
0	    1	true   	a   
1	    2	false  	b   
2	    4	       	c   
3	    8	true   	d   
4	   16	true   	e   
5	   32	false  	f   
6	   64	       	g   
7	  128	       	h   
        TABLE
      end

      test("remove") do
        assert_equal(<<-TABLE, @table.merge(:visible => nil).to_s)
	count
0	    1
1	    2
2	    4
3	    8
4	   16
5	   32
6	   64
7	  128
        TABLE
      end

      test("replace") do
        visible_array = Arrow::Int32Array.new([1] * @visible_array.length)
        assert_equal(<<-TABLE, @table.merge(:visible => visible_array).to_s)
	count	visible
0	    1	      1
1	    2	      1
2	    4	      1
3	    8	      1
4	   16	      1
5	   32	      1
6	   64	      1
7	  128	      1
        TABLE
      end
    end

    sub_test_case("Arrow::Table") do
      test("add") do
        name_array = Arrow::StringArray.new(["a", "b", "c", "d", "e", "f", "g", "h"])
        table = Arrow::Table.new("name" => name_array)
        assert_equal(<<-TABLE, @table.merge(table).to_s)
	count	visible	name
0	    1	true   	a   
1	    2	false  	b   
2	    4	       	c   
3	    8	true   	d   
4	   16	true   	e   
5	   32	false  	f   
6	   64	       	g   
7	  128	       	h   
        TABLE
      end

      test("replace") do
        visible_array = Arrow::Int32Array.new([1] * @visible_array.length)
        table = Arrow::Table.new("visible" => visible_array)
        assert_equal(<<-TABLE, @table.merge(table).to_s)
	count	visible
0	    1	      1
1	    2	      1
2	    4	      1
3	    8	      1
4	   16	      1
5	   32	      1
6	   64	      1
7	  128	      1
        TABLE
      end
    end
  end

  test("column name getter") do
    assert_equal(@visible_column, @table.visible)
  end

  sub_test_case("#remove_column") do
    test("String") do
      assert_equal(<<-TABLE, @table.remove_column("visible").to_s)
	count
0	    1
1	    2
2	    4
3	    8
4	   16
5	   32
6	   64
7	  128
      TABLE
    end

    test("Symbol") do
      assert_equal(<<-TABLE, @table.remove_column(:visible).to_s)
	count
0	    1
1	    2
2	    4
3	    8
4	   16
5	   32
6	   64
7	  128
      TABLE
    end

    test("unknown column name") do
      assert_raise(KeyError) do
        @table.remove_column(:nonexistent)
      end
    end

    test("Integer") do
      assert_equal(<<-TABLE, @table.remove_column(1).to_s)
	count
0	    1
1	    2
2	    4
3	    8
4	   16
5	   32
6	   64
7	  128
      TABLE
    end

    test("negative integer") do
      assert_equal(<<-TABLE, @table.remove_column(-1).to_s)
	count
0	    1
1	    2
2	    4
3	    8
4	   16
5	   32
6	   64
7	  128
      TABLE
    end

    test("too small index") do
      assert_raise(IndexError) do
        @table.remove_column(-3)
      end
    end

    test("too large index") do
      assert_raise(IndexError) do
        @table.remove_column(2)
      end
    end
  end

  sub_test_case("#select_columns") do
    def setup
      raw_table = {
        :a => Arrow::UInt8Array.new([1]),
        :b => Arrow::UInt8Array.new([1]),
        :c => Arrow::UInt8Array.new([1]),
        :d => Arrow::UInt8Array.new([1]),
        :e => Arrow::UInt8Array.new([1]),
      }
      @table = Arrow::Table.new(raw_table)
    end

    test("names") do
      assert_equal(<<-TABLE, @table.select_columns(:c, :a).to_s)
	c	a
0	1	1
      TABLE
    end

    test("range") do
      assert_equal(<<-TABLE, @table.select_columns(2...4).to_s)
	c	d
0	1	1
      TABLE
    end

    test("indexes") do
      assert_equal(<<-TABLE, @table.select_columns(0, -1, 2).to_s)
	a	e	c
0	1	1	1
      TABLE
    end

    test("mixed") do
      assert_equal(<<-TABLE, @table.select_columns(:a, -1, 2..3).to_s)
	a	e	c	d
0	1	1	1	1
      TABLE
    end

    test("block") do
      selected_table = @table.select_columns.with_index do |column, i|
        column.name == "a" or i.odd?
      end
      assert_equal(<<-TABLE, selected_table.to_s)
	a	b	d
0	1	1	1
      TABLE
    end

    test("names, indexes and block") do
      selected_table = @table.select_columns(:a, -1) do |column|
        column.name == "a"
      end
      assert_equal(<<-TABLE, selected_table.to_s)
	a
0	1
      TABLE
    end
  end

  sub_test_case("#save and .load") do
    module SaveLoadFormatTests
      def test_default
        output = create_output(".arrow")
        @table.save(output)
        assert_equal(@table, Arrow::Table.load(output))
      end

      def test_batch
        output = create_output(".arrow")
        @table.save(output, format: :batch)
        assert_equal(@table, Arrow::Table.load(output, format: :batch))
      end

      def test_stream
        output = create_output(".arrow")
        @table.save(output, format: :stream)
        assert_equal(@table, Arrow::Table.load(output, format: :stream))
      end

      def test_csv
        output = create_output(".csv")
        @table.save(output, format: :csv)
        assert_equal(@table,
                     Arrow::Table.load(output,
                                       format: :csv,
                                       schema: @table.schema))
      end

      def test_csv_gz
        output = create_output(".csv.gz")
        @table.save(output,
                    format: :csv,
                    compression: :gzip)
        assert_equal(@table,
                     Arrow::Table.load(output,
                                       format: :csv,
                                       compression: :gzip,
                                       schema: @table.schema))
      end
    end

    sub_test_case("path") do
      sub_test_case(":format") do
        include SaveLoadFormatTests

        def create_output(extension)
          @file = Tempfile.new(["red-arrow", extension])
          @file.path
        end

        sub_test_case("save: auto detect") do
          test("csv") do
            output = create_output(".csv")
            @table.save(output)
            assert_equal(@table,
                         Arrow::Table.load(output,
                                           format: :csv,
                                           schema: @table.schema))
          end

          test("csv.gz") do
            output = create_output(".csv.gz")
            @table.save(output)
            assert_equal(@table,
                         Arrow::Table.load(output,
                                           format: :csv,
                                           compression: :gzip,
                                           schema: @table.schema))
          end
        end

        sub_test_case("load: auto detect") do
          test("batch") do
            output = create_output(".arrow")
            @table.save(output, format: :batch)
            assert_equal(@table, Arrow::Table.load(output))
          end

          test("stream") do
            output = create_output(".arrow")
            @table.save(output, format: :stream)
            assert_equal(@table, Arrow::Table.load(output))
          end

          test("csv") do
            path = fixture_path("with-header.csv")
            table = Arrow::Table.load(path, skip_lines: /^\#/)
            assert_equal(<<-TABLE, table.to_s)
	name	score
0	alice	   10
1	bob 	   29
2	chris	   -1
            TABLE
          end

          test("csv.gz") do
            file = Tempfile.new(["red-arrow", ".csv.gz"])
            Zlib::GzipWriter.wrap(file) do |gz|
              gz.write(<<-CSV)
name,score
alice,10
bob,29
chris,-1
              CSV
            end
            assert_equal(<<-TABLE, Arrow::Table.load(file.path).to_s)
	name	score
0	alice	   10
1	bob 	   29
2	chris	   -1
          TABLE
          end
        end
      end
    end

    sub_test_case("Buffer") do
      sub_test_case(":format") do
        include SaveLoadFormatTests

        def create_output(extension)
          Arrow::ResizableBuffer.new(1024)
        end
      end
    end
  end

  test("#pack") do
    packed_table = @table.pack
    column_n_chunks = packed_table.columns.collect {|c| c.data.n_chunks}
    assert_equal([[1, 1], <<-TABLE], [column_n_chunks, packed_table.to_s])
	count	visible
0	    1	true   
1	    2	false  
2	    4	       
3	    8	true   
4	   16	true   
5	   32	false  
6	   64	       
7	  128	       
    TABLE
  end

  sub_test_case("#to_s") do
    sub_test_case(":format") do
      def setup
        columns = {
          "count" => Arrow::UInt8Array.new([1, 2]),
          "visible" => Arrow::BooleanArray.new([true, false]),
        }
        @table = Arrow::Table.new(columns)
      end

      test(":column") do
        assert_equal(<<-TABLE, @table.to_s(format: :column))
count: uint8
visible: bool
----
count:
  [
    [
      1,
      2
    ]
  ]
visible:
  [
    [
      true,
      false
    ]
  ]
        TABLE
      end

      test(":list") do
        assert_equal(<<-TABLE, @table.to_s(format: :list))
==================== 0 ====================
count: 1
visible: true
==================== 1 ====================
count: 2
visible: false
        TABLE
      end

      test(":table") do
        assert_equal(<<-TABLE, @table.to_s(format: :table))
	count	visible
0	    1	true   
1	    2	false  
        TABLE
      end

      test("invalid") do
        message = ":format must be :column, :list, :table or nil: <:invalid>"
        assert_raise(ArgumentError.new(message)) do
          @table.to_s(format: :invalid)
        end
      end
    end

    sub_test_case("#==") do
      test("Arrow::Table") do
        assert do
          @table == @table
        end
      end

      test("not Arrow::Table") do
        assert do
          not (@table == 29)
        end
      end
    end
  end
end
