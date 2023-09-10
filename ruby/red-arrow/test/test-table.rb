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
    @table = Arrow::Table.new(schema, [@count_array, @visible_array])
  end

  sub_test_case(".new") do
    test("{Symbol: Arrow::Array}") do
      schema = Arrow::Schema.new(numbers: :int64)
      assert_equal(Arrow::Table.new(schema,
                                    [Arrow::Int64Array.new([1, 2, 3])]),
                   Arrow::Table.new(numbers: Arrow::Int64Array.new([1, 2, 3])))
    end

    test("{Symbol: Arrow::ChunkedArray}") do
      chunked_array = Arrow::ChunkedArray.new([Arrow::Int64Array.new([1, 2, 3])])
      schema = Arrow::Schema.new(numbers: :int64)
      assert_equal(Arrow::Table.new(schema,
                                    [Arrow::Int64Array.new([1, 2, 3])]),
                   Arrow::Table.new(numbers: chunked_array))
    end

    test("{Symbol: Arrow::Tensor}") do
      schema = Arrow::Schema.new(numbers: :uint8)
      assert_equal(Arrow::Table.new(schema,
                                    [Arrow::UInt8Array.new([1, 2, 3])]),
                   Arrow::Table.new(numbers: Arrow::Tensor.new([1, 2, 3])))
    end

    test("{Symbol: #to_ary}") do
      array_like = Object.new
      def array_like.to_ary
        [1, 2, 3]
      end
      schema = Arrow::Schema.new(numbers: :uint8)
      assert_equal(Arrow::Table.new(schema, [Arrow::UInt8Array.new([1, 2, 3])]),
                   Arrow::Table.new(numbers: array_like))
    end
  end

  test("#columns") do
    assert_equal([
                   Arrow::Column.new(@table, 0),
                   Arrow::Column.new(@table, 1),
                 ],
                 @table.columns)
  end

  sub_test_case("#slice") do
    test("Arrow::BooleanArray") do
      target_rows_raw = [nil, true, true, false, true, false, true, true]
      target_rows = Arrow::BooleanArray.new(target_rows_raw)
      assert_equal(<<-TABLE, @table.slice(target_rows).to_s)
	count	visible
0	    2	false  
1	    4	 (null)
2	   16	true   
3	   64	 (null)
4	  128	 (null)
      TABLE
    end

    test("Array: boolean") do
      target_rows_raw = [nil, true, true, false, true, false, true, true]
      assert_equal(<<-TABLE, @table.slice(target_rows_raw).to_s)
	count	visible
0	    2	false  
1	    4	 (null)
2	   16	true   
3	   64	 (null)
4	  128	 (null)
      TABLE
    end

    test("Integer: positive") do
      assert_equal({"count" => 128, "visible" => nil},
                   @table.slice(@table.n_rows - 1).to_h)
    end

    test("Integer: negative") do
      assert_equal({"count" => 1, "visible" => true},
                   @table.slice(-@table.n_rows).to_h)
    end

    test("Integer: out of index") do
      assert_equal([
                     nil,
                     nil,
                   ],
                   [
                     @table.slice(@table.n_rows),
                     @table.slice(-(@table.n_rows + 1)),
                   ])
    end

    test("Range: positive: include end") do
      assert_equal(<<-TABLE, @table.slice(2..4).to_s)
	count	visible
0	    4	 (null)
1	    8	true   
2	   16	true   
      TABLE
    end

    test("Range: positive: exclude end") do
      assert_equal(<<-TABLE, @table.slice(2...4).to_s)
	count	visible
0	    4	 (null)
1	    8	true   
      TABLE
    end

    test("Range: negative: include end") do
      assert_equal(<<-TABLE, @table.slice(-4..-2).to_s)
	count	visible
0	   16	true   
1	   32	false  
2	   64	 (null)
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

    test("{key: Number}") do
      assert_equal(<<-TABLE, @table.slice(count: 16).to_s)
	count	visible
0	   16	true   
      TABLE
    end

    test("{key: String}") do
      table = Arrow::Table.new(name: Arrow::StringArray.new(["a", "b", "c"]))
      assert_equal(<<-TABLE, table.slice(name: 'b').to_s)
	name
0	b   
      TABLE
    end

    test("{key: true}") do
      assert_equal(<<-TABLE, @table.slice(visible: true).to_s)
	count	visible
0	    1	true   
1	    8	true   
2	   16	true   
      TABLE
    end

    test("{key: false}") do
      assert_equal(<<-TABLE, @table.slice(visible: false).to_s)
	count	visible
0	    2	false  
1	   32	false  
      TABLE
    end

    test("{key: Range}: beginless include end") do
      begin
        range = eval("..8")
      rescue SyntaxError
        omit("beginless range isn't supported")
      end
      assert_equal(<<-TABLE, @table.slice(count: range).to_s)
	count	visible
0	    1	true   
1	    2	false  
2	    4	 (null)
3	    8	true   
      TABLE
    end

    test("{key: Range}: beginless exclude end") do
      begin
        range = eval("...8")
      rescue SyntaxError
        omit("beginless range isn't supported")
      end
      assert_equal(<<-TABLE, @table.slice(count: range).to_s)
	count	visible
0	    1	true   
1	    2	false  
2	    4	 (null)
      TABLE
    end

    test("{key: Range}: endless") do
      begin
        range = eval("16..")
      rescue SyntaxError
        omit("endless range isn't supported")
      end
      assert_equal(<<-TABLE, @table.slice(count: range).to_s)
	count	visible
0	   16	true   
1	   32	false  
2	   64	 (null)
3	  128	 (null)
      TABLE
    end

    test("{key: Range}: include end") do
      assert_equal(<<-TABLE, @table.slice(count: 1..16).to_s)
	count	visible
0	    1	true   
1	    2	false  
2	    4	 (null)
3	    8	true   
4	   16	true   
      TABLE
    end

    test("{key: Range}: exclude end") do
      assert_equal(<<-TABLE, @table.slice(count: 1...16).to_s)
	count	visible
0	    1	true   
1	    2	false  
2	    4	 (null)
3	    8	true   
      TABLE
    end

    test("{key1: Range, key2: true}") do
      assert_equal(<<-TABLE, @table.slice(count: 0..8, visible: false).to_s)
	count	visible
0	    2	false  
      TABLE
    end

    sub_test_case("wrong argument") do
      test("no arguments") do
        message = "wrong number of arguments (given 0, expected 1..2)"
        assert_raise(ArgumentError.new(message)) do
          @table.slice
        end
      end

      test("too many arguments") do
        message = "wrong number of arguments (given 3, expected 1..2)"
        assert_raise(ArgumentError.new(message)) do
          @table.slice(1, 2, 3)
        end
      end

      test("arguments: with block") do
        message = "must not specify both arguments and block"
        assert_raise(ArgumentError.new(message)) do
          @table.slice(1, 2) {}
        end
      end

      test("offset: too small") do
        n_rows = @table.n_rows
        offset = -(n_rows + 1)
        message = "offset is out of range (-#{n_rows + 1},#{n_rows}): #{offset}"
        assert_raise(ArgumentError.new(message)) do
          @table.slice(offset, 1)
        end
      end

      test("offset: too large") do
        n_rows = @table.n_rows
        offset = n_rows
        message = "offset is out of range (-#{n_rows + 1},#{n_rows}): #{offset}"
        assert_raise(ArgumentError.new(message)) do
          @table.slice(offset, 1)
        end
      end
    end
  end

  sub_test_case("#[]") do
    def setup
      @table = Arrow::Table.new(a: [true],
                                b: [true],
                                c: [true],
                                d: [true],
                                e: [true],
                                f: [true],
                                g: [true])
    end

    test("[String]") do
      assert_equal(Arrow::Column.new(@table, 0),
                   @table["a"])
    end

    test("[Symbol]") do
      assert_equal(Arrow::Column.new(@table, 1),
                   @table[:b])
    end

    test("[Integer]") do
      assert_equal(Arrow::Column.new(@table, 6),
                   @table[-1])
    end

    test("[Range]") do
      assert_equal(Arrow::Table.new(d: [true],
                                    e: [true]),
                   @table[3..4])
    end

    test("[[Symbol, String, Integer, Range]]") do
      assert_equal(Arrow::Table.new(c: [true],
                                    a: [true],
                                    g: [true],
                                    d: [true],
                                    e: [true]),
                   @table[[:c, "a", -1, 3..4]])
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
2	    4	 (null)	c   
3	    8	true   	d   
4	   16	true   	e   
5	   32	false  	f   
6	   64	 (null)	g   
7	  128	 (null)	h   
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
2	    4	 (null)	c   
3	    8	true   	d   
4	   16	true   	e   
5	   32	false  	f   
6	   64	 (null)	g   
7	  128	 (null)	h   
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
    assert_equal(Arrow::Column.new(@table, 1),
                 @table.visible)
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

    test("empty result") do
      selected_table = @table.filter([false] * @table.size).select_columns(:a)
      assert_equal(<<-TABLE, selected_table.to_s)
	a
      TABLE
    end
  end

  sub_test_case("#column_names") do
    test("unique") do
      table = Arrow::Table.new(a: [1], b: [2], c: [3])
      assert_equal(%w[a b c], table.column_names)
    end

    test("duplicated") do
      table = Arrow::Table.new([["a", [1, 2, 3]], ["a", [4, 5, 6]]])
      assert_equal(%w[a a], table.column_names)
    end
  end

  sub_test_case("#save and .load") do
    module SaveLoadFormatTests
      def test_default
        output = create_output(".arrow")
        @table.save(output)
        assert_equal(@table, Arrow::Table.load(output))
      end

      def test_arrow_file
        output = create_output(".arrow")
        @table.save(output, format: :arrow_file)
        assert_equal(@table, Arrow::Table.load(output, format: :arrow_file))
      end

      def test_batch
        output = create_output(".arrow")
        @table.save(output, format: :batch)
        assert_equal(@table, Arrow::Table.load(output, format: :batch))
      end

      def test_arrows
        output = create_output(".arrows")
        @table.save(output, format: :arrows)
        assert_equal(@table, Arrow::Table.load(output, format: :arrows))
      end

      def test_arrow_streaming
        output = create_output(".arrows")
        @table.save(output, format: :arrow_streaming)
        assert_equal(@table, Arrow::Table.load(output, format: :arrow_streaming))
      end

      def test_stream
        output = create_output(".arrows")
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

      def test_tsv
        output = create_output(".tsv")
        @table.save(output, format: :tsv)
        assert_equal(@table,
                     Arrow::Table.load(output,
                                       format: :tsv,
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
          test("arrow") do
            output = create_output(".arrow")
            @table.save(output)
            assert_equal(@table,
                         Arrow::Table.load(output,
                                           format: :arrow,
                                           schema: @table.schema))
          end

          test("arrows") do
            output = create_output(".arrows")
            @table.save(output)
            assert_equal(@table,
                         Arrow::Table.load(output,
                                           format: :arrows,
                                           schema: @table.schema))
          end

          test("csv") do
            output = create_output(".csv")
            @table.save(output)
            assert_equal(@table,
                         Arrow::Table.load(output,
                                           format: :csv,
                                           schema: @table.schema))
          end

          test("csv, return value") do
            output = create_output(".csv")
            assert_equal(@table, @table.save(output))
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

          test("tsv") do
            output = create_output(".tsv")
            @table.save(output)
            assert_equal(@table,
                         Arrow::Table.load(output,
                                           format: :tsv,
                                           schema: @table.schema))
          end
        end

        sub_test_case("load: auto detect") do
          test("arrow: file") do
            output = create_output(".arrow")
            @table.save(output, format: :arrow_file)
            assert_equal(@table, Arrow::Table.load(output))
          end

          test("arrow: streaming") do
            output = create_output(".arrow")
            @table.save(output, format: :arrows)
            assert_equal(@table, Arrow::Table.load(output))
          end

          test("arrows") do
            output = create_output(".arrows")
            @table.save(output, format: :arrows)
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
            file.close
            Zlib::GzipWriter.open(file.path) do |gz|
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

          test("tsv") do
            file = Tempfile.new(["red-arrow", ".tsv"])
            file.puts(<<-TSV)
name\tscore
alice\t10
bob\t29
chris\t-1
            TSV
            file.close
            table = Arrow::Table.load(file.path)
            assert_equal(<<-TABLE, table.to_s)
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

    sub_test_case("URI") do
      def start_web_server(path, data, content_type)
        http_server = WEBrick::HTTPServer.new(:Port => 0)
        http_server.mount_proc(path) do |request, response|
          response.body = data
          response.content_type = content_type
        end
        http_server_thread = Thread.new do
          http_server.start
        end
        begin
          Timeout.timeout(1) do
            yield(http_server[:Port])
          end
        ensure
          http_server.shutdown
          http_server_thread.join
        end
      end

      data("Arrow File",
           ["arrow", "application/vnd.apache.arrow.file"])
      data("Arrow Stream",
           ["arrows", "application/vnd.apache.arrow.stream"])
      data("CSV",
           ["csv", "text/csv"])
      def test_http(data)
        extension, content_type = data
        output = Arrow::ResizableBuffer.new(1024)
        @table.save(output, format: extension.to_sym)
        path = "/data.#{extension}"
        start_web_server(path,
                         output.data.to_s,
                         content_type) do |port|
          input = URI("http://127.0.0.1:#{port}#{path}")
          loaded_table = Arrow::Table.load(input)
          assert_equal(@table.to_s, loaded_table.to_s)
        end
      end
    end

    sub_test_case("GC") do
      def setup
        table = Arrow::Table.new(integer: [1, 2, 3],
                                 string: ["a", "b", "c"])
        @buffer = Arrow::ResizableBuffer.new(1024)
        table.save(@buffer, format: :arrow)
        @loaded_table = Arrow::Table.load(@buffer)
      end

      def test_chunked_array
        chunked_array = @loaded_table[0].data
        assert_equal(@buffer,
                     chunked_array.instance_variable_get(:@input).buffer)
      end

      def test_array
        array = @loaded_table[0].data.chunks[0]
        assert_equal(@buffer,
                     array.instance_variable_get(:@input).buffer)
      end

      def test_record_batch
        record_batch = @loaded_table.each_record_batch.first
        assert_equal(@buffer,
                     record_batch.instance_variable_get(:@input).buffer)
      end

      def test_record_batch_array
        array = @loaded_table.each_record_batch.first[0].data
        assert_equal(@buffer,
                     array.instance_variable_get(:@input).buffer)
      end

      def test_record_batch_table
        table = @loaded_table.each_record_batch.first.to_table
        assert_equal(@buffer,
                     table.instance_variable_get(:@input).buffer)
      end

      def test_slice
        table = @loaded_table.slice(0..-1)
        assert_equal(@buffer,
                     table.instance_variable_get(:@input).buffer)
      end

      def test_merge
        table = @loaded_table.merge({})
        assert_equal(@buffer,
                     table.instance_variable_get(:@input).buffer)
      end

      def test_remove_column
        table = @loaded_table.remove_column(0)
        assert_equal(@buffer,
                     table.instance_variable_get(:@input).buffer)
      end

      def test_pack
        table = @loaded_table.pack
        assert_equal(@buffer,
                     table.instance_variable_get(:@input).buffer)
      end

      def test_join
        table = @loaded_table.join(@loaded_table, :integer)
        assert_equal(@buffer,
                     table.instance_variable_get(:@input).buffer)
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
2	    4	 (null)
3	    8	true   
4	   16	true   
5	   32	false  
6	   64	 (null)
7	  128	 (null)
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

  sub_test_case("#filter") do
    def setup
      super
      @options = Arrow::FilterOptions.new
      @options.null_selection_behavior = :emit_null
    end

    test("Array: boolean") do
      filter = [nil, true, true, false, true, false, true, true]
      assert_equal(<<-TABLE, @table.filter(filter, @options).to_s)
	 count	visible
0	(null)	 (null)
1	     2	false  
2	     4	 (null)
3	    16	true   
4	    64	 (null)
5	   128	 (null)
      TABLE
    end

    test("Arrow::BooleanArray") do
      array = [nil, true, true, false, true, false, true, true]
      filter = Arrow::BooleanArray.new(array)
      assert_equal(<<-TABLE, @table.filter(filter, @options).to_s)
	 count	visible
0	(null)	 (null)
1	     2	false  
2	     4	 (null)
3	    16	true   
4	    64	 (null)
5	   128	 (null)
      TABLE
    end

    test("Arrow::ChunkedArray") do
      filter_chunks = [
        Arrow::BooleanArray.new([nil, true, true]),
        Arrow::BooleanArray.new([false, true, false]),
        Arrow::BooleanArray.new([true, true]),
      ]
      filter = Arrow::ChunkedArray.new(filter_chunks)
      assert_equal(<<-TABLE, @table.filter(filter, @options).to_s)
	 count	visible
0	(null)	 (null)
1	     2	false  
2	     4	 (null)
3	    16	true   
4	    64	 (null)
5	   128	 (null)
      TABLE
    end
  end

  sub_test_case("#take") do
    test("Arrow: boolean") do
      indices = [1, 0, 2]
      assert_equal(<<-TABLE, @table.take(indices).to_s)
	count	visible
0	    2	false  
1	    1	true   
2	    4	 (null)
      TABLE
    end

    test("Arrow::Array") do
      indices = Arrow::Int16Array.new([1, 0, 2])
      assert_equal(<<-TABLE, @table.take(indices).to_s)
	count	visible
0	    2	false  
1	    1	true   
2	    4	 (null)
      TABLE
    end

    test("Arrow::ChunkedArray") do
      chunks = [
        Arrow::Int16Array.new([1, 0]),
        Arrow::Int16Array.new([2])
      ]
      indices = Arrow::ChunkedArray.new(chunks)
      assert_equal(<<-TABLE, @table.take(indices).to_s)
	count	visible
0	    2	false  
1	    1	true   
2	    4	 (null)
      TABLE
    end
  end

  sub_test_case("#concatenate") do
    test("options: :unify_schemas") do
      table1 = Arrow::Table.new(a: [true],
                                b: [false])
      table2 = Arrow::Table.new(b: [false])
      concatenated = table1.concatenate([table2], unify_schemas: true)
      assert_equal(<<-TABLE, concatenated.to_s)
	a	b
0	true	false
1	(null)	false
      TABLE
    end
  end

  sub_test_case("#join") do
    test("keys: nil (natural join)") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [1, 3]],
                                      ["number", [10, 30]],
                                      ["string", ["one", "three"]],
                                    ]),
                   table1.join(table2))
    end

    test("keys: String") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [1, 3]],
                                      ["number", [10, 30]],
                                      ["string", ["one", "three"]],
                                    ]),
                   table1.join(table2, "key"))
    end

    test("keys: Symbol") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [1, 3]],
                                      ["number", [10, 30]],
                                      ["string", ["one", "three"]],
                                    ]),
                   table1.join(table2, :key))
    end

    test("keys: [String]") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [1, 3]],
                                      ["number", [10, 30]],
                                      ["key", [1, 3]],
                                      ["string", ["one", "three"]],
                                    ]),
                   table1.join(table2, ["key"]))
    end

    test("keys: [String, Symbol]") do
      table1 = Arrow::Table.new(key1: [1, 1, 2, 2],
                                key2: [10, 100, 20, 200],
                                number: [1010, 1100, 2020, 2200])
      table2 = Arrow::Table.new(key1: [1, 2, 2],
                                key2: [100, 20, 50],
                                string: ["1-100", "2-20", "2-50"])
      assert_equal(Arrow::Table.new([
                                      ["key1", [1, 2]],
                                      ["key2", [100, 20]],
                                      ["number", [1100, 2020]],
                                      ["key1", [1, 2]],
                                      ["key2", [100, 20]],
                                      ["string", ["1-100", "2-20"]],
                                    ]),
                   table1.join(table2, ["key1", :key2]))
    end

    test("keys: {left: String, right: Symbol}") do
      table1 = Arrow::Table.new(left_key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(right_key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["left_key", [1, 3]],
                                      ["number", [10, 30]],
                                      ["right_key", [1, 3]],
                                      ["string", ["one", "three"]],
                                    ]),
                   table1.join(table2,
                               {left: "left_key", right: :right_key},
                               type: :inner))
    end

    test("keys: {left: [String, Symbol], right: [Symbol, String]}") do
      table1 = Arrow::Table.new(left_key1: [1, 1, 2, 2],
                                left_key2: [10, 100, 20, 200],
                                number: [1010, 1100, 2020, 2200])
      table2 = Arrow::Table.new(right_key1: [1, 2, 2],
                                right_key2: [100, 20, 50],
                                string: ["1-100", "2-20", "2-50"])
      assert_equal(Arrow::Table.new([
                                      ["left_key1", [1, 2]],
                                      ["left_key2", [100, 20]],
                                      ["number", [1100, 2020]],
                                      ["right_key1", [1, 2]],
                                      ["right_key2", [100, 20]],
                                      ["string", ["1-100", "2-20"]],
                                    ]),
                   table1.join(table2,
                               {
                                 left: ["left_key1", :left_key2],
                                 right: [:right_key1, "right_key2"],
                               },
                               type: :inner))
    end

    test("type: :left_outer") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [1, 3, 2]],
                                      ["number", [10, 30, 20]],
                                      ["string", ["one", "three", nil]],
                                    ]),
                   table1.join(table2, "key", type: :left_outer))
    end

    test("type: :right_outer") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [1, 3]],
                                      ["number", [10, 30]],
                                      ["string", ["one", "three"]],
                                    ]),
                   table1.join(table2, "key", type: :right_outer))
    end

    test("type: :full_outer") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [1, 3, 2]],
                                      ["number", [10, 30, 20]],
                                      ["string", ["one", "three", nil]],
                                    ]),
                   table1.join(table2, "key", type: :full_outer))
    end

    test("type: :left_semi") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [1, 3]],
                                      ["number", [10, 30]],
                                    ]),
                   table1.join(table2, "key", type: :left_semi))
    end

    test("type: :right_semi") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [3, 1]],
                                      ["string", ["three", "one"]],
                                    ]),
                   table1.join(table2, "key", type: :right_semi))
    end

    test("type: :left_anti") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [2]],
                                      ["number", [20]],
                                    ]),
                   table1.join(table2, "key", type: :left_anti))
    end

    test("type: :right_anti") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", Arrow::ChunkedArray.new(:uint8)],
                                      ["string", Arrow::ChunkedArray.new(:string)],
                                    ]),
                   table1.join(table2, "key", type: :right_anti))
    end

    test("left_outputs: & right_outputs:") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new(key: [1, 3],
                                    number: [10, 30],
                                    string: ["one", "three"]),
                   table1.join(table2,
                               "key",
                               left_outputs: ["key", "number"],
                               right_outputs: ["string"]))
    end

    test("left_outputs: & type: :inner") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [1, 3]],
                                      ["number", [10, 30]],
                                      ["key", [1, 3]],
                                      ["string", ["one", "three"]]
                                    ]),
                   table1.join(table2,
                               type: :inner,
                               left_outputs: table1.column_names,
                               right_outputs: table2.column_names))
    end

    test("left_outputs: & type: :left_outer") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [1, 3, 2]],
                                      ["number", [10, 30, 20]],
                                      ["key", [1, 3, nil]],
                                      ["string", ["one", "three", nil]],
                                    ]),
                   table1.join(table2,
                               type: :left_outer,
                               left_outputs: table1.column_names,
                               right_outputs: table2.column_names))
    end

    test("left_outputs: & type: :right_outer") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [1, 3]],
                                      ["number", [10, 30]],
                                      ["key", [1, 3]],
                                      ["string", ["one", "three"]],
                                    ]),
                   table1.join(table2,
                               type: :right_outer,
                               left_outputs: table1.column_names,
                               right_outputs: table2.column_names))
    end

    test("left_outputs: & type: :full_outer") do
      table1 = Arrow::Table.new(key: [1, 2, 3],
                                number: [10, 20, 30])
      table2 = Arrow::Table.new(key: [3, 1],
                                string: ["three", "one"])
      assert_equal(Arrow::Table.new([
                                      ["key", [1, 3, 2]],
                                      ["number", [10, 30, 20]],
                                      ["key", [1, 3, nil]],
                                      ["string", ["one", "three", nil]],
                                    ]),
                   table1.join(table2,
                               type: :full_outer,
                               left_outputs: table1.column_names,
                               right_outputs: table2.column_names))
    end

    test("left_suffix: & keys: [String]") do
      table1 = Arrow::Table.new(key1: [1, 1, 2, 2],
                                key2: [10, 100, 20, 200],
                                number: [1010, 1100, 2020, 2200])
      table2 = Arrow::Table.new(key1: [1, 2, 2],
                                key2: [100, 20, 50],
                                string: ["1-100", "2-20", "2-50"])
      assert_equal(Arrow::Table.new([
                                      ["key1_left", [1, 2]],
                                      ["key2_left", [100, 20]],
                                      ["number", [1100, 2020]],
                                      ["key1_right", [1, 2]],
                                      ["key2_right", [100, 20]],
                                      ["string", ["1-100", "2-20"]],
                                    ]),
                    table1.join(table2,
                                ["key1", "key2"],
                                left_suffix: "_left",
                                right_suffix: "_right"))
    end
  end
end
