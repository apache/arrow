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

class TestArrowTable < Test::Unit::TestCase
  def setup
    Dir.mktmpdir do |tmpdir|
      @dir = tmpdir
      @path1 = File.join(@dir, "data", "table1.arrow")
      @table1 = Arrow::Table.new(visible: [true, false, true],
                                 point: [1, 2, 3])
      @path2 = File.join(@dir, "data", "table2.arrow")
      @table2 = Arrow::Table.new(visible: [true],
                                 point: [10])
      yield
    end
  end

  def build_file_uri(path)
    absolute_path = File.expand_path(path)
    if absolute_path.start_with?("/")
      URI("file://#{absolute_path}")
    else
      URI("file:///#{absolute_path}")
    end
  end

  sub_test_case("load") do
    def test_no_scheme
      Dir.chdir(@dir) do
        uri = URI(File.basename(@path1))
        @table1.save(uri)
        assert_equal(@table1, Arrow::Table.load(uri))
      end
    end

    def test_file
      uri = build_file_uri(@path1)
      @table1.save(uri)
      assert_equal(@table1, Arrow::Table.load(uri))
    end

    def test_directory_uri
      uri = build_file_uri(@dir)
      @table1.save(build_file_uri(@path1))
      @table2.save(build_file_uri(@path2))
      assert_equal(@table1.concatenate([@table2]),
                   Arrow::Table.load(uri))
    end

    def test_directory_path
      @table1.save(build_file_uri(@path1))
      @table2.save(build_file_uri(@path2))
      assert_equal(@table1.concatenate([@table2]),
                   Arrow::Table.load(@dir))
    end

    def test_filter
      @table1.save(build_file_uri(@path1))
      @table2.save(build_file_uri(@path2))
      assert_equal(Arrow::Table.new(visible: [true, true, true],
                                    point: [1, 3, 10]),
                   Arrow::Table.load(@dir,
                                     filter: ["equal", :visible, true]))
    end

    def test_schema
      uri = build_file_uri(@path1)
      @table1.save(uri)
      schema = Arrow::Schema.new(visible: :boolean,
                                 point: :int64)
      assert_equal(Arrow::Table.new(schema,
                                    [
                                      @table1[:visible].data,
                                      @table1[:point].cast(:int64),
                                    ]),
                   Arrow::Table.load(uri, schema: schema))
    end
  end
end
