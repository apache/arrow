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

class TestFlightInfo < Test::Unit::TestCase
  include Helper::Writable

  def setup
    omit("Arrow Flight is required") unless defined?(ArrowFlight)
    @generator = Helper::FlightInfoGenerator.new
  end

  sub_test_case("#get_schema") do
    def test_with_options
      info = @generator.page_view
      table = @generator.page_view_table
      options = Arrow::ReadOptions.new
      assert_equal(table.schema,
                   info.get_schema(options))
    end

    def test_without_options
      info = @generator.page_view
      table = @generator.page_view_table
      assert_equal(table.schema,
                   info.get_schema)
    end
  end

  def test_descriptor
    info = @generator.page_view
    assert_equal(@generator.page_view_descriptor,
                 info.descriptor)
  end

  def test_endpoints
    info = @generator.page_view
    assert_equal(@generator.page_view_endpoints,
                 info.endpoints)
  end

  def test_total_records
    info = @generator.page_view
    table = @generator.page_view_table
    assert_equal(table.n_rows,
                 info.total_records)
  end

  def test_total_bytes
    info = @generator.page_view
    table = @generator.page_view_table
    output = Arrow::ResizableBuffer.new(0)
    write_table(table, output, type: :stream)
    assert_equal(output.size,
                 info.total_bytes)
  end

  def test_equal
    info1 = @generator.page_view
    info2 = @generator.page_view
    assert do
      info1 == info2
    end
  end
end
