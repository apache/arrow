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

class TestClient < Test::Unit::TestCase
  def setup
    @server = nil
    omit("Unstable on Windows") if Gem.win_platform?
    @server = Helper::Server.new
    @server.listen("grpc://127.0.0.1:0")
    @location = "grpc://127.0.0.1:#{@server.port}"
  end

  def teardown
    return if @server.nil?
    @server.shutdown
  end

  def test_list_flights
    client = ArrowFlight::Client.new(@location)
    generator = Helper::InfoGenerator.new
    assert_equal([generator.page_view],
                 client.list_flights)
  end

  def test_do_get
    client = ArrowFlight::Client.new(@location)
    generator = Helper::InfoGenerator.new
    reader = client.do_get(generator.page_view_ticket)
    assert_equal(generator.page_view_table,
                 reader.read_all)
  end

  def test_do_put_with_block
    client = ArrowFlight::Client.new(@location)
    generator = Helper::InfoGenerator.new
    descriptor = generator.page_view_descriptor
    table = generator.page_view_table
    client.do_put(descriptor, table.schema) do |reader, writer|
      writer.write_table(table)
      writer.done_writing
      metadata = reader.read
      assert_equal(["done", table],
                   [metadata.data.to_s, @server.uploaded_table])
    end
  end

  def test_do_put_without_block
    client = ArrowFlight::Client.new(@location)
    generator = Helper::InfoGenerator.new
    descriptor = generator.page_view_descriptor
    table = generator.page_view_table
    reader, writer = client.do_put(descriptor, table.schema)
    begin
      writer.write_table(table)
      writer.done_writing
      metadata = reader.read
      assert_equal(["done", table],
                   [metadata.data.to_s, @server.uploaded_table])
    ensure
      writer.close
    end
  end
end
