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

class TestFlightStreamReader < Test::Unit::TestCase
  include Helper::Omittable

  def setup
    @server = nil
    omit("Arrow Flight is required") unless defined?(ArrowFlight)
    omit("Unstable on Windows") if Gem.win_platform?
    require_gi_bindings(3, 4, 5)
    @server = Helper::FlightServer.new
    host = "127.0.0.1"
    location = ArrowFlight::Location.new("grpc://#{host}:0")
    options = ArrowFlight::ServerOptions.new(location)
    @server.listen(options)
    location = ArrowFlight::Location.new("grpc://#{host}:#{@server.port}")
    client = ArrowFlight::Client.new(location)
    @generator = Helper::FlightInfoGenerator.new
    @reader = client.do_get(@generator.page_view_ticket)
  end

  def teardown
    return if @server.nil?
    @server.shutdown
  end

  def test_read_next
    chunks = []
    loop do
      chunk = @reader.read_next
      break if chunk.nil?
      chunks << chunk
    end
    chunks_content = chunks.collect do |chunk|
      [
        chunk.data,
        chunk.metadata&.data&.to_s,
      ]
    end
    table_batch_reader = Arrow::TableBatchReader.new(@generator.page_view_table)
    assert_equal([
                   [
                     table_batch_reader.read_next,
                     nil,
                   ],
                 ],
                 chunks_content)
  end

  def test_read_all
    assert_equal(@generator.page_view_table,
                 @reader.read_all)
  end
end
