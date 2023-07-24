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
    @client = ArrowFlight::Client.new(@location)
    @sql_client = ArrowFlightSQL::Client.new(@client)
  end

  def teardown
    return if @server.nil?
    @server.shutdown
  end

  def test_execute
    info = @sql_client.execute("SELECT * FROM page_view_table")
    endpoint = info.endpoints.first
    generator = Helper::InfoGenerator.new
    reader = @sql_client.do_get(endpoint.ticket)
    assert_equal(generator.page_view_table,
                 reader.read_all)
  end
end
