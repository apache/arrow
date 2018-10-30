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
    omit("Plasma is required") unless defined?(::Plasma)
    plasma_store_server_path = `pkg-config --variable=executable plasma`.chomp
    plasma_store_memory_size = 1000000000
    @plasma_store_socket_name = "/tmp/plasma"
    @pid = spawn("#{plasma_store_server_path}      \
                 -m #{plasma_store_memory_size}    \
                 -s #{@plasma_store_socket_name}")
    sleep 0.1
  end

  def teardown
    Process.kill(:TERM, @pid) if @pid
  end

  def test_new
    assert_nothing_raised do
      Plasma::Client.new(@plasma_store_socket_name)
    end
  end
end
