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

class TestFlightClientOptions < Test::Unit::TestCase
  def setup
    omit("Arrow Flight is required") unless defined?(ArrowFlight)
    @options = ArrowFlight::ClientOptions.new
  end

  def test_tls_root_certificates
    assert_equal("", @options.tls_root_certificates)
    @options.tls_root_certificates = "root"
    assert_equal("root", @options.tls_root_certificates)
  end

  def test_override_host_name
    assert_equal("", @options.override_host_name)
    @options.override_host_name = "client.example.com"
    assert_equal("client.example.com", @options.override_host_name)
  end

  def test_certificate_chain
    assert_equal("", @options.certificate_chain)
    @options.certificate_chain = "chain"
    assert_equal("chain", @options.certificate_chain)
  end

  def test_private_key
    assert_equal("", @options.private_key)
    @options.private_key = "private"
    assert_equal("private", @options.private_key)
  end

  def test_write_size_limit_bytes
    assert_equal(0, @options.write_size_limit_bytes)
    @options.write_size_limit_bytes = 100
    assert_equal(100, @options.write_size_limit_bytes)
  end

  def test_disable_server_verifiation
    assert do
      not @options.disable_server_verification?
    end
    @options.disable_server_verification = true
    assert do
      @options.disable_server_verification?
    end
  end
end
