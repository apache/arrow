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

class TestPlasmaClient < Test::Unit::TestCase
  def setup
    @store = nil
    @store = Helper::PlasmaStore.new
    @store.start
    @id = Plasma::ObjectID.new("Hello")
    @data = "World"
  end

  def teardown
    @store.stop if @store
  end

  def test_new_pathname
    client = Plasma::Client.new(Pathname(@store.socket_path))
    object = client.create(@id, @data.bytesize, nil)
    object.data.set_data(0, @data)
    object.seal

    object = client.refer_object(@id, -1)
    assert_equal(@data, object.data.data.to_s)
  end

  def test_new_options
    client = Plasma::Client.new(@store.socket_path, n_retries: 1)
    object = client.create(@id, @data.bytesize, nil)
    object.data.set_data(0, @data)
    object.seal

    object = client.refer_object(@id, -1)
    assert_equal(@data, object.data.data.to_s)
  end
end
