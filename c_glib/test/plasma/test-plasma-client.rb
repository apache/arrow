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
  include Helper::Omittable

  def setup
    @store = nil
    omit("Plasma is required") unless defined?(::Plasma)
    @store = Helper::PlasmaStore.new
    @store.start
    @options = Plasma::ClientOptions.new
    @client = Plasma::Client.new(@store.socket_path, @options)
    @id = Plasma::ObjectID.new("Hello")
    @data = "World"
    @options = Plasma::ClientCreateOptions.new
  end

  def teardown
    @store.stop if @store
  end

  sub_test_case("#create") do
    def setup
      super

      @metadata = "Metadata"
    end

    test("no options") do
      require_gi(1, 42, 0)

      object = @client.create(@id, @data.bytesize)
      object.data.set_data(0, @data)
      object.seal

      object = @client.refer_object(@id, -1)
      assert_equal(@data, object.data.data.to_s)
    end

    test("options: metadata") do
      @options.set_metadata(@metadata)
      object = @client.create(@id, 1, @options)
      object.seal

      object = @client.refer_object(@id, -1)
      assert_equal(@metadata, object.metadata.data.to_s)
    end

    test("options: GPU device") do
      omit("Arrow CUDA is required") unless defined?(::ArrowCUDA)

      gpu_device = 0

      @options.gpu_device = gpu_device
      @options.metadata = @metadata
      object = @client.create(@id, @data.bytesize, @options)
      object.data.copy_from_host(@data)
      object.seal

      object = @client.refer_object(@id, -1)
      assert_equal([
                     gpu_device,
                     @data,
                     @metadata,
                   ],
                   [
                     object.gpu_device,
                     object.data.copy_to_host(0, @data.bytesize).to_s,
                     object.metadata.copy_to_host(0, @metadata.bytesize).to_s,
                   ])
    end
  end

  test("#disconnect") do
    @client.disconnect
    assert_raise(Arrow::Error::Io) do
      @client.create(@id, @data.bytesize, @options)
    end
  end
end
