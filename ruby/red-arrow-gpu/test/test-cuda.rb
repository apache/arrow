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

class TestCUDA < Test::Unit::TestCase
  def setup
    @manager = ArrowGPU::CUDADeviceManager.new
    omit("At least one GPU is required") if @manager.n_devices.zero?
    @context = @manager[0]
  end

  sub_test_case("BufferOutputStream") do
    def setup
      super
      @buffer = ArrowGPU::CUDABuffer.new(@context, 128)
    end

    def test_new
      ArrowGPU::CUDABufferOutputStream.open(@buffer) do |stream|
        stream.write("Hello World")
      end
      assert_equal("Hello World", @buffer.copy_to_host(0, 11).to_s)
    end
  end
end
