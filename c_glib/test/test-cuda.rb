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
  include Helper::Buildable

  def setup
    omit("Arrow CUDA is required") unless defined?(::ArrowCUDA)
    @manager = ArrowCUDA::DeviceManager.new
    omit("At least one GPU is required") if @manager.n_devices.zero?
    @context = @manager.get_context(0)
  end

  sub_test_case("Context") do
    def test_allocated_size
      allocated_size_before = @context.allocated_size
      size = 128
      buffer = ArrowCUDA::Buffer.new(@context, size)
      assert_equal(size,
                   @context.allocated_size - allocated_size_before)
    end
  end

  sub_test_case("Buffer") do
    def setup
      super
      @buffer = ArrowCUDA::Buffer.new(@context, 128)
    end

    def test_copy
      @buffer.copy_from_host("Hello World")
      assert_equal("llo W", @buffer.copy_to_host(2, 5).to_s)
    end

    def test_export
      @buffer.copy_from_host("Hello World")
      handle = @buffer.export
      serialized_handle = handle.serialize.data
      Tempfile.open("arrow-cuda-export") do |output|
        pid = spawn(RbConfig.ruby, "-e", <<-SCRIPT)
require "gi"

Gio = GI.load("Gio")
Arrow = GI.load("Arrow")
ArrowCUDA = GI.load("ArrowCUDA")

manager = ArrowCUDA::DeviceManager.new
context = manager.get_context(0)
serialized_handle = #{serialized_handle.to_s.dump}
handle = ArrowCUDA::IPCMemoryHandle.new(serialized_handle)
buffer = ArrowCUDA::Buffer.new(context, handle)
File.open(#{output.path.dump}, "w") do |output|
  output.print(buffer.copy_to_host(0, 6).to_s)
end
        SCRIPT
        Process.waitpid(pid)
        assert_equal("Hello ", output.read)
      end
    end

    def test_context
      assert_equal(@context.allocated_size,
                   @buffer.context.allocated_size)
    end

    def test_record_batch
      field = Arrow::Field.new("enabled", Arrow::BooleanDataType.new)
      schema = Arrow::Schema.new([field])
      columns = [
        build_boolean_array([true]),
      ]
      cpu_record_batch = Arrow::RecordBatch.new(schema, 1, columns)

      buffer = ArrowCUDA::Buffer.new(@context, cpu_record_batch)
      gpu_record_batch = buffer.read_record_batch(schema)
      assert_equal(cpu_record_batch.n_rows,
                   gpu_record_batch.n_rows)
    end
  end

  sub_test_case("HostBuffer") do
    def test_new
      buffer = ArrowCUDA::HostBuffer.new(0, 128)
      assert_equal(128, buffer.size)
    end
  end

  sub_test_case("BufferInputStream") do
    def test_new
      buffer = ArrowCUDA::Buffer.new(@context, 128)
      buffer.copy_from_host("Hello World")
      stream = ArrowCUDA::BufferInputStream.new(buffer)
      begin
        assert_equal("Hello Worl", stream.read(5).copy_to_host(0, 10).to_s)
      ensure
        stream.close
      end
    end
  end

  sub_test_case("BufferOutputStream") do
    def setup
      super
      @buffer = ArrowCUDA::Buffer.new(@context, 128)
      @buffer.copy_from_host("\x00" * @buffer.size)
      @stream = ArrowCUDA::BufferOutputStream.new(@buffer)
    end

    def cleanup
      super
      @stream.close
    end

    def test_new
      @stream.write("Hello World")
      assert_equal("Hello World", @buffer.copy_to_host(0, 11).to_s)
    end

    def test_buffer
      assert_equal(0, @stream.buffer_size)
      @stream.buffer_size = 5
      assert_equal(5, @stream.buffer_size)
      @stream.write("Hell")
      assert_equal(4, @stream.buffered_size)
      assert_equal("\x00" * 5, @buffer.copy_to_host(0, 5).to_s)
      @stream.write("o")
      assert_equal("Hello", @buffer.copy_to_host(0, 5).to_s)
    end
  end
end
