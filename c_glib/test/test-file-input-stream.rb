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

class TestFileInputStream < Test::Unit::TestCase
  def setup
    @data = "Hello World"
    @tempfile = Tempfile.open("arrow-file-input-stream")
    @tempfile.write(@data)
    @tempfile.close
  end

  def test_new
    input = Arrow::FileInputStream.new(@tempfile.path)
    begin
      buffer = input.read(5)
      assert_equal("Hello", buffer.data.to_s)
    ensure
      input.close
    end
  end

  def test_close
    input = Arrow::FileInputStream.new(@tempfile.path)
    assert do
      not input.closed?
    end
    input.close
    assert do
      input.closed?
    end
  end

  def test_size
    input = Arrow::FileInputStream.new(@tempfile.path)
    begin
      assert_equal(@data.bytesize, input.size)
    ensure
      input.close
    end
  end

  def test_read
    input = Arrow::FileInputStream.new(@tempfile.path)
    begin
      buffer = input.read(5)
      assert_equal("Hello", buffer.data.to_s)
    ensure
      input.close
    end
  end

  def test_read_at
    input = Arrow::FileInputStream.new(@tempfile.path)
    begin
      buffer = input.read_at(6, 5)
      assert_equal("World", buffer.data.to_s)
    ensure
      input.close
    end
  end

  def test_mode
    input = Arrow::FileInputStream.new(@tempfile.path)
    begin
      assert_equal(Arrow::FileMode::READ, input.mode)
    ensure
      input.close
    end
  end

  def test_file_descriptor
    @tempfile.open
    begin
      fd = @tempfile.fileno
      input = Arrow::FileInputStream.new(fd)
      begin
        assert_equal(fd, input.file_descriptor)
      ensure
        input.close
      end
    ensure
      begin
        @tempfile.close
      rescue
      end
    end
  end
end
