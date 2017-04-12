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

class TestMemoryMappedFile < Test::Unit::TestCase
  def test_open
    tempfile = Tempfile.open("arrow-io-memory-mapped-file")
    tempfile.write("Hello")
    tempfile.close
    file = Arrow::MemoryMappedFile.open(tempfile.path, :read)
    begin
      buffer = " " * 5
      file.read(buffer)
      assert_equal("Hello", buffer)
    ensure
      file.close
    end
  end

  def test_size
    tempfile = Tempfile.open("arrow-io-memory-mapped-file")
    tempfile.write("Hello")
    tempfile.close
    file = Arrow::MemoryMappedFile.open(tempfile.path, :read)
    begin
      assert_equal(5, file.size)
    ensure
      file.close
    end
  end

  def test_read
    tempfile = Tempfile.open("arrow-io-memory-mapped-file")
    tempfile.write("Hello World")
    tempfile.close
    file = Arrow::MemoryMappedFile.open(tempfile.path, :read)
    begin
      buffer = " " * 5
      _success, n_read_bytes = file.read(buffer)
      assert_equal("Hello", buffer.byteslice(0, n_read_bytes))
    ensure
      file.close
    end
  end

  def test_read_at
    tempfile = Tempfile.open("arrow-io-memory-mapped-file")
    tempfile.write("Hello World")
    tempfile.close
    file = Arrow::MemoryMappedFile.open(tempfile.path, :read)
    begin
      buffer = " " * 5
      _success, n_read_bytes = file.read_at(6, buffer)
      assert_equal("World", buffer.byteslice(0, n_read_bytes))
    ensure
      file.close
    end
  end

  def test_write
    tempfile = Tempfile.open("arrow-io-memory-mapped-file")
    tempfile.write("Hello")
    tempfile.close
    file = Arrow::MemoryMappedFile.open(tempfile.path, :readwrite)
    begin
      file.write("World")
    ensure
      file.close
    end
    assert_equal("World", File.read(tempfile.path))
  end

  def test_write_at
    tempfile = Tempfile.open("arrow-io-memory-mapped-file")
    tempfile.write("Hello")
    tempfile.close
    file = Arrow::MemoryMappedFile.open(tempfile.path, :readwrite)
    begin
      file.write_at(2, "rld")
    ensure
      file.close
    end
    assert_equal("Herld", File.read(tempfile.path))
  end

  def test_flush
    tempfile = Tempfile.open("arrow-io-memory-mapped-file")
    tempfile.write("Hello")
    tempfile.close
    file = Arrow::MemoryMappedFile.open(tempfile.path, :readwrite)
    begin
      file.write("World")
      file.flush
      assert_equal("World", File.read(tempfile.path))
    ensure
      file.close
    end
  end

  def test_tell
    tempfile = Tempfile.open("arrow-io-memory-mapped-file")
    tempfile.write("Hello World")
    tempfile.close
    file = Arrow::MemoryMappedFile.open(tempfile.path, :read)
    begin
      buffer = " " * 5
      file.read(buffer)
      assert_equal(5, file.tell)
    ensure
      file.close
    end
  end

  def test_mode
    tempfile = Tempfile.open("arrow-io-memory-mapped-file")
    tempfile.write("Hello World")
    tempfile.close
    file = Arrow::MemoryMappedFile.open(tempfile.path, :readwrite)
    begin
      assert_equal(Arrow::FileMode::READWRITE, file.mode)
    ensure
      file.close
    end
  end
end
