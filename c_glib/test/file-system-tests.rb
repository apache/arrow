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

module FileSystemTests
  private def all_entries
    selector = Arrow::FileSelector.new
    selector.base_dir = ""
    selector.recursive = true
    infos = @fs.get_file_infos_selector(selector)
    infos.map {|info| [info.path, info.type.nick.to_sym]}.to_h
  end

  private def mkpath(path)
    @fs.create_dir(path, true)
  end

  private def create_file(path, content=nil)
    stream = @fs.open_output_stream(path)
    stream.write(content) if content
    stream.close
  end

  private def read_file(path)
    stream = @fs.open_input_stream(path)
    size = @fs.get_file_info(path).size
    bytes = stream.read_bytes(size)
    stream.close
    bytes.to_s
  end

  private def file?(path)
    info = @fs.get_file_info(path)
    info.file?
  rescue Arrow::Error::Io
    false
  end

  private def directory?(path)
    info = @fs.get_file_info(path)
    info.dir?
  rescue Arrow::Error::Io
    false
  end

  def test_empty
    assert { all_entries.empty? }
  end

  def test_create_dir
    @fs.create_dir("AB/CD/EF", true) # recursive
    @fs.create_dir("AB/GH", false) # non-recursive
    assert_equal({
                   "AB" => :dir,
                   "AB/CD" => :dir,
                   "AB/CD/EF" => :dir,
                   "AB/GH" => :dir
                 },
                 all_entries)
  end

  def test_create_dir_with_nonexistent_parent
    assert_raise(Arrow::Error::Io) do
      @fs.create_dir("AB/GH/IJ", false) # non-recursive, parent doesn't exist
    end
    assert_equal({},
                 all_entries)
  end

  def test_create_dir_under_file
    create_file("empty_file")
    assert_raise(Arrow::Error::Io) do
      @fs.create_dir(File.join("empty_file", "XY"), true)
    end
    assert_equal({"empty_file" => :file},
                 all_entries)
  end

  def test_delete_dir
    mkpath("AB/CD/EF")
    mkpath("AB/GH/IJ")
    create_file("AB/abc")
    create_file("AB/CD/def")
    create_file("AB/CD/EF/ghi")

    @fs.delete_dir("AB/CD")
    @fs.delete_dir("AB/GH/IJ")

    assert_equal({
                   "AB" => :dir,
                   "AB/GH" => :dir,
                   "AB/abc" => :file
                 },
                 all_entries)
  end

  def test_delete_dir_contents
    mkpath("AB/CD/EF")
    mkpath("AB/GH/IJ")
    create_file("AB/abc")
    create_file("AB/CD/def")
    create_file("AB/CD/EF/ghi")

    @fs.delete_dir_contents("AB/CD")
    @fs.delete_dir_contents("AB/GH/IJ")

    assert_equal({
                   "AB" => :dir,
                   "AB/CD" => :dir,
                   "AB/GH" => :dir,
                   "AB/GH/IJ" => :dir,
                   "AB/abc" => :file
                 },
                 all_entries)
  end

  def test_delete_file
    mkpath("AB")
    create_file("AB/def")
    assert { file?("AB/def") }

    @fs.delete_file("AB/def")
    assert { not file?("AB/def") }
  end

  def test_delete_files
    mkpath("AB")
    create_file("abc")
    {
      def: 123,
      ghi: 456,
      jkl: 789,
      mno: 789
    }.each do |name, content|
      create_file(File.join("AB", name.to_s), content.to_s)
    end

    assert_equal({
                   "AB" => :dir,
                   "AB/def" => :file,
                   "AB/ghi" => :file,
                   "AB/jkl" => :file,
                   "AB/mno" => :file,
                   "abc" => :file
                 },
                 all_entries)

    @fs.delete_files(["abc", "AB/def"])
    assert_equal({
                   "AB" => :dir,
                   "AB/ghi" => :file,
                   "AB/jkl" => :file,
                   "AB/mno" => :file
                 },
                 all_entries)
  end

  def test_move_file
    mkpath("AB/CD")
    mkpath("EF")
    create_file("abc")
    assert_equal({
                   "AB" => :dir,
                   "AB/CD" => :dir,
                   "EF" => :dir,
                   "abc" => :file
                 },
                 all_entries)

    @fs.move("abc", "AB/CD/ghi")
    assert_equal({
                   "AB" => :dir,
                   "AB/CD" => :dir,
                   "EF" => :dir,
                   "AB/CD/ghi" => :file
                 },
                 all_entries)
  end

  def move_dir_is_supported?
    true
  end

  def test_move_dir
    omit("move_dir is not allowed") unless move_dir_is_supported?

    mkpath("AB/CD")
    mkpath("EF")
    assert_equal({
                   "AB" => :dir,
                   "AB/CD" => :dir,
                   "EF" => :dir
                 },
                 all_entries)

    @fs.move("AB", "GH")
    assert_equal({
                   "EF" => :dir,
                   "GH" => :dir,
                   "GH/CD" => :dir
                 },
                 all_entries)
  end

  def test_copy_file
    mkpath("AB/CD")
    mkpath("EF")
    create_file("AB/abc", "data")
    assert_equal({
                   "AB" => :dir,
                   "AB/CD" => :dir,
                   "EF" => :dir,
                   "AB/abc" => :file
                 },
                 all_entries)

    @fs.copy_file("AB/abc", "def")
    assert_equal({
                   "AB" => :dir,
                   "AB/CD" => :dir,
                   "EF" => :dir,
                   "AB/abc" => :file,
                   "def" => :file
                 },
                 all_entries)
    assert_equal("data",
                 read_file("def"))
  end

  def test_get_file_info
    mkpath("AB/CD")
    create_file("AB/CD/ghi", "some data")

    info = @fs.get_file_info("AB")
    assert_equal(Arrow::FileType::DIR,
                 info.type)
    assert_equal("AB",
                 info.base_name)
    assert_equal(-1,
                 info.size)
    assert do
      info.mtime > 0
    end

    info = @fs.get_file_info("AB/CD/ghi")
    assert_equal(Arrow::FileType::FILE,
                 info.type)
    assert_equal("ghi",
                 info.base_name)
    assert_equal(9,
                 info.size)
    assert do
      info.mtime > 0
    end
  end

  def test_get_file_infos_paths
    mkpath("AB/CD")
    create_file("AB/CD/ghi", "some data")

    infos = @fs.get_file_infos_paths(["AB", "AB/CD/ghi"])
    assert_equal({
                   "AB" => -1,
                   "AB/CD/ghi" => 9
                 },
                 infos.map {|info| [info.path, info.size]}.to_h)
  end

  def test_get_file_infos_selector
    mkpath("AB/CD")
    create_file("abc", "data")
    create_file("AB/def", "some data")
    create_file("AB/CD/ghi", "some other data")

    selector = Arrow::FileSelector.new
    infos = @fs.get_file_infos_selector(selector)
    assert_equal({
                   "AB" => -1,
                   "abc" => 4
                 },
                 infos.map {|info| [info.path, info.size]}.to_h)

    selector.base_dir = "AB"
    infos = @fs.get_file_infos_selector(selector)
    assert_equal({
                   "AB/CD" => -1,
                   "AB/def" => 9
                 },
                 infos.map {|info| [info.path, info.size]}.to_h)
  end

  def test_get_file_infos_selector_with_recursion
    mkpath("AB/CD")
    create_file("abc", "data")
    create_file("AB/def", "some data")
    create_file("AB/CD/ghi", "some other data")

    selector = Arrow::FileSelector.new
    selector.recursive = true
    infos = @fs.get_file_infos_selector(selector)
    assert_equal({
                   "AB" => -1,
                   "AB/CD" => -1,
                   "AB/CD/ghi" => 15,
                   "AB/def" => 9,
                   "abc" => 4
                 },
                 infos.map {|info| [info.path, info.size]}.to_h)
  end

  def test_open_output_stream
    assert { not file?("abc") }
    stream = @fs.open_output_stream("abc")
    assert_equal(0, stream.tell)
    stream.write("some ")
    stream.write("data")
    stream.close
    assert { file?("abc") }
    assert_equal("some data",
                 read_file("abc"))

    stream = @fs.open_output_stream("abc")
    assert_equal(0, stream.tell)
    stream.write("other data")
    stream.close
    assert { file?("abc") }
    assert_equal("other data",
                 read_file("abc"))
  end

  def test_open_append_stream
    assert { not file?("abc") }
    stream = @fs.open_append_stream("abc")
    assert_equal(0, stream.tell)
    stream.write("some ")
    stream.close
    assert { file?("abc") }
    assert_equal("some ",
                 read_file("abc"))

    stream = @fs.open_append_stream("abc")
    assert_equal(5, stream.tell)
    stream.write("data")
    stream.close
    assert { file?("abc") }
    assert_equal("some data",
                 read_file("abc"))
  end

  def test_open_input_stream
    mkpath("AB")
    create_file("AB/abc", "some data")

    stream = @fs.open_input_stream("AB/abc")
    bytes = stream.read_bytes(4)
    assert_equal("some",
                 bytes.to_s)
    stream.close
  end

  def test_open_input_file
    create_file("ab", "some data")

    stream = @fs.open_input_file("ab")
    bytes = stream.read_at_bytes(5, 4)
    assert_equal("data",
                 bytes.to_s)
    stream.close
  end
end
