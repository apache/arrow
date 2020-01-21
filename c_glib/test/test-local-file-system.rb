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

require "fileutils"
require "tmpdir"

module GenericFileSystemTestMethods
  private def all_entries
    selector = Arrow::FileSelector.new
    selector.base_dir = ""
    selector.recursive = true
    stats = @fs.get_target_stats_selector(selector)
    stats.map {|stat|
      case stat.type
      when Arrow::FileType::DIRECTORY
        [stat.path, :directory]
      when Arrow::FileType::FILE
        [stat.path, :file]
      else
        nil
      end
    }.compact.to_h
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
    size = @fs.get_target_stats_path(path).size
    buffer = stream.read(size)
    stream.close
    buffer.data.to_s
  end

  private def file?(path)
    st = @fs.get_target_stats_path(path)
    st.type == Arrow::FileType::FILE
  rescue Arrow::Error::Io
    false
  end

  private def directory?(path)
    st = @fs.get_target_stats_path(path)
    st.type == Arrow::FileType::DIRECTORY
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
                   "AB" => :directory,
                   "AB/CD" => :directory,
                   "AB/CD/EF" => :directory,
                   "AB/GH" => :directory
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
                   "AB" => :directory,
                   "AB/GH" => :directory,
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
                   "AB" => :directory,
                   "AB/CD" => :directory,
                   "AB/GH" => :directory,
                   "AB/GH/IJ" => :directory,
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
                   "AB" => :directory,
                   "AB/def" => :file,
                   "AB/ghi" => :file,
                   "AB/jkl" => :file,
                   "AB/mno" => :file,
                   "abc" => :file
                 },
                 all_entries)

    @fs.delete_files(["abc", "AB/def"], 2)
    assert_equal({
                   "AB" => :directory,
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
                   "AB" => :directory,
                   "AB/CD" => :directory,
                   "EF" => :directory,
                   "abc" => :file
                 },
                 all_entries)

    @fs.move("abc", "AB/CD/ghi")
    assert_equal({
                   "AB" => :directory,
                   "AB/CD" => :directory,
                   "EF" => :directory,
                   "AB/CD/ghi" => :file
                 },
                 all_entries)
  end

  def test_move_dir
    omit("move_dir is not allowed") if @do_not_allow_move_dir

    mkpath("AB/CD")
    mkpath("EF")
    assert_equal({
                   "AB" => :directory,
                   "AB/CD" => :directory,
                   "EF" => :directory
                 },
                 all_entries)

    @fs.move("AB", "GH")
    assert_equal({
                   "EF" => :directory,
                   "GH" => :directory,
                   "GH/CD" => :directory
                 },
                 all_entries)
  end

  def test_copy_file
    mkpath("AB/CD")
    mkpath("EF")
    create_file("AB/abc", "data")
    assert_equal({
                   "AB" => :directory,
                   "AB/CD" => :directory,
                   "EF" => :directory,
                   "AB/abc" => :file
                 },
                 all_entries)

    @fs.copy_file("AB/abc", "def")
    assert_equal({
                   "AB" => :directory,
                   "AB/CD" => :directory,
                   "EF" => :directory,
                   "AB/abc" => :file,
                   "def" => :file
                 },
                 all_entries)
    assert_equal("data",
                 read_file("def"))
  end

  def test_get_target_stats_path
    mkpath("AB/CD")
    create_file("AB/CD/ghi", "some data")

    st = @fs.get_target_stats_path("AB")
    assert_equal(Arrow::FileType::DIRECTORY,
                 st.type)
    assert_equal("AB",
                 st.base_name)
    assert_equal(-1,
                 st.size)
    assert do
      st.mtime > 0
    end

    st = @fs.get_target_stats_path("AB/CD/ghi")
    assert_equal(Arrow::FileType::FILE,
                 st.type)
    assert_equal("ghi",
                 st.base_name)
    assert_equal(9,
                 st.size)
    assert do
      st.mtime > 0
    end
  end

  def test_get_target_stats_paths
    mkpath("AB/CD")
    create_file("AB/CD/ghi", "some data")

    sts = @fs.get_target_stats_paths(["AB", "AB/CD/ghi"], 2)
    assert_equal({
                   "AB" => -1,
                   "AB/CD/ghi" => 9
                 },
                 sts.map {|st| [st.path, st.size] }.to_h)
  end

  def test_get_target_stats_selector
    mkpath("AB/CD")
    create_file("abc", "data")
    create_file("AB/def", "some data")
    create_file("AB/CD/ghi", "some other data")

    selector = Arrow::FileSelector.new
    sts = @fs.get_target_stats_selector(selector)
    sts = sts.sort_by {|st| st.path }
    assert_equal({
                   "AB" => -1,
                   "abc" => 4
                 },
                 sts.map {|st| [st.path, st.size] }.to_h)

    selector.base_dir = "AB"
    sts = @fs.get_target_stats_selector(selector)
    sts = sts.sort_by {|st| st.path }
    assert_equal({
                   "AB/CD" => -1,
                   "AB/def" => 9
                 },
                 sts.map {|st| [st.path, st.size] }.to_h)
  end

  def test_get_target_stats_selector_with_recursion
    mkpath("AB/CD")
    create_file("abc", "data")
    create_file("AB/def", "some data")
    create_file("AB/CD/ghi", "some other data")

    selector = Arrow::FileSelector.new
    selector.recursive = true
    sts = @fs.get_target_stats_selector(selector)
    sts = sts.sort_by {|st| st.path }
    assert_equal({
                   "AB" => -1,
                   "AB/CD" => -1,
                   "AB/CD/ghi" => 15,
                   "AB/def" => 9,
                   "abc" => 4
                 },
                 sts.map {|st| [st.path, st.size] }.to_h)
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
    assert_equal("some",
                 stream.read(4).data.to_s)
    stream.close
  end

  def test_open_input_file
    omit("TODO: Not implemented")
  end
end

class TestLocalFileSystem < Test::Unit::TestCase
  def setup
    @tmpdir = Dir.mktmpdir
    @options ||= Arrow::LocalFileSystemOptions.defaults
    @local_fs = Arrow::LocalFileSystem.new(@options)
    @fs = Arrow::SubTreeFileSystem.new(@tmpdir, @local_fs)
  end

  def options
    Arrow::LocalFileSystemOptions.defaults
  end

  sub_test_case("do not use mmap") do
    include GenericFileSystemTestMethods
  end

  sub_test_case("use mmap") do
    def setup
      @options = Arrow::LocalFileSystemOptions.defaults
      @options.use_mmap = true
      super
    end

    include GenericFileSystemTestMethods
  end
end
