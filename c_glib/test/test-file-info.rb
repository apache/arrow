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

class TestFileInfo < Test::Unit::TestCase
  def setup
    @file_info = Arrow::FileInfo.new
  end

  sub_test_case("#type") do
    test("default") do
      assert_equal(Arrow::FileType::UNKNOWN,
                   @file_info.type)
    end
  end

  test("#type=") do
    @file_info.type = :dir
    assert_equal(Arrow::FileType::DIR,
                 @file_info.type)
  end

  sub_test_case("#path") do
    test("default") do
      assert_equal("", @file_info.path)
    end
  end

  test("#path=") do
    @file_info.path = "/a/b/c.d"
    assert_equal("/a/b/c.d",
                 @file_info.path)
  end

  sub_test_case("#base_name") do
    test("default") do
      assert_equal("", @file_info.base_name)
    end

    test("with directory") do
      @file_info.path = "/a/b/c.d"
      assert_equal("c.d", @file_info.base_name)
    end
  end

  sub_test_case("#dir_name") do
    test("default") do
      assert_equal("", @file_info.dir_name)
    end

    test("with directory") do
      @file_info.path = "/a/b/c.d"
      assert_equal("/a/b", @file_info.dir_name)
    end
  end

  sub_test_case("#extension") do
    test("default") do
      assert_equal("", @file_info.extension)
    end

    test("exist") do
      @file_info.path = "/a/b/c.d"
      assert_equal("d", @file_info.extension)
    end
  end

  sub_test_case("#size") do
    test("default") do
      assert_equal(-1, @file_info.size)
    end
  end

  sub_test_case("#mtime") do
    test("default") do
      assert_equal(-1, @file_info.mtime)
    end
  end

  sub_test_case("#==") do
    def setup
      super
      @other_file_info = Arrow::FileInfo.new
    end

    test("all the properties are the same") do
      assert do
        @file_info == @other_file_info
      end
    end

    test("the different type") do
      @other_file_info.type = Arrow::FileType::FILE
      assert do
        @file_info != @other_file_info
      end
    end

    test("the different path") do
      @other_file_info.path = "/a/b/c"
      assert do
        @file_info != @other_file_info
      end
    end

    test("the different size") do
      @other_file_info.size = 42
      assert do
        @file_info != @other_file_info
      end
    end

    test("the different mtime") do
      @other_file_info.mtime = Time.now.to_i
      assert do
        @file_info != @other_file_info
      end
    end
  end

  sub_test_case("#file?") do
    test("true") do
      @file_info.type = :file
      assert do
        @file_info.file?
      end
    end

    test("false") do
      @file_info.type = :dir
      assert do
        not @file_info.file?
      end
    end
  end

  sub_test_case("#dir?") do
    test("true") do
      @file_info.type = :dir
      assert do
        @file_info.dir?
      end
    end

    test("false") do
      @file_info.type = :file
      assert do
        not @file_info.dir?
      end
    end
  end

  test("#to_s") do
    assert_equal("FileInfo(FileType::Unknown, , -1, -1)",
                 @file_info.to_s)
  end
end
