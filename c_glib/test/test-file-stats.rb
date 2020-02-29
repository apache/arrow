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

class TestFileStats < Test::Unit::TestCase
  def setup
    @file_stats = Arrow::FileStats.new
  end

  sub_test_case("#type") do
    test("default") do
      assert_equal(Arrow::FileType::UNKNOWN,
                   @file_stats.type)
    end
  end

  test("#type=") do
    @file_stats.type = :directory
    assert_equal(Arrow::FileType::DIRECTORY,
                 @file_stats.type)
  end

  sub_test_case("#path") do
    test("default") do
      assert_equal("", @file_stats.path)
    end
  end

  test("#path=") do
    @file_stats.path = "/a/b/c.d"
    assert_equal("/a/b/c.d",
                 @file_stats.path)
  end

  sub_test_case("#base_name") do
    test("default") do
      assert_equal("", @file_stats.base_name)
    end

    test("with directory") do
      @file_stats.path = "/a/b/c.d"
      assert_equal("c.d", @file_stats.base_name)
    end
  end

  sub_test_case("#directory_name") do
    test("default") do
      assert_equal("", @file_stats.directory_name)
    end

    test("with directory") do
      @file_stats.path = "/a/b/c.d"
      assert_equal("/a/b", @file_stats.directory_name)
    end
  end

  sub_test_case("#extension") do
    test("default") do
      assert_equal("", @file_stats.extension)
    end

    test("exist") do
      @file_stats.path = "/a/b/c.d"
      assert_equal("d", @file_stats.extension)
    end
  end

  sub_test_case("#size") do
    test("default") do
      assert_equal(-1, @file_stats.size)
    end
  end

  sub_test_case("#mtime") do
    test("default") do
      assert_equal(-1, @file_stats.mtime)
    end
  end

  sub_test_case("#==") do
    def setup
      super
      @other_file_stats = Arrow::FileStats.new
    end

    test("all the properties are the same") do
      assert do
        @file_stats == @other_file_stats
      end
    end

    test("the different type") do
      @other_file_stats.type = Arrow::FileType::FILE
      assert do
        @file_stats != @other_file_stats
      end
    end

    test("the different path") do
      @other_file_stats.path = "/a/b/c"
      assert do
        @file_stats != @other_file_stats
      end
    end

    test("the different size") do
      @other_file_stats.size = 42
      assert do
        @file_stats != @other_file_stats
      end
    end

    test("the different mtime") do
      @other_file_stats.mtime = Time.now.to_i
      assert do
        @file_stats != @other_file_stats
      end
    end
  end

  sub_test_case("#file?") do
    test("true") do
      @file_stats.type = :file
      assert do
        @file_stats.file?
      end
    end

    test("false") do
      @file_stats.type = :directory
      assert do
        not @file_stats.file?
      end
    end
  end

  sub_test_case("#directory?") do
    test("true") do
      @file_stats.type = :directory
      assert do
        @file_stats.directory?
      end
    end

    test("false") do
      @file_stats.type = :file
      assert do
        not @file_stats.directory?
      end
    end
  end
end
