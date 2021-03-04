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

class TestFileSelector < Test::Unit::TestCase
  def setup
    @file_selector = Arrow::FileSelector.new
  end

  sub_test_case("#base_dir") do
    test("default") do
      assert do
        "" == @file_selector.base_dir
      end
    end
  end

  test("#base_dir=") do
    @file_selector.base_dir = "/a/b"
    assert do
      "/a/b" == @file_selector.base_dir
    end
  end

  sub_test_case("#allow_not_found?") do
    test("default") do
      assert do
        not @file_selector.allow_not_found?
      end
    end
  end

  test("#allow_not_found=") do
    @file_selector.allow_not_found = true
    assert do
      @file_selector.allow_not_found?
    end
  end

  sub_test_case("#recursive?") do
    test("default") do
      assert do
        false == @file_selector.recursive?
      end
    end
  end

  test("#recursive=") do
    @file_selector.recursive = true
      assert do
        true == @file_selector.recursive?
      end
  end

  sub_test_case("#max_recursion") do
    test("default") do
      assert do
        (1<<31) - 1 == @file_selector.max_recursion
      end
    end
  end

  test("#max_recursion=") do
    @file_selector.max_recursion = 42
    assert do
      42 == @file_selector.max_recursion
    end
  end
end
