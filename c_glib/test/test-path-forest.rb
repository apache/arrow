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

class TestPathForest < Test::Unit::TestCase
  def setup
    @file_infos = Array.new(3) { Arrow::FileInfo.new }
    @file_infos[0].type = Arrow::FileType::DIR
    @file_infos[0].path = "AA"

    @file_infos[1].type = Arrow::FileType::FILE
    @file_infos[1].path = "AA/aa"

    @file_infos[2].type = Arrow::FileType::FILE
    @file_infos[2].path = "bb"
  end

  sub_test_case("#size") do
    test("with single FileInfo") do
      forest = Arrow::PathForest.new(@file_infos[0,1])
      assert_equal(1, forest.size)
    end

    test("with two FileInfos") do
      forest = Arrow::PathForest.new(@file_infos[0,2])
      assert_equal(2, forest.size)
    end
  end

  sub_test_case("#==") do
    test("with the same FileInfos") do
      forest_a = Arrow::PathForest.new(@file_infos)
      forest_b = Arrow::PathForest.new(@file_infos)
      assert_equal(forest_a, forest_b)
    end

    test("with the same FileInfos but different order") do
      forest_a = Arrow::PathForest.new(@file_infos)
      forest_b = Arrow::PathForest.new(@file_infos.reverse)
      assert_equal(forest_a, forest_b)
    end
  end

  test("#!=") do
    forest_a = Arrow::PathForest.new(@file_infos[0, 1])
    forest_b = Arrow::PathForest.new(@file_infos[0, 2])
    assert_not_equal(forest_a, forest_b)
  end

  test("#to_s") do
  end

  test("#[]") do
    forest = Arrow::PathForest.new(@file_infos)
    node = forest[1]
    assert_equal({
                   type: Arrow::FileType::FILE,
                   path: "AA/aa",
                 },
                 {
                   type: node.type,
                   path: node.path,
                 })
  end

  sub_test_case("roots") do
    def setup
      super
      @forest = Arrow::PathForest.new(@file_infos)
    end

    test("info.path") do
      assert_equal(["AA", "bb"],
                   @forest.roots.map {|r| r.info.path })
    end

    test("n_descendants") do
      assert_equal([1, 0],
                   @forest.roots.map {|r| r.n_descendants })
    end

    test("descendants") do
      assert_equal([["AA/aa"], []],
                   @forest.roots.map {|r| r.descendants.infos.map {|i| i.path }})
    end
  end

  test("infos") do
    sorted_infos = @file_infos.sort_by {|fi| fi.path }
    forest = Arrow::PathForest.new(sorted_infos.reverse)
    # This always returns sorted FileInfos
    assert_equal(sorted_infos, forest.infos)
  end

  test("each") do
    forest = Arrow::PathForest.new(@file_infos)
    paths = []
    forest.each {|x| paths << x.info.path }
    assert_equal(["AA", "AA/aa", "bb"], paths)
  end
end
