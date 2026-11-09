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

class TestFileSystem < Test::Unit::TestCase
  sub_test_case("register_type") do
    def test_valid
      assert_true(Arrow::FileSystem.register_type("test-file-system-valid",
                                                  Arrow::MockFileSystem.gtype))
    end

    def test_duplicate
      type_name = "test-file-system-duplicate"
      Arrow::FileSystem.register_type(type_name, Arrow::MockFileSystem.gtype)
      assert_raise(Arrow::Error::Key) do
        Arrow::FileSystem.register_type(type_name, Arrow::LocalFileSystem.gtype)
      end
    end

    def test_empty_name
      assert_raise(Arrow::Error::Invalid) do
        Arrow::FileSystem.register_type("", Arrow::MockFileSystem.gtype)
      end
    end

    def test_unrelated_type
      assert_raise(Arrow::Error::Invalid) do
        Arrow::FileSystem.register_type("test-file-system-unrelated",
                                        Arrow::Int32Array.gtype)
      end
    end
  end

  def test_create_mock
    assert_instance_of(Arrow::MockFileSystem, Arrow::FileSystem.create("mock:///"))
  end
end
