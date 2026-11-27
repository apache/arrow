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
  class RegisteredFileSystem < Arrow::FileSystem
    type_register
  end

  class DuplicateFileSystem < Arrow::FileSystem
    type_register
  end

  sub_test_case(".register_type") do
    def test_valid
      assert_true(Arrow::FileSystem.register_type("test-file-system-valid",
                                                  RegisteredFileSystem.gtype))
    end

    def test_duplicate
      type_name = "test-file-system-duplicate"
      Arrow::FileSystem.register_type(type_name, DuplicateFileSystem.gtype)
      assert_raise(Arrow::Error::Key) do
        Arrow::FileSystem.register_type(type_name, Arrow::LocalFileSystem.gtype)
      end
    end

    def test_type_alias
      assert_raise(Arrow::Error::Key) do
        Arrow::FileSystem.register_type("test-file-system-alias",
                                        Arrow::MockFileSystem.gtype)
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

  sub_test_case(".ensure_finalized") do
    def test_create_after_finalization
      # Finalization affects the whole process, so isolate it from the test runner.
      pid = spawn(RbConfig.ruby, "-e", <<-SCRIPT)
require "gi"
require "test/unit/assertions"

include Test::Unit::Assertions

Gio = GI.load("Gio")
Arrow = GI.load("Arrow")

Arrow::FileSystem.create("mock://")
Arrow::FileSystem.ensure_finalized
assert_raise(Arrow::Error::Invalid) do
  Arrow::FileSystem.create("mock://")
end
      SCRIPT
      _, status = Process.wait2(pid)
      assert_predicate(status, :success?)
    end
  end
end
