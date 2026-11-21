/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <memory>
#include <string>
#include <utility>

#include <gio/gio.h>

#include <arrow/filesystem/localfs.h>

#include <arrow-glib/error.h>
#include <arrow-glib/file-system.hpp>
#include <arrow-glib/local-file-system.h>

namespace {
  class TestFileSystem : public arrow::fs::LocalFileSystem {
  public:
    explicit TestFileSystem(std::string type_name) : type_name_(std::move(type_name)) {}

    std::string
    type_name() const override
    {
      return type_name_;
    }

  private:
    std::string type_name_;
  };

  void
  test_builtin()
  {
    std::shared_ptr<arrow::fs::FileSystem> file_system =
      std::make_shared<arrow::fs::LocalFileSystem>();
    auto wrapper = garrow_file_system_new_raw(&file_system);
    g_assert_true(GARROW_IS_LOCAL_FILE_SYSTEM(wrapper));
    g_assert_true(garrow_file_system_get_raw(wrapper) == file_system);
    g_object_unref(wrapper);
  }

  void
  test_registered()
  {
    auto wrapper_type = g_type_register_static_simple(GARROW_TYPE_FILE_SYSTEM,
                                                      "TestRegisteredFileSystem",
                                                      sizeof(GArrowFileSystemClass),
                                                      nullptr,
                                                      sizeof(GArrowFileSystem),
                                                      nullptr,
                                                      static_cast<GTypeFlags>(0));
    std::string type_name = "test-registered";
    GError *error = nullptr;
    g_assert_true(
      garrow_file_system_register_type(type_name.c_str(), wrapper_type, &error));
    g_assert_no_error(error);
    type_name = "changed-after-registration";

    std::shared_ptr<arrow::fs::FileSystem> file_system =
      std::make_shared<TestFileSystem>("test-registered");
    auto wrapper = garrow_file_system_new_raw(&file_system);
    g_assert_true(G_OBJECT_TYPE(wrapper) == wrapper_type);
    g_assert_true(garrow_file_system_get_raw(wrapper) == file_system);
    g_object_unref(wrapper);
  }

  void
  test_gio_registered()
  {
    std::shared_ptr<arrow::fs::FileSystem> file_system =
      std::make_shared<TestFileSystem>("test-gio-registered");
    auto fallback = garrow_file_system_new_raw(&file_system);
    g_assert_true(G_OBJECT_TYPE(fallback) == GARROW_TYPE_FILE_SYSTEM);
    g_object_unref(fallback);

    auto point = g_io_extension_point_lookup("arrow-file-system");
    g_assert_nonnull(point);
    g_assert_true(g_io_extension_point_get_required_type(point) ==
                  GARROW_TYPE_FILE_SYSTEM);
    auto wrapper_type = g_type_register_static_simple(GARROW_TYPE_FILE_SYSTEM,
                                                      "TestGIORegisteredFileSystem",
                                                      sizeof(GArrowFileSystemClass),
                                                      nullptr,
                                                      sizeof(GArrowFileSystem),
                                                      nullptr,
                                                      static_cast<GTypeFlags>(0));
    g_io_extension_point_implement("arrow-file-system",
                                   wrapper_type,
                                   "test-gio-registered",
                                   0);

    auto wrapper = garrow_file_system_new_raw(&file_system);
    g_assert_true(G_OBJECT_TYPE(wrapper) == wrapper_type);
    g_assert_true(garrow_file_system_get_raw(wrapper) == file_system);
    g_object_unref(wrapper);
  }

  void
  test_fallback()
  {
    std::shared_ptr<arrow::fs::FileSystem> file_system =
      std::make_shared<TestFileSystem>("test-unregistered");
    auto wrapper = garrow_file_system_new_raw(&file_system);
    g_assert_true(G_OBJECT_TYPE(wrapper) == GARROW_TYPE_FILE_SYSTEM);
    g_assert_true(garrow_file_system_get_raw(wrapper) == file_system);

    auto type_name = garrow_file_system_get_type_name(wrapper);
    g_assert_cmpstr(type_name, ==, "test-unregistered");
    g_free(type_name);

    GError *error = nullptr;
    auto info = garrow_file_system_get_file_info(wrapper, g_get_tmp_dir(), &error);
    g_assert_no_error(error);
    g_assert_nonnull(info);
    g_assert_true(garrow_file_info_is_dir(info));
    g_object_unref(info);
    g_object_unref(wrapper);
  }

  void
  test_abstract_type()
  {
    auto wrapper_type = g_type_register_static_simple(GARROW_TYPE_FILE_SYSTEM,
                                                      "TestAbstractFileSystem",
                                                      sizeof(GArrowFileSystemClass),
                                                      nullptr,
                                                      sizeof(GArrowFileSystem),
                                                      nullptr,
                                                      G_TYPE_FLAG_ABSTRACT);
    GError *error = nullptr;
    g_assert_false(
      garrow_file_system_register_type("test-abstract", wrapper_type, &error));
    g_assert_error(error, GARROW_ERROR, GARROW_ERROR_INVALID);
    g_clear_error(&error);
  }
} // namespace

int
main(int argc, char **argv)
{
  g_test_init(&argc, &argv, nullptr);
  g_test_add_func("/file-system/builtin", test_builtin);
  g_test_add_func("/file-system/registered", test_registered);
  g_test_add_func("/file-system/gio-registered", test_gio_registered);
  g_test_add_func("/file-system/fallback", test_fallback);
  g_test_add_func("/file-system/abstract-type", test_abstract_type);
  return g_test_run();
}
