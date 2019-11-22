load("//bazel:arrow_cc_library.bzl", "arrow_cc_library")
package(default_visibility = ["//cpp/src/arrow/python:__pkg__"])

arrow_cc_library(
  name = "test_main",
  srcs = ["test_main.cc"],
  deps = ["//cpp/src/arrow/python:platform",
          "//cpp/src/arrow/python:datetime",
          "//cpp/src/arrow/python:init",
          "@googletest//:gtest"])

