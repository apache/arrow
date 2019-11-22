load("@rules_cc//cc:defs.bzl", "cc_test")
load("//bazel:arrow_cc_library.bzl", "arrow_cc_library")

package(default_visibility = ["//visibility:public"])

arrow_cc_library(
    name = "rapidjson_defs",
    hdrs = ["rapidjson_defs.h"],
    deps = ["//cpp/src/arrow/util:sse_util"],
)


arrow_cc_library(
  name = "options",
  hdrs = ["options.h"])

arrow_cc_library(
  name = "reader",
  hdrs = ["reader.h"])
