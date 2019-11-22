load("@rules_cc//cc:defs.bzl", "cc_test")
load("//bazel:arrow_cc_library.bzl", "arrow_cc_library")

package(default_visibility = ["//cpp/src/arrow/ipc:__pkg__"])

arrow_cc_library(
    name = "generated",
    hdrs = glob(["*.h"]),
    includes = ["."],
    deps = ["@flatbuffers"],
)
