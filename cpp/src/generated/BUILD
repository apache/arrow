load("//bazel::arrow_cc_library.bzl", arrow_cc_library)

package(default_visibility = ["//cpp/src/arrow/ipc:__pkg__"])

cc_library(
    name = "generated",
    hdrs = glob(["*.h"]),
    includes = ["."],
    deps = ["@flatbuffers"],
)
