load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//cpp/src/arrow/ipc:__pkg__"])

cc_library(
    name = "generated",
    hdrs = glob(["*.h"]),
    includes = ["."],
    deps = ["@flatbuffers"],
)
