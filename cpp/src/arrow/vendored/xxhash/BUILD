load("//bazel:arrow_cc_library.bzl", "arrow_cc_library")

arrow_cc_library(
  name = "xxh3",
  hdrs = ["xxh3.h"],
  copts = ["-I."],
    visibility = ["//cpp/src/arrow/vendored:__pkg__"],
)

cc_library(
  name = "xxhash_hdr",
  hdrs = ["xxhash.h"],
  strip_include_prefix = "/cpp/src/arrow/vendored/xxhash",
)



arrow_cc_library(
    name = "xxhash",
    srcs = [
        "xxh3.h",
    ],
    hdrs = [
        "xxhash.c",
        "xxhash.h"
    ],
    deps = [":xxh3", ":xxhash_hdr"],
    
    visibility = ["//cpp/src/arrow/vendored:__pkg__"],
)

