load("@rules_cc//cc:defs.bzl", "cc_test")
load("//bazel:arrow_cc_library.bzl", "arrow_cc_library")

arrow_cc_library(
    name = "datetime",
    hdrs = ["datetime.h"],
    visibility = ["//visibility:public"],
    deps = ["//cpp/src/arrow/vendored/datetime"],
)

arrow_cc_library(
    name = "string_view",
    hdrs = ["string_view.hpp"],
    visibility = ["//cpp/src/arrow/util:__pkg__"],
)


arrow_cc_library(
    name = "variant",
    hdrs = ["variant.hpp"],
    visibility = ["//cpp/src/arrow/util:__pkg__"],
)

arrow_cc_library(
    name = "xxhash",
    hdrs = ["xxhash.h"],
    visibility = ["//visibility:public"],
    deps = ["//cpp/src/arrow/vendored/xxhash"],
)
