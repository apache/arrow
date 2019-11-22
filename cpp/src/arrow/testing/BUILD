load("@rules_cc//cc:defs.bzl", "cc_test")
load("//bazel:arrow_cc_library.bzl", "arrow_cc_library")

package(default_visibility = ["//visibility:public"])

arrow_cc_library(
    name = "gtest_common",
    testonly = True,
    hdrs = ["gtest_common.h"],
)

arrow_cc_library(
    name = "gtest_util",
    testonly = True,
    srcs = ["gtest_util.cc"],
    hdrs = ["gtest_util.h"],
    deps = [
        ":util",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow:buffer",
        "//cpp/src/arrow/compute:kernel",
        "//cpp/src/arrow/ipc:json_simple",
        "@googletest//:gtest",
    ],
)

arrow_cc_library(
    name = "gtest_util_hdr",
    hdrs = [
        "gtest_util.h",
        "util.h",
    ],
    deps = [
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow:buffer",
    ],
)

arrow_cc_library(
    name = "random",
    testonly = True,
    srcs = ["random.cc"],
    hdrs = ["random.h"],
    deps = [
        ":gtest_util_hdr",
        "//cpp/src/arrow:array",
        "//cpp/src/arrow/util:visibility",
        "@googletest//:gtest",
    ],
)


arrow_cc_library(
    name = "util",
    testonly = True,
    srcs = ["util.cc"],
    hdrs = ["util.h"],
    deps = [
        ":random",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow:buffer",
    ],
)
