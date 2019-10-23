load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "gtest_common",
    testonly = True,
    hdrs = ["gtest_common.h"],
)

cc_library(
    name = "gtest_util",
    testonly = True,
    srcs = ["gtest_util.cc"],
    hdrs = ["gtest_util.h"],
    deps = [
        ":util",
        "//arrow:array_builder",
        "//arrow:buffer",
        "//arrow/compute:kernel",
        "//arrow/ipc:json_simple",
        "@googletest//:gtest",
    ],
)

cc_library(
    name = "gtest_util_hdr",
    hdrs = [
        "gtest_util.h",
        "util.h",
    ],
    deps = [
        "//arrow:array_builder",
        "//arrow:buffer",
    ],
)

cc_library(
    name = "random",
    testonly = True,
    srcs = ["random.cc"],
    hdrs = ["random.h"],
    deps = [
        ":gtest_util_hdr",
        "//arrow:array",
        "//arrow/util:visibility",
        "@googletest//:gtest",
    ],
)


cc_library(
    name = "util",
    testonly = True,
    srcs = ["util.cc"],
    hdrs = ["util.h"],
    deps = [
        ":random",
        "//arrow:array_builder",
        "//arrow:buffer",
    ],
)
