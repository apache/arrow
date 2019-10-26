load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(default_visibility = ["//arrow:__pkg__"])

cc_library(
    name = "builder_adaptive",
    srcs = ["builder_adaptive.cc"],
    hdrs = ["builder_adaptive.h"],
    deps = [
        ":builder_base",
    ],
)

cc_library(
    name = "builder_base",
    srcs = ["builder_base.cc"],
    hdrs = ["builder_base.h"],
    deps = [
        "//arrow:array_hdrs",
        "//arrow:buffer",
        "//arrow:buffer_builder",
        "//arrow/util:int_util",
    ],
)

cc_library(
    name = "builder_binary",
    srcs = ["builder_binary.cc"],
    hdrs = ["builder_binary.h"],
    deps = [
        ":builder_base",
        "//arrow:array",
    ],
)

cc_library(
    name = "builder_decimal",
    srcs = ["builder_decimal.cc"],
    hdrs = ["builder_decimal.h"],
    deps = [
        ":builder_base",
        ":builder_binary",
    ],
)

cc_library(
    name = "builder_dict",
    srcs = [
        "builder_dict.cc",
        "dict_internal.h",
    ],
    hdrs = ["builder_dict.h"],
    deps = [
        ":builder_adaptive",
        ":builder_base",
        ":builder_primitive",
        "//arrow/util:hashing",
    ],
)

filegroup(
    name = "builder_hdrs",
    srcs = glob(["builder_*.h"]),
)

cc_library(
    name = "builder_nested",
    srcs = ["builder_nested.cc"],
    hdrs = ["builder_nested.h"],
    deps = [
        ":builder_base",
    ],
)

cc_library(
    name = "builder_primitive",
    srcs = ["builder_primitive.cc"],
    hdrs = ["builder_primitive.h"],
    deps = [":builder_base"],
)

cc_library(
    name = "builder_union",
    srcs = ["builder_union.cc"],
    hdrs = ["builder_union.h"],
    visibility = [
        "//arrow:__pkg__",
        "//arrow/python:__pkg__",
    ],
    deps = [
        ":builder_base",
    ],
)

cc_library(
    name = "concatenate",
    srcs = ["concatenate.cc"],
    hdrs = ["concatenate.h"],
    deps = [
        "//arrow:array_hdrs",
    ],
)

cc_test(
    name = "concatenate_test",
    srcs = ["concatenate_test.cc"],
    deps = [
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow/compute:context",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
        "@flatbuffers",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "dict_internal",
    srcs = ["dict_internal.cc"],
    hdrs = ["dict_internal.h"],
    visibility = [
        "//arrow:__pkg__",
        "//arrow/compute/kernels:__pkg__",
    ],
    deps = [":builder_dict"],
)

cc_library(
    name = "diff",
    srcs = ["diff.cc"],
    hdrs = ["diff.h"],
    deps = [
        "//arrow:array_hdrs",
        "//arrow:buffer_builder",
        "//arrow/util:range",
    ],
)

cc_test(
    name = "diff_test",
    srcs = ["diff_test.cc"],
    deps = [
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow/compute:context",
        "//arrow/compute/kernels:filter",
        "//arrow/io:file",
        "//arrow/io:memory",
        "//arrow/io:test_common",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
        "@flatbuffers",
        "@googletest//:gtest_main",
    ],
)
