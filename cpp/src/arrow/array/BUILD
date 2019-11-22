load("@rules_cc//cc:defs.bzl", "cc_test")
load("//bazel:arrow_cc_library.bzl", "arrow_cc_library")

package(default_visibility = ["//cpp/src/arrow:__pkg__"])

arrow_cc_library(
    name = "builder_adaptive",
    srcs = ["builder_adaptive.cc"],
    hdrs = ["builder_adaptive.h"],
    deps = [
        ":builder_base",
    ],
)

arrow_cc_library(
    name = "builder_base",
    srcs = ["builder_base.cc"],
    hdrs = ["builder_base.h"],
    deps = [
        "//cpp/src/arrow:array_hdrs",
        "//cpp/src/arrow:buffer",
        "//cpp/src/arrow:buffer_builder",
        "//cpp/src/arrow/util:int_util",
    ],
)

arrow_cc_library(
    name = "builder_binary",
    srcs = ["builder_binary.cc"],
    hdrs = ["builder_binary.h"],
    deps = [
        ":builder_base",
        "//cpp/src/arrow:array",
    ],
)

arrow_cc_library(
    name = "builder_decimal",
    srcs = ["builder_decimal.cc"],
    hdrs = ["builder_decimal.h"],
    deps = [
        ":builder_base",
        ":builder_binary",
    ],
)

arrow_cc_library(
name = "dict_internal_hdr",
hdrs = ["dict_internal.h"],
)

arrow_cc_library(
    name = "builder_dict",
    srcs = [
        "builder_dict.cc",
    ],
    hdrs = ["builder_dict.h"],
    deps = [
        ":builder_adaptive",
        ":builder_base",
        ":builder_primitive",
        "//cpp/src/arrow/util:hashing",
":dict_internal_hdr"
    ],
)

filegroup(
    name = "builder_hdrs",
    srcs = glob(["builder_*.h"]),
)

arrow_cc_library(
    name = "builder_nested",
    srcs = ["builder_nested.cc"],
    hdrs = ["builder_nested.h"],
    deps = [
        ":builder_base",
    ],
)

arrow_cc_library(
    name = "builder_primitive",
    srcs = ["builder_primitive.cc"],
    hdrs = ["builder_primitive.h"],
    deps = [":builder_base"],
)

arrow_cc_library(
    name = "builder_union",
    srcs = ["builder_union.cc"],
    hdrs = ["builder_union.h"],
    visibility = [
        "//cpp/src/arrow:__pkg__",
        "//cpp/src/arrow/python:__pkg__",
    ],
    deps = [
        ":builder_base",
    ],
)

arrow_cc_library(
    name = "concatenate",
    srcs = ["concatenate.cc"],
    hdrs = ["concatenate.h"],
    deps = [
        "//cpp/src/arrow:array_hdrs",
    ],
)

cc_test(
    name = "concatenate_test",
    srcs = ["concatenate_test.cc"],
    deps = [
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow/compute:context",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@flatbuffers",
        "@googletest//:gtest_main",
    ],
)

arrow_cc_library(
    name = "dict_internal",
    srcs = ["dict_internal.cc"],
    hdrs = ["dict_internal.h"],
    visibility = [
        "//cpp/src/arrow:__pkg__",
        "//cpp/src/arrow/compute/kernels:__pkg__",
    ],
    deps = [":builder_dict"],
)

arrow_cc_library(
    name = "diff",
    srcs = ["diff.cc"],
    hdrs = ["diff.h"],
    deps = [
        "//cpp/src/arrow:array_hdrs",
        "//cpp/src/arrow:buffer_builder",
        "//cpp/src/arrow/util:range",
    ],
)

cc_test(
    name = "diff_test",
    srcs = ["diff_test.cc"],
    deps = [
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow/compute:context",
        "//cpp/src/arrow/compute/kernels:filter",
        "//cpp/src/arrow/io:file",
        "//cpp/src/arrow/io:memory",
        "//cpp/src/arrow/io:test_common",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@flatbuffers",
        "@googletest//:gtest_main",
    ],
)
