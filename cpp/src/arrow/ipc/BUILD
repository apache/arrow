load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "dictionary",
    srcs = ["dictionary.cc"],
    hdrs = ["dictionary.h"],
    deps = [
        "//arrow:array",
        "//arrow:status",
    ],
)

cc_library(
    name = "json_internal",
    srcs = ["json_internal.cc"],
    hdrs = ["json_internal.h"],
    deps = [
        ":dictionary",
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow:status",
        "//arrow/json:rapidjson_defs",
        "@rapidjson",
    ],
)

cc_library(
    name = "json_simple",
    srcs = ["json_simple.cc"],
    hdrs = ["json_simple.h"],
    deps = [
        ":json_internal",
        "//arrow:array",
        "//arrow:array_builder",
    ],
)

cc_library(
    name = "message",
    srcs = ["message.cc"],
    hdrs = ["message.h"],
    deps = [
        ":metadata_internal",
        ":options",
        "//arrow:buffer",
        "//arrow:status",
        "//arrow/io:interfaces",
        "//generated",
        "@flatbuffers",
    ],
)

cc_library(
    name = "message_hdr",
    hdrs = ["message.h"],
    deps = [":options"],
)

cc_library(
    name = "metadata_internal",
    srcs = ["metadata_internal.cc"],
    hdrs = ["metadata_internal.h"],
    deps = [
        ":dictionary",
        ":message_hdr",
        ":util",
        "//arrow:buffer",
        "//generated",
        "@flatbuffers",
    ],
)

cc_library(
    name = "options",
    srcs = ["options.cc"],
    hdrs = ["options.h"],
    deps = ["//arrow/util:visibility"],
)

cc_test(
    name = "read_write_test",
    srcs = ["read_write_test.cc"],
    deps = [
        ":message",
        ":reader",
        ":test_common",
        ":writer",
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow/io:file",
        "//arrow/io:memory",
        "//arrow/io:test_common",
        "//arrow/testing:gtest_util",
        "@flatbuffers",
        "@googletest//:gtest_main",
    ],
)


cc_library(
    name = "reader",
    srcs = ["reader.cc"],
    hdrs = ["reader.h"],
    deps = [
        ":dictionary",
        ":message",
        ":metadata_internal",
        ":options",
        "//arrow:array",
        "//arrow/io:memory",
        "//arrow/util:visibility",
    ],
)

cc_library(
    name = "test_common",
    testonly = True,
    srcs = ["test_common.cc"],
    hdrs = ["test_common.h"],
    deps = [
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow/testing:gtest_util",
    ],
)

cc_library(
    name = "util",
    hdrs = ["util.h"],
    deps = [
        "//arrow:array",
        "//arrow:status",
        "//arrow/io:interfaces",
    ],
)

cc_library(
    name = "writer",
    srcs = ["writer.cc"],
    hdrs = ["writer.h"],
    deps = [
        ":dictionary",
        ":message",
        ":metadata_internal",
        ":options",
        "//arrow:array",
        "//arrow:result_internal",
        "//arrow/io:memory",
        "//arrow/util:visibility",
    ],
)
