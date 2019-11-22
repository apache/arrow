load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "dictionary",
    srcs = ["dictionary.cc"],
    hdrs = ["dictionary.h"],
    deps = [
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:status",
    ],
)

cc_library(
    name = "feather",
    srcs = ["feather.cc"],
    hdrs = ["feather.h"],
    deps = [
        ":feather_internal",
        ":util",
        "//cpp/src/arrow:array",
        "//cpp/src/arrow/util:visibility",
        "//cpp/src/generated",
        "@flatbuffers",
    ],
)

cc_library(
    name = "feather_internal",
    hdrs = ["feather_internal.h"],
)

cc_library(
    name = "json_internal",
    srcs = ["json_internal.cc"],
    hdrs = ["json_internal.h"],
    deps = [
        ":dictionary",
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow:status",
        "//cpp/src/arrow/json:rapidjson_defs",
        "@rapidjson",
    ],
)

cc_library(
    name = "json_simple",
    srcs = ["json_simple.cc"],
    hdrs = ["json_simple.h"],
    deps = [
        ":json_internal",
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
    ],
)

cc_library(
    name = "message",
    srcs = ["message.cc"],
    hdrs = ["message.h"],
    deps = [
        ":metadata_internal",
        ":options",
        "//cpp/src/arrow:buffer",
        "//cpp/src/arrow:status",
        "//cpp/src/arrow/io:interfaces",
        "//cpp/src/generated",
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
        "//cpp/src/arrow:buffer",
        "//cpp/src/generated",
        "@flatbuffers",
    ],
)

cc_library(
    name = "options",
    srcs = ["options.cc"],
    hdrs = ["options.h"],
    deps = ["//cpp/src/arrow/util:visibility"],
)

cc_library(
    name = "public_api",
    hdrs = ["api.h"],
    deps = [
        ":feather",
        ":message",
        ":reader",
        ":writer",
    ],
)

cc_test(
    name = "read_write_test",
    srcs = ["read_write_test.cc"],
    deps = [
        ":message",
        ":reader",
        ":test_common",
        ":writer",
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow/io:file",
        "//cpp/src/arrow/io:memory",
        "//cpp/src/arrow/io:test_common",
        "//cpp/src/arrow/testing:gtest_util",
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
        "//cpp/src/arrow:array",
        "//cpp/src/arrow/io:memory",
        "//cpp/src/arrow/util:visibility",
    ],
)

cc_library(
    name = "test_common",
    testonly = True,
    srcs = ["test_common.cc"],
    hdrs = ["test_common.h"],
    deps = [
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow/testing:gtest_util",
    ],
)

cc_library(
    name = "util",
    hdrs = ["util.h"],
    deps = [
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:status",
        "//cpp/src/arrow/io:interfaces",
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
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:result_internal",
        "//cpp/src/arrow/io:memory",
        "//cpp/src/arrow/util:visibility",
    ],
)
