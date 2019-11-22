load("@rules_cc//cc:defs.bzl", "cc_library", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "buffered",
    srcs = ["buffered.cc"],
    hdrs = ["buffered.h"],
    deps = [
        ":concurrency",
        "//cpp/src/arrow:buffer",
        "//cpp/src/arrow:status",
    ],
)

cc_library(
    name = "compressed",
    srcs = ["compressed.cc"],
    hdrs = ["compressed.h"],
    deps = [
        ":concurrency",
        "//cpp/src/arrow:buffer",
        "//cpp/src/arrow/util:compression",
    ],
)

cc_library(
    name = "concurrency",
    hdrs = ["concurrency.h"],
    deps = [
        ":interfaces_hdr",
        "//cpp/src/arrow:status",
        "//cpp/src/arrow/util:checked_cast",
    ],
)

cc_library(
    name = "file",
    srcs = ["file.cc"],
    hdrs = ["file.h"],
    deps = [
        ":concurrency",
        ":util_internal",
        "//cpp/src/arrow:buffer",
        "//cpp/src/arrow:status",
        "//cpp/src/arrow/util:memory",
    ],
)

cc_test(
    name = "file_test",
    srcs = ["file_test.cc"],
    deps = [
        ":file",
        ":test_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "hdfs",
    hdrs = ["hdfs.h"],
)

cc_library(
    name = "interfaces",
    srcs = ["interfaces.cc"],
    hdrs = ["interfaces.h"],
    deps = [
        ":concurrency",
        "//cpp/src/arrow:buffer",
        "//cpp/src/arrow:type_fwd",
        "//cpp/src/arrow/util:iterator",
        "//cpp/src/arrow/util:macros",
        "//cpp/src/arrow/util:string_view",
        "//cpp/src/arrow/util:visibility",
    ],
)

cc_library(
    name = "interfaces_hdr",
    hdrs = ["interfaces.h"],
    deps =
        [
            ":util_internal",
            "//cpp/src/arrow:type_fwd",
            "//cpp/src/arrow/util:macros",
            "//cpp/src/arrow/util:string_view",
            "//cpp/src/arrow/util:visibility",
        ],
)

cc_library(
    name = "memory",
    srcs = ["memory.cc"],
    hdrs = ["memory.h"],
    deps = [
        ":concurrency",
        ":util_internal",
        "//cpp/src/arrow:buffer",
        "//cpp/src/arrow:status",
        "//cpp/src/arrow/util:memory",
    ],
)

cc_library(
    name = "public_api",
    hdrs = ["api.h"],
    deps = [
        ":buffered",
        ":compressed",
        ":file",
        ":hdfs",
        ":memory",
    ],
)

cc_library(
    name = "test_common",
    testonly = True,
    srcs = ["test_common.cc"],
    hdrs = ["test_common.h"],
    deps = [
        ":buffered",
        ":file",
        ":memory",
        "//cpp/src/arrow/testing:gtest_util",
    ],
)

cc_library(
    name = "util_internal",
    hdrs = ["util_internal.h"],
)
