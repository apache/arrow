load("@rules_cc//cc:defs.bzl", "cc_library")
load("//bazel:template.bzl", "template_rule")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "atomic_shared_ptr",
    hdrs = ["atomic_shared_ptr.h"],
)

cc_library(
    name = "basic_decimal",
    srcs = ["basic_decimal.cc"],
    hdrs = ["basic_decimal.h"],
    deps = [
        ":bit_util",
        ":int_util",
        "//cpp/src/arrow:status",
    ],
)

cc_library(
    name = "bit_util",
    srcs = [
        "align_util.h",
        "bit_util.cc",
    ],
    hdrs = ["bit_util.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":string_view",
        ":type_traits",
        "//cpp/src/arrow:buffer_hdr",
    ],
)

cc_library(
    name = "checked_cast",
    hdrs = ["checked_cast.h"],
)

cc_library(
    name = "compare",
    hdrs = ["compare.h"],
    deps = [":macros"],
)

cc_library(
    name = "compression",
    srcs = ["compression.cc"],
    hdrs = ["compression.h"],
    deps = [
        ":visibility",
        "//cpp/src/arrow:status",
    ],
)

cc_library(
    name = "config",
    hdrs = [":config_gen"],
)

template_rule(
    name = "config_gen",
    src = "config.h.cmake",
    out = "config.h",
    substitutions = {
        "@ARROW_VERSION_MAJOR@": "1",
        "@ARROW_VERSION_MINOR@": "0",
        "@ARROW_VERSION_PATCH@": "0",
        "#cmakedefine DOUBLE_CONVERSION_HAS_CASE_INSENSIBILITY": "#define DOUBLE_CONVERSION_HAS_CASE_INSENSIBILITY",
        "#cmakedefine GRPCPP_PP_INCLUDE": "",
    },
)

cc_library(
    name = "cpu_info",
    srcs = ["cpu_info.cc"],
    hdrs = ["cpu_info.h"],
    deps = [
        ":logging",
        ":string",
        ":visibility",
    ],
)

cc_library(
    name = "decimal",
    srcs = ["decimal.cc"],
    hdrs = ["decimal.h"],
    deps = [
        ":basic_decimal",
        ":bit_util",
        ":int_util",
        ":parsing",
        "//cpp/src/arrow:status",
    ],
)

cc_library(
    name = "formatting",
    srcs = ["formatting.cc"],
    hdrs = ["formatting.h"],
    deps = [
        ":utf8",
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:status",
        "@double-conversion//:double-conversion",
    ],
)

cc_library(
    name = "functional",
    hdrs = ["functional.h"],
    deps = [":macros"],
)

cc_library(
    name = "hashing",
    hdrs = ["hashing.h"],
    deps = [
        "//cpp/src/arrow:builder_hdr",
        "//cpp/src/arrow/vendored:xxhash",
    ],
)

cc_library(
    name = "int_util",
    srcs = ["int_util.cc"],
    hdrs = ["int_util.h"],
    deps = [
        ":bit_util",
        ":visibility",
    ],
)

cc_library(
    name = "io_util",
    srcs = ["io_util.cc"],
    hdrs = ["io_util.h"],
    deps = [
        ":logging",
        ":macros",
        ":windows_compatibility",
        "//cpp/src/arrow:buffer",
        "//cpp/src/arrow:status",
        "//cpp/src/arrow/io:interfaces",
    ],
)

cc_library(
    name = "iterator",
    srcs = ["iterator.cc"],
    hdrs = ["iterator.h"],
    deps = [
        ":compare",
        ":functional",
        ":logging",
        ":macros",
        ":visibility",
        "//cpp/src/arrow:result",
        "//cpp/src/arrow:status",
    ],
)

cc_library(
    name = "key_value_metadata",
    srcs = ["key_value_metadata.cc"],
    hdrs = ["key_value_metadata.h"],
    deps = [
        ":logging",
        ":macros",
        ":stl",
        ":visibility",
    ],
)

cc_library(
    name = "logging",
    srcs = ["logging.cc"],
    hdrs = ["logging.h"],
    deps = [
        ":macros",
        ":visibility",
    ],
)

cc_library(
    name = "macros",
    hdrs = ["macros.h"],
)

cc_library(
    name = "memory",
    srcs = ["memory.cc"],
    hdrs = ["memory.h"],
    deps = [
        ":macros",
        ":thread_pool",
    ],
)

cc_library(
    name = "parallel",
    hdrs = ["parallel.h"],
)

cc_library(
    name = "parsing",
    hdrs = ["parsing.h"],
    deps = [
        ":config",
        "//cpp/src/arrow:result",
        "//cpp/src/arrow:type_hdr",
        "//cpp/src/arrow:type_traits",
        "//cpp/src/arrow/vendored:datetime",
        "@double-conversion//:double-conversion",
    ],
)

cc_library(
    name = "range",
    hdrs = ["range.h"],
)

cc_library(
    name = "sse_util",
    hdrs = ["sse_util.h"],
    deps = [":macros"],
)

cc_library(
    name = "stl",
    hdrs = ["stl.h"],
    deps = [":logging"],
)

cc_library(
    name = "string",
    srcs = ["string.cc"],
    hdrs = ["string.h"],
    deps = [
        ":string_view",
        ":visibility",
        "//cpp/src/arrow:status",
    ],
)

cc_library(
    name = "string_builder",
    srcs = ["string_builder.cc"],
    hdrs = ["string_builder.h"],
    deps = [
        ":stl",
        ":visibility",
    ],
)

cc_library(
    name = "string_view",
    hdrs = ["string_view.h"],
    deps = ["//cpp/src/arrow/vendored:string_view"],
)

cc_library(
    name = "thread_pool",
    srcs = ["thread_pool.cc"],
    hdrs = ["thread_pool.h"],
    deps = [
        ":io_util",
        ":logging",
        ":macros",
        ":visibility",
        "//cpp/src/arrow:status",
    ],
)

cc_library(
    name = "type_traits",
    hdrs = ["type_traits.h"],
)

cc_library(
    name = "ubsan",
    hdrs = ["ubsan.h"],
)

cc_library(
    name = "uri",
    hdrs = ["uri.h"],
)

cc_library(
    name = "utf8",
    srcs = [
        "utf8.cc",
        "//cpp/src/arrow/vendored:utf8cpp",
    ],
    hdrs = ["utf8.h"],
    deps = [
        ":logging",
        ":string_view",
        "//cpp/src/arrow:status",
    ],
)

cc_library(
    name = "variant",
    hdrs = ["variant.h"],
    deps = ["//cpp/src/arrow/vendored:variant"],
)

cc_library(
    name = "visibility",
    hdrs = ["visibility.h"],
)

cc_library(
    name = "windows_compatibility",
    hdrs = ["windows_compatibility.h"],
)
