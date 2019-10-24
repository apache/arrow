load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "aggregate",
    srcs = [
        "aggregate.cc",
    ],
    hdrs = ["aggregate.h"],
    deps = [
        ":util_internal",
        "//arrow:array_builder",
        "//arrow/compute:context",
        "//arrow/compute:kernel",
    ],
)

cc_library(
    name = "boolean",
    srcs = ["boolean.cc"],
    hdrs = ["boolean.h"],
    deps = [
        ":util_internal",
        "//arrow:array_builder",
        "//arrow/compute:context",
        "//arrow/compute:kernel",
    ],
)

cc_library(
    name = "cast",
    srcs = [
        "cast.cc",
        "generated/cast_codegen_internal.h",
    ],
    hdrs = ["cast.h"],
    deps = [
        ":util_internal",
        "//arrow:array_builder",
        "//arrow/compute:context",
        "//arrow/compute:kernel",
        "//arrow/util:formatting",
    ],
)

cc_library(
    name = "compare",
    srcs = [
        "compare.cc",
    ],
    hdrs = ["compare.h"],
    deps = [
        ":util_internal",
        "//arrow:array_builder",
        "//arrow/compute:context",
        "//arrow/compute:kernel",
    ],
)

cc_library(
    name = "count",
    srcs = [
        "count.cc",
    ],
    hdrs = ["count.h"],
    deps = [
        ":aggregate",
        ":util_internal",
        "//arrow:array_builder",
        "//arrow/compute:context",
        "//arrow/compute:kernel",
    ],
)

cc_library(
    name = "filter",
    srcs = ["filter.cc"],
    hdrs = ["filter.h"],
    deps = [
        ":take_internal",
        "//arrow:array_builder",
        "//arrow/compute:kernel",
    ],
)

cc_library(
    name = "hash",
    srcs = ["hash.cc"],
    hdrs = ["hash.h"],
    deps = [
        ":util_internal",
        "//arrow/array:dict_internal",
        "//arrow/compute:context",
        "//arrow/compute:kernel",
    ],
)

cc_library(
    name = "isin",
    srcs = ["isin.cc"],
    hdrs = ["isin.h"],
    deps = [
        ":util_internal",
        "//arrow/array:dict_internal",
        "//arrow/compute:context",
        "//arrow/compute:kernel",
    ],
)

cc_library(
    name = "mean",
    srcs = ["mean.cc"],
    hdrs = ["mean.h"],
    deps = [
        ":sum_internal",
    ],
)

cc_library(
    name = "sort_to_indices",
    srcs = ["sort_to_indices.cc"],
    hdrs = ["sort_to_indices.h"],
    deps = [
        "//arrow:array_builder",
        "//arrow/compute:context",
        "//arrow/compute:expression",
        "//arrow/compute:kernel",
    ],
)

cc_library(
    name = "sum_internal",
    hdrs = ["sum_internal.h"],
    deps = [
        ":aggregate",
    ],
)

cc_library(
    name = "sum",
    srcs = ["sum.cc"],
    hdrs = ["sum.h"],
    deps = [
        ":sum_internal",
    ],
)

cc_library(
    name = "take",
    srcs = ["take.cc"],
    hdrs = ["take.h"],
    deps = [
        ":take_internal",
    ],
)

cc_library(
    name = "take_internal",
    hdrs = ["take_internal.h"],
    deps = [
        "//arrow:array_builder",
        "//arrow/compute:context",
        "//arrow/compute:kernel",
    ],
)

cc_library(
    name = "util_internal",
    srcs = ["util_internal.cc"],
    hdrs = ["util_internal.h"],
    deps = [
        "//arrow:array",
        "//arrow/compute:context",
        "//arrow/compute:kernel",
    ],
)
