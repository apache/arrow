load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

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
    name = "take_internal",
    hdrs = ["take_internal.h"],
    deps = [
        "//arrow:array_builder",
        "//arrow/compute:context",
        "//arrow/compute:kernel",
    ],
)

