load("@rules_cc//cc:defs.bzl", "cc_test")
load("//bazel:arrow_cc_library.bzl", "arrow_cc_library")

package(default_visibility = ["//visibility:public"])

arrow_cc_library(
    name = "context",
    srcs = ["context.cc"],
    hdrs = ["context.h"],
    deps = [
        "//cpp/src/arrow:buffer",
        "//cpp/src/arrow:memory_pool",
        "//cpp/src/arrow/util:cpu_info",
    ],
)

arrow_cc_library(
    name = "expression",
    srcs = ["expression.cc"],
    hdrs = ["expression.h"],
    deps = [
        ":logical_type",
        ":operation",
        ":type_fwd",
        "//cpp/src/arrow:status",
    ],
)

arrow_cc_library(
    name = "expression_hdr",
    hdrs = ["expression.h"],
    deps = ["//cpp/src/arrow:status"],
)

arrow_cc_library(
    name = "logical_type",
    srcs = ["logical_type.cc"],
    hdrs = ["logical_type.h"],
    deps = [
        ":expression_hdr",
        ":type_fwd",
        "//cpp/src/arrow:array",
    ],
)

arrow_cc_library(
    name = "operation",
    srcs = ["operation.cc"],
    hdrs = ["operation.h"],
    deps = [
        ":type_fwd",
    ],
)

arrow_cc_library(
    name = "public_api",
    hdrs = ["api.h"],
    deps = [
        ":context",
        "//cpp/src/arrow/compute/kernels:boolean",
        "//cpp/src/arrow/compute/kernels:cast",
        "//cpp/src/arrow/compute/kernels:compare",
        "//cpp/src/arrow/compute/kernels:count",
        "//cpp/src/arrow/compute/kernels:filter",
        "//cpp/src/arrow/compute/kernels:hash",
        "//cpp/src/arrow/compute/kernels:isin",
        "//cpp/src/arrow/compute/kernels:mean",
        "//cpp/src/arrow/compute/kernels:sort_to_indices",
        "//cpp/src/arrow/compute/kernels:sum",
        "//cpp/src/arrow/compute/kernels:take",
    ],
)

arrow_cc_library(
    name = "kernel",
    hdrs = ["kernel.h"],
    deps = ["//cpp/src/arrow:array"],
)

arrow_cc_library(
    name = "type_fwd",
    hdrs = ["type_fwd.h"],
    deps = ["//cpp/src/arrow:type_fwd"],
)
