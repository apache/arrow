load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "context",
    srcs = ["context.cc"],
    hdrs = ["context.h"],
    deps = [
        "//arrow:buffer",
        "//arrow:memory_pool",
        "//arrow/util:cpu_info",
    ],
)

cc_library(
    name = "expression",
    srcs = ["expression.cc"],
    hdrs = ["expression.h"],
    deps = [
        ":logical_type",
        ":operation",
        ":type_fwd",
        "//arrow:status",
    ],
)

cc_library(
    name = "expression_hdr",
    hdrs = ["expression.h"],
    deps = ["//arrow:status"],
)

cc_library(
    name = "logical_type",
    srcs = ["logical_type.cc"],
    hdrs = ["logical_type.h"],
    deps = [
        ":expression_hdr",
        ":type_fwd",
        "//arrow:array",
    ],
)

cc_library(
    name = "operation",
    srcs = ["operation.cc"],
    hdrs = ["operation.h"],
    deps = [
        ":type_fwd",
    ],
)

cc_library(
    name = "public_api",
    hdrs = ["api.h"],
    deps = [
        ":context",
        "//arrow/compute/kernels:boolean",
        "//arrow/compute/kernels:cast",
        "//arrow/compute/kernels:compare",
        "//arrow/compute/kernels:count",
        "//arrow/compute/kernels:filter",
        "//arrow/compute/kernels:hash",
        "//arrow/compute/kernels:isin",
        "//arrow/compute/kernels:mean",
        "//arrow/compute/kernels:sort_to_indices",
        "//arrow/compute/kernels:sum",
        "//arrow/compute/kernels:take",
    ],
)

cc_library(
    name = "kernel",
    hdrs = ["kernel.h"],
    deps = ["//arrow:array"],
)

cc_library(
    name = "type_fwd",
    hdrs = ["type_fwd.h"],
    deps = ["//arrow:type_fwd"],
)
