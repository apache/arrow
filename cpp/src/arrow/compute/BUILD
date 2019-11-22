load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "context",
    srcs = ["context.cc"],
    hdrs = ["context.h"],
    deps = [
        "//cpp/src/arrow:buffer",
        "//cpp/src/arrow:memory_pool",
        "//cpp/src/arrow/util:cpu_info",
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
        "//cpp/src/arrow:status",
    ],
)

cc_library(
    name = "expression_hdr",
    hdrs = ["expression.h"],
    deps = ["//cpp/src/arrow:status"],
)

cc_library(
    name = "logical_type",
    srcs = ["logical_type.cc"],
    hdrs = ["logical_type.h"],
    deps = [
        ":expression_hdr",
        ":type_fwd",
        "//cpp/src/arrow:array",
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

cc_library(
    name = "kernel",
    hdrs = ["kernel.h"],
    deps = ["//cpp/src/arrow:array"],
)

cc_library(
    name = "type_fwd",
    hdrs = ["type_fwd.h"],
    deps = ["//cpp/src/arrow:type_fwd"],
)
