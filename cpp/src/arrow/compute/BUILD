package(default_visibility = ["//visibility:public"])

cc_library(
    name = "kernel",
    hdrs = ["kernel.h"],
    deps = ["//arrow:array"],
)

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
