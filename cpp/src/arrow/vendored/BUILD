load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "datetime",
    hdrs = ["datetime.h"],
    visibility = ["//visibility:public"],
    deps = ["//cpp/src/arrow/vendored/datetime"],
)

cc_library(
    name = "string_view",
    hdrs = ["string_view.hpp"],
    visibility = ["//cpp/src/arrow/util:__pkg__"],
)

filegroup(
    name = "utf8cpp",
    srcs = [
        "utf8cpp/checked.h",
        "utf8cpp/core.h",
    ],
    visibility = ["//cpp/src/arrow/util:__pkg__"],
)

cc_library(
    name = "variant",
    hdrs = ["variant.hpp"],
    visibility = ["//cpp/src/arrow/util:__pkg__"],
)

cc_library(
    name = "xxhash",
    hdrs = ["xxhash.h"],
    visibility = ["//visibility:public"],
    deps = ["//cpp/src/arrow/vendored/xxhash"],
)
