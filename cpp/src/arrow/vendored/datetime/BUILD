load("@rules_cc//cc:defs.bzl", "cc_test")
load("//bazel:arrow_cc_library.bzl", "arrow_cc_library")

filegroup(
    name = "hdrs_grp",
    srcs = [
        "date.h",
        "ios.h",
        "tz.h",
        "tz_private.h",
        "visibility.h",

    ],
)

cc_library(
  name = "hdrs",
  hdrs = [":hdrs_grp"],
  strip_include_prefix = "/cpp/src/arrow/vendored/datetime"
)

arrow_cc_library(
    name = "datetime",
    srcs = ["tz.cpp"],
    hdrs = [ ":hdrs_grp",
            ],
    deps = [":hdrs",],
    visibility = ["//cpp/src/arrow/vendored:__pkg__"],
)

