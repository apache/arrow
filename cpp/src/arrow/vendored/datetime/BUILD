load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "datetime",
    srcs = ["tz.cpp"],
    hdrs = [
        "date.h",
        "ios.h",
        "tz.h",
        "tz_private.h",
        "visibility.h",
    ],
    visibility = ["//cpp/src/arrow/vendored:__pkg__"],
)

