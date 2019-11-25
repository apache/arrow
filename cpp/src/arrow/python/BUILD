load("@rules_cc//cc:defs.bzl", "cc_test")
load("//bazel:arrow_cc_library.bzl", "arrow_cc_library")
package(default_visibility = ["//visibility:public"])

arrow_cc_library(
    name = "arrow_to_pandas",
    srcs = ["arrow_to_pandas.cc"],
    hdrs = ["arrow_to_pandas.h"],
    deps = [
        ":common",
        ":datetime",
        ":decimal",
        ":numpy_convert",
        ":numpy_internal",
        ":numpy_interop",
        ":python_to_arrow",
        ":visibility",
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:memory_pool",
        "//cpp/src/arrow/compute:public_api",
        "//cpp/src/arrow/util:hashing",
        "//cpp/src/arrow/util:parallel",
    ],
)

arrow_cc_library(
    name = "common",
    srcs = ["common.cc"],
    hdrs = ["common.h"],
    deps = [
        ":config",
        ":helpers_hdr",
        ":pyarrow",
        "//cpp/src/arrow:buffer",
        "//cpp/src/arrow/io:memory",
    ],
)

arrow_cc_library(
    name = "common_hdr",
    hdrs = ["common.h"],
    deps = [
        ":config",
        ":pyarrow_hdr",
    ],
)

arrow_cc_library(
    name = "config",
    srcs = ["config.cc"],
    hdrs = ["config.h"],
    deps = [
        ":numpy_interop",
        ":platform",
        ":visibility",
    ],
)

arrow_cc_library(
    name = "decimal",
    srcs = ["decimal.cc"],
    hdrs = ["decimal.h"],
    copts = ["-I."],
    deps = [
        ":common_hdr",
        ":helpers_hdr",
        "//cpp/src/arrow:public_api",
    ],
)

arrow_cc_library(
    name = "datetime",
    srcs = ["datetime.cc"],
    hdrs = ["datetime.h"],
    deps = [
        ":common_hdr",
        ":pyarrow_hdr",
    ],
)

arrow_cc_library(
    name = "deserialize",
    srcs = ["deserialize.cc"],
    hdrs = ["deserialize.h"],
    deps = [
        ":numpy_interop",
        ":serialize",
        "//cpp/src/arrow/ipc:reader",
    ],
)

arrow_cc_library(
    name = "extension_type",
    srcs = ["extension_type.cc"],
    hdrs = ["extension_type.h"],
    deps = [
        ":common",
        "//cpp/src/arrow:array",
    ],
)

arrow_cc_library(
    name = "flight",
    srcs = ["flight.cc"],
    hdrs = ["flight.h"],
    deps = [
        ":common",
        "//cpp/src/arrow/flight:internal",
        "//cpp/src/arrow/flight:public_api",
    ],
)

arrow_cc_library(
    name = "helpers",
    srcs = ["helpers.cc"],
    hdrs = ["helpers.h"],
    copts = ["-I."],
    deps = [
        ":common",
        ":decimal",
        ":numpy_interop",
        ":visibility",
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:public_api",
    ],
)

arrow_cc_library(
    name = "helpers_hdr",
    hdrs = ["helpers.h"],
)

arrow_cc_library(
    name = "inference",
    srcs = ["inference.cc"],
    hdrs = ["inference.h"],
    deps = [
        ":common",
        ":datetime",
        ":decimal",
        ":iterators",
        ":numpy_convert",
        ":platform",
        ":visibility",
        "//cpp/src/arrow:array",
    ],
)

arrow_cc_library(
    name = "init",
    srcs = ["init.cc"],
    hdrs = ["init.h"],
    deps = [
        ":numpy_interop",
        ":platform",
        ":visibility",
    ],
)

arrow_cc_library(
    name = "io",
    srcs = ["io.cc"],
    hdrs = ["io.h"],
    deps = [
        ":common",
        ":config",
        ":visibility",
        "//cpp/src/arrow/io:interfaces",
        "//cpp/src/arrow/io:memory",
    ],
)

arrow_cc_library(
    name = "iterators",
    hdrs = ["iterators.h"],
    deps = [":numpy_internal"],
)

arrow_cc_library(
    name = "numpy_convert",
    srcs = ["numpy_convert.cc"],
    hdrs = ["numpy_convert.h"],
    deps = [
        ":common",
        ":numpy_interop",
        ":platform",
        ":type_traits",
        ":visibility",
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow:buffer",
    ],
)

arrow_cc_library(
    name = "numpy_internal",
    hdrs = ["numpy_internal.h"],
    deps = [],
)

arrow_cc_library(
    name = "numpy_interop",
    hdrs = ["numpy_interop.h"],
    deps = [
        ":platform",
        "@numpy//:numpy_headers",
    ],
)

arrow_cc_library(
    name = "numpy_to_arrow",
    srcs = ["numpy_to_arrow.cc"],
    hdrs = ["numpy_to_arrow.h"],
    deps = [
        ":common",
        ":datetime",
        ":iterators",
        ":numpy_convert",
        ":numpy_interop",
        ":platform",
        ":python_to_arrow",
        ":visibility",
        "//cpp/src/arrow/compute/kernels:cast",
    ],
)

arrow_cc_library(
    name = "platform",
    hdrs = ["platform.h"],
    deps = ["@python"],
)

arrow_cc_library(
    name = "pyarrow",
    srcs = ["pyarrow.cc"],
    hdrs = ["pyarrow.h"],
    deps = [
        ":common_hdr",
        ":datetime",
        ":platform",
        ":pyarrow_api",
        ":visibility",
        "//cpp/src/arrow:array",
    ],
)

arrow_cc_library(
    name = "pyarrow_api",
    hdrs = ["pyarrow_api.h"],
    deps = [":pyarrow_lib"],
)

arrow_cc_library(
  name = "benchmark",
  hdrs = ["benchmark.h"],
  srcs = ["benchmark.cc"],
  deps = [":platform", ":visibility", ":helpers"])

arrow_cc_library(
    name = "public_api",
    hdrs = ["api.h"],
    deps = [":pyarrow_lib",
            ":arrow_to_pandas",
":deserialize", ":io", ":numpy_to_arrow", ":init", ":extension_type", ":benchmark", ":helpers"],
)


arrow_cc_library(
    name = "pyarrow_hdr",
    hdrs = ["pyarrow.h"],
    deps = ["//cpp/src/arrow:array"],
)

cc_library(
    name = "pyarrow_lib",
    hdrs = ["pyarrow_lib.h"],
    deps = ["//cpp/src/arrow:array"],
    strip_include_prefix = "/cpp/src/arrow/python/"
)

arrow_cc_library(
    name = "python_to_arrow",
    srcs = ["python_to_arrow.cc"],
    hdrs = ["python_to_arrow.h"],
    deps = [
        ":common",
        ":datetime",
        ":decimal",
        ":inference",
        ":iterators",
        ":platform",
        ":visibility",
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
    ],
)

cc_test(
    name = "python_test",
    srcs = ["python_test.cc"],
    deps = [
        ":arrow_to_pandas",
        ":helpers",
        ":init",
        ":platform",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "//cpp/src/arrow/python/util:test_main",
        "@python",
    ],
)

arrow_cc_library(
    name = "serialize",
    srcs = ["serialize.cc"],
    hdrs = ["serialize.h"],
    deps = [
        ":common",
        ":datetime",
        ":iterators",
        ":numpy_convert",
        ":numpy_interop",
        ":visibility",
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow:status",
        "//cpp/src/arrow/array:builder_union",
        "//cpp/src/arrow/io:memory",
        "//cpp/src/arrow/ipc:options",
        "//cpp/src/arrow/ipc:util",
        "//cpp/src/arrow/ipc:writer",
    ],
)

arrow_cc_library(
    name = "visibility",
    hdrs = ["visibility.h"],
)

arrow_cc_library(
    name = "type_traits",
    hdrs = ["type_traits.h"],
)
