package(default_visibility = ["//visibility:public"])

cc_library(
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
        "//arrow:array",
        "//arrow:memory_pool",
        "//arrow/compute:public_api",
        "//arrow/util:hashing",
        "//arrow/util:parallel",
    ],
)

cc_library(
    name = "common",
    srcs = ["common.cc"],
    hdrs = ["common.h"],
    deps = [
        ":config",
        ":helpers_hdr",
        ":pyarrow",
        "//arrow:buffer",
        "//arrow/io:memory",
    ],
)

cc_library(
    name = "common_hdr",
    hdrs = ["common.h"],
    deps = [
        ":config",
        ":pyarrow_hdr",
    ],
)

cc_library(
    name = "config",
    srcs = ["config.cc"],
    hdrs = ["config.h"],
    deps = [
        ":numpy_interop",
        ":platform",
        ":visibility",
    ],
)

cc_library(
    name = "decimal",
    srcs = ["decimal.cc"],
    hdrs = ["decimal.h"],
    copts = ["-I."],
    deps = [
        ":common_hdr",
        ":helpers_hdr",
        "//arrow:public_api",
    ],
)

cc_library(
    name = "datetime",
    srcs = ["datetime.cc"],
    hdrs = ["datetime.h"],
    deps = [
        ":common_hdr",
        ":pyarrow_hdr",
    ],
)

cc_library(
    name = "deserialize",
    srcs = ["deserialize.cc"],
    hdrs = ["deserialize.h"],
    deps = [
        ":numpy_interop",
        ":serialize",
        "//arrow/ipc:reader",
    ],
)

cc_library(
    name = "extension_type",
    srcs = ["extension_type.cc"],
    hdrs = ["extension_type.h"],
    deps = [
        ":common",
        "//arrow:array",
    ],
)

cc_library(
    name = "flight",
    srcs = ["flight.cc"],
    hdrs = ["flight.h"],
    deps = [
        ":common",
        "//arrow/flight:internal",
        "//arrow/flight:public_api",
    ],
)

cc_library(
    name = "helpers",
    srcs = ["helpers.cc"],
    hdrs = ["helpers.h"],
    copts = ["-I."],
    deps = [
        ":common",
        ":decimal",
        ":numpy_interop",
        ":visibility",
        "//arrow:array",
        "//arrow:public_api",
    ],
)

cc_library(
    name = "helpers_hdr",
    hdrs = ["helpers.h"],
)

cc_library(
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
        "//arrow:array",
    ],
)

cc_library(
    name = "init",
    srcs = ["init.cc"],
    hdrs = ["init.h"],
    deps = [
        ":numpy_interop",
        ":platform",
        ":visibility",
    ],
)

cc_library(
    name = "io",
    srcs = ["io.cc"],
    hdrs = ["io.h"],
    deps = [
        ":common",
        ":config",
        ":visibility",
        "//arrow/io:interfaces",
        "//arrow/io:memory",
    ],
)

cc_library(
    name = "iterators",
    hdrs = ["iterators.h"],
    deps = [":numpy_internal"],
)

cc_library(
    name = "numpy_convert",
    srcs = ["numpy_convert.cc"],
    hdrs = ["numpy_convert.h"],
    deps = [
        ":common",
        ":numpy_interop",
        ":platform",
        ":type_traits",
        ":visibility",
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow:buffer",
    ],
)

cc_library(
    name = "numpy_internal",
    hdrs = ["numpy_internal.h"],
    deps = [],
)

cc_library(
    name = "numpy_interop",
    hdrs = ["numpy_interop.h"],
    deps = [
        ":platform",
        "@numpy//:numpy_headers",
    ],
)

cc_library(
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
        "//arrow/compute/kernels:cast",
    ],
)

cc_library(
    name = "platform",
    hdrs = ["platform.h"],
    deps = ["@python"],
)

cc_library(
    name = "pyarrow",
    srcs = ["pyarrow.cc"],
    hdrs = ["pyarrow.h"],
    deps = [
        ":common_hdr",
        ":datetime",
        ":platform",
        ":pyarrow_api",
        ":visibility",
        "//arrow:array",
    ],
)

cc_library(
    name = "pyarrow_api",
    hdrs = ["pyarrow_api.h"],
    deps = [":pyarrow_lib"],
)

cc_library(
    name = "pyarrow_hdr",
    hdrs = ["pyarrow.h"],
    deps = ["//arrow:array"],
)

cc_library(
    name = "pyarrow_lib",
    hdrs = ["pyarrow_lib.h"],
    deps = ["//arrow:array"],
)

cc_library(
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
        "//arrow:array",
        "//arrow:array_builder",
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
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
        "@googletest//:gtest_main",
        "@python",
    ],
)

cc_library(
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
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow:status",
        "//arrow/array:builder_union",
        "//arrow/io:memory",
        "//arrow/ipc:options",
        "//arrow/ipc:util",
        "//arrow/ipc:writer",
    ],
)

cc_library(
    name = "visibility",
    hdrs = ["visibility.h"],
)

cc_library(
    name = "type_traits",
    hdrs = ["type_traits.h"],
)
