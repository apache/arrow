load("//bazel::arrow_cc_library.bzl", arrow_cc_library)

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "array",
    srcs = [
        "array.cc",
        "compare.cc",
        "pretty_print.cc",
        "record_batch.cc",
        "scalar.cc",
        "sparse_tensor.cc",
        "table.cc",
        "tensor.cc",
        "type.cc",
        "visitor.cc",
        "//cpp/src/arrow/array:dict_internal",
    ],
    hdrs = [
        "array.h",
        "record_batch.h",
        "table.h",
    ],
    deps = [
        ":array_hdrs",
        ":buffer",
        ":buffer_builder",
        ":compare",
        ":extension_type",
        ":result",
        ":type_fwd",
        ":type_traits",
        "//cpp/src/arrow/array:concatenate",
        "//cpp/src/arrow/util:atomic_shared_ptr",
        "//cpp/src/arrow/util:checked_cast",
        "//cpp/src/arrow/util:decimal",
        "//cpp/src/arrow/util:iterator",
        "//cpp/src/arrow/util:key_value_metadata",
        "//cpp/src/arrow/util:macros",
        "//cpp/src/arrow/util:memory",
        "//cpp/src/arrow/util:range",
        "//cpp/src/arrow/util:visibility",
        "//cpp/src/arrow/vendored:datetime",
    ],
)

cc_test(
    name = "array_binary_test",
    srcs = ["array_binary_test.cc"],
    deps = [
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "array_builder",
    srcs = ["builder.cc"],
    hdrs = ["builder.h"],
    deps = [
        "//cpp/src/arrow/array:builder_adaptive",
        "//cpp/src/arrow/array:builder_binary",
        "//cpp/src/arrow/array:builder_decimal",
        "//cpp/src/arrow/array:builder_dict",
        "//cpp/src/arrow/array:builder_nested",
        "//cpp/src/arrow/array:builder_primitive",
        "//cpp/src/arrow/array:builder_union",
    ],
)

cc_test(
    name = "array_dict_test",
    srcs = ["array_dict_test.cc"],
    deps = [
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "array_hdrs",
    hdrs = [":array_hdrs_grp"],
    deps = [
        ":buffer",
        ":result",
        ":type_fwd",
        ":type_traits",
        "//cpp/src/arrow/util:checked_cast",
        "//cpp/src/arrow/util:decimal",
    ],
)

filegroup(
    name = "array_hdrs_grp",
    srcs = [
        "array.h",
        "compare.h",
        "extension_type.h",
        "pretty_print.h",
        "scalar.h",
        "sparse_tensor.h",
        "tensor.h",
        "type.h",
        "visitor.h",
        "visitor_inline.h",
    ],
)

cc_test(
    name = "array_list_test",
    srcs = ["array_list_test.cc"],
    deps = [
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "array_struct_test",
    srcs = ["array_struct_test.cc"],
    deps = [
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "array_test",
    srcs = ["array_test.cc"],
    deps = [
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "array_union_test",
    srcs = ["array_union_test.cc"],
    deps = [
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow/ipc:test_common",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "array_view_test",
    srcs = ["array_view_test.cc"],
    deps = [
        "//cpp/src/arrow:array",
        "//cpp/src/arrow:array_builder",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "buffer",
    srcs = ["buffer.cc"],
    visibility = ["//visibility:public"],
    deps = [
        ":buffer_hdr",
        ":memory_pool",
        "//cpp/src/arrow/util:bit_util",
        "//cpp/src/arrow/util:string",
        "//cpp/src/arrow/util:string_view",
    ],
)

cc_library(
    name = "buffer_builder",
    hdrs = ["buffer_builder.h"],
    deps = ["//cpp/src/arrow/util:ubsan"],
)

cc_library(
    name = "buffer_hdr",
    hdrs = ["buffer.h"],
    visibility = ["//visibility:public"],
    deps = [":memory_pool"],
)

cc_library(
    name = "builder_hdr",
    hdrs = [
        "builder.h",
        "//cpp/src/arrow/array:builder_hdrs",
    ],
)

cc_test(
    name = "buffer_test",
    srcs = ["buffer_test.cc"],
    deps = [
        "//cpp/src/arrow:buffer",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "compare",
    srcs = ["compare.cc"],
    hdrs = ["compare.h"],
    deps = [
        ":array_hdrs",
        "//cpp/src/arrow/array:diff",
        "//cpp/src/arrow/util:macros",
        "//cpp/src/arrow/util:memory",
        "//cpp/src/arrow/util:visibility",
    ],
)

cc_library(
    name = "extension_type",
    srcs = [
        "extension_type.cc",
    ],
    hdrs = ["extension_type.h"],
    deps = [
        ":array_hdrs",
        ":buffer",
        ":result",
    ],
)

cc_test(
    name = "extension_type_test",
    srcs = ["extension_type_test.cc"],
    deps = [
        "//cpp/src/arrow:extension_type",
        "//cpp/src/arrow/io:memory",
        "//cpp/src/arrow/ipc:reader",
        "//cpp/src/arrow/ipc:writer",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "memory_pool",
    srcs = ["memory_pool.cc"],
    hdrs = ["memory_pool.h"],
    deps = [
        ":status",
    ],
)

cc_test(
    name = "memory_pool_test",
    srcs = [
        "memory_pool_test.cc",
        "memory_pool_test.h",
    ],
    deps = [
        ":memory_pool",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "pretty_print_test",
    srcs = [
        "pretty_print_test.cc",
    ],
    deps = [
        ":array",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "public_api",
    hdrs = ["api.h"],
    deps = [
        ":array",
        ":array_builder",
        ":table_builder",
        "//cpp/src/arrow/io:public_api",
        "//cpp/src/arrow/ipc:public_api",
    ],
)

cc_test(
    name = "public_api_test",
    srcs = [
        "public_api_test.cc",
    ],
    deps = [
        ":public_api",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "result",
    srcs = ["result.cc"],
    hdrs = ["result.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":status",
        "//cpp/src/arrow/util:variant",
    ],
)

cc_test(
    name = "result_test",
    srcs = [
        "result_test.cc",
    ],
    deps = [
        ":result",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "result_internal",
    hdrs = ["result_internal.h"],
    deps = [":result"],
)

cc_test(
    name = "scalar_test",
    srcs = [
        "scalar_test.cc",
    ],
    deps = [
        ":array",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "sparse_tensor_test",
    srcs = [
        "sparse_tensor_test.cc",
    ],
    deps = [
        ":array",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "status",
    srcs = ["status.cc"],
    hdrs = ["status.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//cpp/src/arrow/util:compare",
        "//cpp/src/arrow/util:logging",
        "//cpp/src/arrow/util:string_builder",
    ],
)

cc_test(
    name = "status_test",
    srcs = [
        "status_test.cc",
    ],
    deps = [
        ":status",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "stl",
    hdrs = ["stl.h"],
    deps = ["//cpp/src/arrow/compute:public_api"],
)

cc_test(
    name = "stl_test",
    srcs = [
        "stl_test.cc",
    ],
    deps = [
        ":stl",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@boost//:boost_headers",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "table_builder",
    srcs = ["table_builder.cc"],
    hdrs = ["table_builder.h"],
    deps = [
        ":array",
        ":array_builder",
    ],
)

cc_test(
    name = "table_builder_test",
    srcs = [
        "table_builder_test.cc",
    ],
    deps = [
        ":table_builder",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "table_test",
    srcs = [
        "table_test.cc",
    ],
    deps = [
        ":array",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "tensor_test",
    srcs = [
        "tensor_test.cc",
    ],
    deps = [
        ":array",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "type_fwd",
    hdrs = ["type_fwd.h"],
    visibility = ["//visibility:public"],
    deps = ["//cpp/src/arrow/util:visibility"],
)

cc_library(
    name = "type_hdr",
    hdrs = [
        "type.h",
        "visitor.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":type_fwd",
        "//cpp/src/arrow/util:checked_cast",
    ],
)

cc_test(
    name = "type_test",
    srcs = [
        "type_test.cc",
    ],
    deps = [
        ":array",
        "//cpp/src/arrow/testing:gtest_common",
        "//cpp/src/arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "type_traits",
    hdrs = ["type_traits.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":status",
        "//cpp/src/arrow/util:bit_util",
        "//cpp/src/arrow/util:visibility",
    ],
)
