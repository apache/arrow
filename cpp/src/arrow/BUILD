load("@rules_cc//cc:defs.bzl", "cc_library")

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
        "//arrow/array:dict_internal",
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
        "//arrow/array:concatenate",
        "//arrow/util:atomic_shared_ptr",
        "//arrow/util:checked_cast",
        "//arrow/util:decimal",
        "//arrow/util:iterator",
        "//arrow/util:key_value_metadata",
        "//arrow/util:macros",
        "//arrow/util:memory",
        "//arrow/util:range",
        "//arrow/util:visibility",
        "//arrow/vendored:datetime",
    ],
)

cc_test(
    name = "array_binary_test",
    srcs = ["array_binary_test.cc"],
    deps = [
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "array_builder",
    srcs = ["builder.cc"],
    hdrs = ["builder.h"],
    deps = [
        "//arrow/array:builder_adaptive",
        "//arrow/array:builder_binary",
        "//arrow/array:builder_decimal",
        "//arrow/array:builder_dict",
        "//arrow/array:builder_nested",
        "//arrow/array:builder_primitive",
        "//arrow/array:builder_union",
    ],
)

cc_test(
    name = "array_dict_test",
    srcs = ["array_dict_test.cc"],
    deps = [
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
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
        "//arrow/util:checked_cast",
        "//arrow/util:decimal",
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
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "array_struct_test",
    srcs = ["array_struct_test.cc"],
    deps = [
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "array_test",
    srcs = ["array_test.cc"],
    deps = [
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "array_union_test",
    srcs = ["array_union_test.cc"],
    deps = [
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow/ipc:test_common",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "array_view_test",
    srcs = ["array_view_test.cc"],
    deps = [
        "//arrow:array",
        "//arrow:array_builder",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
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
        "//arrow/util:bit_util",
        "//arrow/util:string",
        "//arrow/util:string_view",
    ],
)

cc_library(
    name = "buffer_builder",
    hdrs = ["buffer_builder.h"],
    deps = ["//arrow/util:ubsan"],
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
        "//arrow/array:builder_hdrs",
    ],
)

cc_test(
    name = "buffer_test",
    srcs = ["buffer_test.cc"],
    deps = [
        "//arrow:buffer",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "compare",
    srcs = ["compare.cc"],
    hdrs = ["compare.h"],
    deps = [
        ":array_hdrs",
        "//arrow/array:diff",
        "//arrow/util:macros",
        "//arrow/util:memory",
        "//arrow/util:visibility",
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
        "//arrow:extension_type",
        "//arrow/io:memory",
        "//arrow/ipc:reader",
        "//arrow/ipc:writer",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
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
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
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
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
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
        "//arrow/io:public_api",
        "//arrow/ipc:public_api",
    ],
)

cc_test(
    name = "public_api_test",
    srcs = [
        "public_api_test.cc",
    ],
    deps = [
        ":public_api",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
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
        "//arrow/util:variant",
    ],
)

cc_test(
    name = "result_test",
    srcs = [
        "result_test.cc",
    ],
    deps = [
        ":result",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
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
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
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
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "status",
    srcs = ["status.cc"],
    hdrs = ["status.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//arrow/util:compare",
        "//arrow/util:logging",
        "//arrow/util:string_builder",
    ],
)

cc_test(
    name = "status_test",
    srcs = [
        "status_test.cc",
    ],
    deps = [
        ":status",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "stl",
    hdrs = ["stl.h"],
    deps = ["//arrow/compute:public_api"],
)

cc_test(
    name = "stl_test",
    srcs = [
        "stl_test.cc",
    ],
    deps = [
        ":stl",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
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
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
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
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
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
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "type_fwd",
    hdrs = ["type_fwd.h"],
    visibility = ["//visibility:public"],
    deps = ["//arrow/util:visibility"],
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
        "//arrow/util:checked_cast",
    ],
)

cc_test(
    name = "type_test",
    srcs = [
        "type_test.cc",
    ],
    deps = [
        ":array",
        "//arrow/testing:gtest_common",
        "//arrow/testing:gtest_util",
        "@googletest//:gtest_main",
    ],
)

cc_library(
    name = "type_traits",
    hdrs = ["type_traits.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":status",
        "//arrow/util:bit_util",
        "//arrow/util:visibility",
    ],
)
