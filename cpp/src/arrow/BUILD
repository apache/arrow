load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "array",
    srcs = [
        "array.cc",
        "compare.cc",
        "extension_type.cc",
        "pretty_print.cc",
        "record_batch.cc",
        "scalar.cc",
        "sparse_tensor.cc",
        "table.cc",
        "tensor.cc",
        "type.cc",
        "visitor.cc",
    ],
    hdrs = [
        "compare.h",
        "extension_type.h",
        "pretty_print.h",
        "record_batch.h",
        "scalar.h",
        "sparse_tensor.h",
        "table.h",
        "tensor.h",
        "type.h",
        "visitor.h",
        "visitor_inline.h",
    ],
    deps = [
        ":buffer",
        ":buffer_builder",
        ":compare",
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
    name = "memory_pool",
    srcs = ["memory_pool.cc"],
    hdrs = ["memory_pool.h"],
    deps = [
        ":status",
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

cc_library(
    name = "result_internal",
    hdrs = ["result_internal.h"],
    deps = [":result"],
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

cc_library(
    name = "type_fwd",
    hdrs = ["type_fwd.h"],
    visibility = ["//visibility:public"],
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

