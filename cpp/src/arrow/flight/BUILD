load("@rules_cc//cc:defs.bzl", "cc_test")
load("//bazel:arrow_cc_library.bzl", "arrow_cc_library")
package(default_visibility = ["//visibility:public"])


arrow_cc_library(
    name = "client",
    srcs = ["client.cc"],
    hdrs = ["client.h"],
    deps = [
        ":client_auth",
        ":client_middleware",
        ":types",
        "//cpp/src/arrow/ipc:reader",
        "//cpp/src/arrow/ipc:writer",
        "//cpp/src/arrow/util:uri",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

arrow_cc_library(
    name = "client_auth",
    hdrs = ["client_auth.h"],
)

arrow_cc_library(
    name = "client_middleware",
    hdrs = ["client_middleware.h"],
    deps = ["middleware"],
)

# target where files are originally located
# (As generated).
cc_library(
    name = "proto_srcs_unstripped",
    hdrs = [
        "//format:flight_cc_grpc_proto",
        "//format:flight_cc_proto",
    ],
)

# target that moves the top level includes under
# this directory.
cc_library(
    name = "proto_srcs",
    hdrs = [
        "//format:flight_cc_grpc_proto",
        "//format:flight_cc_proto",
    ],
    strip_include_prefix = "/format",
    include_prefix = "arrow/flight"
)

arrow_cc_library(
    name = "internal",
    srcs = ["internal.cc"],
    hdrs = ["internal.h"],
    deps = [
        ":protocol_internal",
        ":types",
    ],
    copts = ["-Ibazel-bin"]
)

arrow_cc_library(
    name = "internal_hdr",
    hdrs = ["internal.h"],
    deps = [
        ":protocol_internal",
    ],
)

arrow_cc_library(
    name = "middleware",
    hdrs = ["middleware.h"],
    deps = [":middleware_internal"],
)

arrow_cc_library(
    name = "middleware_internal",
    hdrs = ["middleware_internal.h"],
)

arrow_cc_library(
    name = "platform",
    hdrs = ["platform.h"],
    deps = [
        "//cpp/src/arrow/util:config",
        "//cpp/src/arrow/util:windows_compatibility",
    ],
)

arrow_cc_library(
  name = "customize_proto_hdr",
hdrs = [
        "customize_protobuf.h"],
)

arrow_cc_library(
    name = "protocol_internal",
    srcs = [
        "protocol_internal.cc",
    ],
    hdrs = ["protocol_internal.h"],
    copts = ["-Ibazel-bin", "-Icpp/src/"],
    deps = [
        ":platform",
        ":proto_srcs",
":proto_srcs_unstripped",
        ":customize_proto_hdr",
        "@com_github_grpc_grpc//:grpc++_codegen_proto",
    ],
)

arrow_cc_library(
    name = "public_api",
    hdrs = ["api.h"],
    deps = [
        ":client",
        ":server_middleware",
    ],
)

arrow_cc_library(
    name = "server_auth",
    hdrs = ["server_auth.h"],
)

arrow_cc_library(
    name = "server_hdr",
    hdrs = ["server.h"],
    deps = [":server_auth"],
)

arrow_cc_library(
    name = "server_middleware",
    hdrs = ["server_middleware.h"],
    deps = [],
)

arrow_cc_library(
    name = "serialization_internal",
    srcs = ["serialization_internal.cc"],
    hdrs = ["serialization_internal.h"],
    deps = [
        ":internal_hdr",
        ":server_hdr",
        ":types_hdr",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

arrow_cc_library(
    name = "types_hdr",
    hdrs = ["types.h"],
    deps = [
        ":visibility",
        "//cpp/src/arrow/ipc:writer",
    ],
)

arrow_cc_library(
    name = "types",
    srcs = ["types.cc"],
    hdrs = ["types.h"],
    deps = [
        ":serialization_internal",
        ":visibility",
        "//cpp/src/arrow/ipc:reader",
        "//cpp/src/arrow/ipc:writer",
        "//cpp/src/arrow/util:uri",
    ],
)

arrow_cc_library(
    name = "visibility",
    hdrs = ["visibility.h"],
)
