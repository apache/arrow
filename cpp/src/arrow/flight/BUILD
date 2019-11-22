package(default_visibility = ["//visibility:public"])

load("@com_github_grpc_grpc//bazel:generate_cc.bzl", "generate_cc")
load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")

cc_library(
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

cc_library(
    name = "client_auth",
    hdrs = ["client_auth.h"],
)

cc_library(
    name = "client_middleware",
    hdrs = ["client_middleware.h"],
    deps = ["middleware"],
)

cc_library(
    name = "proto_srcs",
    hdrs = [
        ":flight_cc_grpc_proto",
        ":flight_cc_proto",
    ],
)

generate_cc(
    name = "flight_cc_grpc_proto",
    srcs = [":flight_proto"],
    plugin = "@com_github_grpc_grpc//src/compiler:grpc_cpp_plugin",
    well_known_protos = False,
)

generate_cc(
    name = "flight_cc_proto",
    srcs = [":flight_proto"],
    well_known_protos = False,
)

proto_library(
    name = "flight_proto",
    srcs = ["Flight.proto"],
)

cc_library(
    name = "internal",
    srcs = ["internal.cc"],
    hdrs = ["internal.h"],
    deps = [
        ":protocol_internal",
        ":types",
    ],
)

cc_library(
    name = "internal_hdr",
    hdrs = ["internal.h"],
    deps = [
        ":protocol_internal",
    ],
)

cc_library(
    name = "middleware",
    hdrs = ["middleware.h"],
    deps = [":middleware_internal"],
)

cc_library(
    name = "middleware_internal",
    hdrs = ["middleware_internal.h"],
)

cc_library(
    name = "platform",
    hdrs = ["platform.h"],
    deps = [
        "//cpp/src/arrow/util:config",
        "//cpp/src/arrow/util:windows_compatibility",
    ],
)

cc_library(
    name = "protocol_internal",
    srcs = [
        "customize_protobuf.h",
        "protocol_internal.cc",
    ],
    hdrs = ["protocol_internal.h"],
    copts = ["-Ibazel-bin"],
    deps = [
        ":platform",
        ":proto_srcs",
        "@com_github_grpc_grpc//:grpc++_codegen_proto",
    ],
)

cc_library(
    name = "public_api",
    hdrs = ["api.h"],
    deps = [
        ":client",
        ":server_middleware",
    ],
)

cc_library(
    name = "server_auth",
    hdrs = ["server_auth.h"],
)

cc_library(
    name = "server_hdr",
    hdrs = ["server.h"],
    deps = [":server_auth"],
)

cc_library(
    name = "server_middleware",
    hdrs = ["server_middleware.h"],
    deps = [],
)

cc_library(
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

cc_library(
    name = "types_hdr",
    hdrs = ["types.h"],
    deps = [
        ":visibility",
        "//cpp/src/arrow/ipc:writer",
    ],
)

cc_library(
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

cc_library(
    name = "visibility",
    hdrs = ["visibility.h"],
)
