package(default_visibility = ["//cpp/src/arrow/flight:__pkg__"])

load("@com_github_grpc_grpc//bazel:generate_cc.bzl", "generate_cc")
load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")

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

