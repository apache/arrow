package(default_visibility = ["//visibility:public"])
load("@rules_foreign_cc//tools/build_defs:boost_build.bzl", "boost_build")

filegroup(name = "all", srcs = glob(["**"]))

cc_library(
  name = "boost_headers", 
	hdrs = glob(["**/*.hpp", "**/*.h"]),
  includes = ["."]
)

boost_build(
    name = "boost_filesystem",
    lib_source = "@boost//:all",
    static_libraries = ["libboost_filesystem.a"],
    deps = [":boost_system"],
)

boost_build(
    name = "boost_system",
    lib_source = "@boost//:all",
    static_libraries = ["libboost_system.a"],
    deps = [],
)
