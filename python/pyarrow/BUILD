load("@pyarrow_deps_test//:requirements.bzl", "requirement")
load("//bazel:cython_library.bzl", "pyx_library")
load("@rules_python//python:defs.bzl", "py_library")

filegroup(
    name = "mygroup",
    srcs = glob(["*.pxd", "*.pyx", "*.pxi", "includes/*"])
)

pyx_library(name = 'lib',
            srcs = ['lib.pyx', ":mygroup"],
            deps = [],
imports = [".."],
            pxi = [":mygroup"],
            cpp_deps = ["//cpp/src/arrow/python:platform", "@numpy//:numpy_headers", "//cpp/src/arrow:public_api",
                        "//cpp/src/arrow/python:common", "//cpp/src/arrow/filesystem:public_api",
"//cpp/src/arrow/ipc:json_simple", "//cpp/src/arrow/csv:public_api", "//cpp/src/arrow/json:options", "//cpp/src/arrow/json:reader",
 "//cpp/src/arrow/compute:public_api", "//cpp/src/arrow/python:public_api"],
            include_path = "python/pyarrow",
 )

py_library(name = "pyarrow",
           srcs = glob(["*.py"]),
           deps = [":lib", requirement("six")],
           imports = [".."],
           visibility = ["//visibility:public"],
            )

