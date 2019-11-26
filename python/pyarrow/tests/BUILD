load("@pyarrow_deps_test//:requirements.bzl", "requirement")
load("@rules_python//python:defs.bzl", "py_test")

py_test(name = "test_array",
       srcs = ["test_array.py"],
       deps = ["//python/pyarrow", requirement("hypothesis"), requirement("attrs"), requirement("pytest"),
requirement("pluggy"), requirement("importlib_metadata"), requirement("zipp"), requirement("more_itertools"),
requirement("py"), requirement("packaging"), ":strategies"])

py_library(name = "strategies",
          srcs = ["strategies.py"], deps= [requirement("pytz")])
