# Bazel (http://bazel.io/) BUILD file for gflags.
#
# See INSTALL.md for instructions for adding gflags to a Bazel workspace.

licenses(["notice"])

exports_files(["src/gflags_completions.sh", "COPYING.txt"])

config_setting(
    name = "x64_windows",
    values = {"cpu": "x64_windows"},
)

load(":bazel/gflags.bzl", "gflags_sources", "gflags_library")

(hdrs, srcs) = gflags_sources(namespace=["gflags", "google"])
gflags_library(hdrs=hdrs, srcs=srcs, threads=0)
gflags_library(hdrs=hdrs, srcs=srcs, threads=1)
