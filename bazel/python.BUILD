cc_library(
    name = "python-linux",
    srcs = ["lib/python3.7/config-3.7m-x86_64-linux-gnu/libpython3.7m.so"],
    hdrs = glob(["include/python3.7/*.h"]),
    includes = ["include/python3.7"],
    visibility = ["//visibility:public"]
)

mac_headers = "local/Cellar/python/3.7.4_1/Frameworks/Python.framework/Versions/3.7/include/python3.7m"
cc_library(
    name = "python-mac",
    srcs = ["local/opt/python/Frameworks/Python.framework/Versions/3.7/lib/python3.7/config-3.7m-darwin/libpython3.7m.dylib"],
    hdrs = glob([mac_headers + "/*.h"]),
    includes = [mac_headers],
    visibility = ["//visibility:public"]
)

alias(
  name = "python",

 visibility = ["//visibility:public"],
  actual = select({ 
    "@bazel_tools//src/conditions:darwin": "python-mac",
    "//conditions:default": "python-linux"
}))
