package(default_visibility = ["//visibility:public"])

cc_library(
   name = "numpy_headers_local",
	 hdrs = glob(["numpy/numpy/include/core/include/numpy/*.h"]),
   includes = ["numpy/numpy/include/core/include"]
 )

brew_path = "local/lib/python3.7/site-packages/numpy/core/include"
cc_library(
   name = "numpy_headers_brew",
   hdrs = glob([brew_path + "/numpy/*.h"]),
   includes = [brew_path]
)

alias(
  name = "numpy_headers",

 visibility = ["//visibility:public"],
  actual = select({ 
    "@bazel_tools//src/conditions:darwin": "numpy_headers_brew",
    "//conditions:default": "numpy_headers_local"
}))
