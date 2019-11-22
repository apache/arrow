def arrow_cc_library(name, **kwargs):
  native.cc_library(name = name,
     strip_include_prefix = "/cpp/src",
     **kwargs)
