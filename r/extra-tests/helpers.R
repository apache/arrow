if_version <- function(version, op = `==`) {
  op(numeric_version(Sys.getenv("OLD_ARROW_VERSION", "0.0.0")), version)
}

skip_if_version_less_than <- function(version, msg) {
  if(if_version(version, `<`)) {
    skip(msg)
  }
}

skip_if_version_equals <- function(version, msg) {
  if(if_version(version, `==`)) {
    skip(msg)
  }
}
