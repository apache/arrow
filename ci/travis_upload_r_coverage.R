library(covr)
# Hack file paths to include the subdirectory
trace("to_codecov", quote(per_line <- function(x) {
   out <- covr:::per_line(x)
   setNames(out, paste0("r/", names(out)))
 }), where = package_coverage)
codecov()
