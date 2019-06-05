library(fs)
library(magrittr)
library(glue)

copy_headers <- function(dir) {
  origin <- glue("../cpp/src/{dir}")
  dest   <- glue("r/inst/include/{dir}")

  # copy all files
  try(dir_delete(dest), silent = TRUE)
  dir_copy(origin, dest)

  # only keep headers
  dir_ls(dest, glob = "*.h", invert = TRUE, all = TRUE, recurse = TRUE, type = "file") %>%
    file_delete()

  message(glue("{n} header files copied in `{dest}`", n = length(dir_ls(dest, recurse = TRUE))))
  invisible()
}
copy_headers("arrow")
copy_headers("parquet")
