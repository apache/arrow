src_dir <- file.path(R_PACKAGE_SOURCE, "src", fsep = "/")
dest_dir <- file.path(R_PACKAGE_DIR, paste0("libs", R_ARCH), fsep="/")

dir.create(file.path(R_PACKAGE_DIR, paste0("libs", R_ARCH), fsep="/"), recursive = TRUE, showWarnings = FALSE)
file.copy(file.path(src_dir, "arrow.dll", fsep = "/"), file.path(dest_dir, "lib_arrow.dll", fsep = "/"))
