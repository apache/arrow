# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

args <- commandArgs(TRUE)
VERSION <- args[1]
dst_dir <- paste0("libarrow/arrow-", VERSION)

arrow_repo <- paste0(getOption("arrow.dev_repo", "https://arrow-r-nightly.s3.amazonaws.com"), "/libarrow/")

options(.arrow.cleanup = character()) # To collect dirs to rm on exit
on.exit(unlink(getOption(".arrow.cleanup")))

env_is <- function(var, value) identical(tolower(Sys.getenv(var)), value)

try_download <- function(from_url, to_file) {
  status <- try(
    suppressWarnings(
      download.file(from_url, to_file, quiet = quietly)
    ),
    silent = quietly
  )
  # Return whether the download was successful
  !inherits(status, "try-error") && status == 0
}

# For local debugging, set ARROW_R_DEV=TRUE to make this script print more
quietly <- !env_is("ARROW_R_DEV", "true")

# Default is build from source, not download a binary
build_ok <- !env_is("LIBARROW_BUILD", "false")
# For LIBARROW_BINARY we support "true" or the name of the OS to use to
# locate the appropriate binary (e.g., 'ubuntu-18.04'). When NOT_CRAN=true, and
# LIBARROW_BINARY is unset, configure script sets LIBARROW_BINARY=true.
binary_ok <- !env_is("LIBARROW_BINARY", "false") || env_is("LIBARROW_BINARY", "")

# Check if we're doing an offline build.
# (Note that cmake will still be downloaded if necessary
#  https://arrow.apache.org/docs/developers/cpp/building.html#offline-builds)
download_ok <- !env_is("TEST_OFFLINE_BUILD", "true") && try_download("https://github.com", tempfile())

# This "tools/thirdparty_dependencies" path, within the tar file, might exist if
# create_package_with_all_dependencies() was run, or if someone has created it
# manually before running make build.
# If you change this path, you also need to edit
# `create_package_with_all_dependencies()` in install-arrow.R
thirdparty_dependency_dir <- Sys.getenv("ARROW_THIRDPARTY_DEPENDENCY_DIR", "tools/thirdparty_dependencies")


download_binary <- function(os = identify_os()) {
  libfile <- tempfile()
  if (!is.null(os)) {
    # See if we can map this os-version to one we have binaries for
    os <- find_available_binary(os)
    binary_url <- paste0(arrow_repo, "bin/", os, "/arrow-", VERSION, ".zip")
    if (try_download(binary_url, libfile)) {
      cat(sprintf("*** Successfully retrieved C++ binaries for %s\n", os))
      if (!identical(os, "centos-7")) {
        # centos-7 uses gcc 4.8 so the binary doesn't have ARROW_S3=ON but the others do
        # TODO: actually check for system requirements?
        cat("**** Binary package requires libcurl and openssl\n")
        cat("**** If installation fails, retry after installing those system requirements\n")
      }
    } else {
      cat(sprintf("*** No libarrow binary found for version %s on %s\n", VERSION, os))
      libfile <- NULL
    }
  } else {
    libfile <- NULL
  }
  libfile
}

# Function to figure out which flavor of binary we should download, if at all.
# By default (unset or "FALSE"), it will not download a precompiled library,
# but you can override this by setting the env var LIBARROW_BINARY to:
# * `TRUE` (not case-sensitive), to try to discover your current OS, or
# * some other string, presumably a related "distro-version" that has binaries
#   built that work for your OS
identify_os <- function(os = Sys.getenv("LIBARROW_BINARY")) {
  if (tolower(os) %in% c("", "false")) {
    # Env var says not to download a binary
    return(NULL)
  } else if (!identical(tolower(os), "true")) {
    # Env var provided an os-version to use--maybe you're on Ubuntu 18.10 but
    # we only build for 18.04 and that's fine--so use what the user set
    return(os)
  }

  linux <- distro()
  if (is.null(linux)) {
    cat("*** Unable to identify current OS/version\n")
    return(NULL)
  }
  paste(linux$id, linux$short_version, sep = "-")
}

#### start distro ####

distro <- function() {
  # The code in this script is a (potentially stale) copy of the distro package
  if (requireNamespace("distro", quietly = TRUE)) {
    # Use the version from the package, which may be updated from this
    return(distro::distro())
  }

  out <- lsb_release()
  if (is.null(out)) {
    out <- os_release()
    if (is.null(out)) {
      out <- system_release()
    }
  }
  if (is.null(out)) {
    return(NULL)
  }

  out$id <- tolower(out$id)
  # debian unstable & testing lsb_release `version` don't include numbers but we can map from pretty name
  if (is.null(out$version) || out$version %in% c("testing", "unstable")) {
    if (grepl("bullseye", out$codename)) {
      out$short_version <- "11"
    } else if (grepl("bookworm", out$codename)) {
      out$short_version <- "12"
    }
  } else if (out$id == "ubuntu") {
    # Keep major.minor version
    out$short_version <- sub('^"?([0-9]+\\.[0-9]+).*"?.*$', "\\1", out$version)
  } else {
    # Only major version number
    out$short_version <- sub('^"?([0-9]+).*"?.*$', "\\1", out$version)
  }
  out
}

lsb_release <- function() {
  if (have_lsb_release()) {
    list(
      id = call_lsb("-is"),
      version = call_lsb("-rs"),
      codename = call_lsb("-cs")
    )
  } else {
    NULL
  }
}

have_lsb_release <- function() nzchar(Sys.which("lsb_release"))
call_lsb <- function(args) system(paste("lsb_release", args), intern = TRUE)

os_release <- function() {
  rel_data <- read_os_release()
  if (!is.null(rel_data)) {
    vals <- as.list(sub('^.*="?(.*?)"?$', "\\1", rel_data))
    names(vals) <- sub("^(.*)=.*$", "\\1", rel_data)

    out <- list(
      id = vals[["ID"]],
      version = vals[["VERSION_ID"]]
    )
    if ("VERSION_CODENAME" %in% names(vals)) {
      out$codename <- vals[["VERSION_CODENAME"]]
    } else {
      # This probably isn't right, maybe could extract codename from pretty name?
      out$codename <- vals[["PRETTY_NAME"]]
    }
    out
  } else {
    NULL
  }
}

read_os_release <- function() {
  if (file.exists("/etc/os-release")) {
    readLines("/etc/os-release")
  }
}

system_release <- function() {
  rel_data <- read_system_release()
  if (!is.null(rel_data)) {
    # Something like "CentOS Linux release 7.7.1908 (Core)"
    list(
      id = sub("^([a-zA-Z]+) .* ([0-9.]+).*$", "\\1", rel_data),
      version = sub("^([a-zA-Z]+) .* ([0-9.]+).*$", "\\2", rel_data),
      codename = NA
    )
  } else {
    NULL
  }
}

read_system_release <- function() {
  if (file.exists("/etc/system-release")) {
    readLines("/etc/system-release")[1]
  }
}

#### end distro ####

find_available_binary <- function(os) {
  # Download a csv that maps one to the other, columns "actual" and "use_this"
  u <- "https://raw.githubusercontent.com/ursa-labs/arrow-r-nightly/master/linux/distro-map.csv"
  lookup <- try(utils::read.csv(u, stringsAsFactors = FALSE), silent = quietly)
  if (!inherits(lookup, "try-error") && os %in% lookup$actual) {
    new <- lookup$use_this[lookup$actual == os]
    if (length(new) == 1 && !is.na(new)) { # Just some sanity checking
      cat(sprintf("*** Using %s binary for %s\n", new, os))
      os <- new
    }
  }
  os
}

find_local_source <- function() {
  # We'll take the first of these that exists
  # The first case probably occurs if we're in the arrow git repo
  # The second probably occurs if we're installing the arrow R package
  cpp_dir_options <- c(
    file.path(Sys.getenv("ARROW_SOURCE_HOME", ".."), "cpp"),
    "tools/cpp"
  )
  for (cpp_dir in cpp_dir_options) {
    if (file.exists(file.path(cpp_dir, "src/arrow/api.h"))) {
      cat(paste0("*** Found local C++ source: '", cpp_dir, "'\n"))
      return(cpp_dir)
    }
  }
  NULL
}

env_vars_as_string <- function(env_var_list) {
  # Do some basic checks on env_var_list:
  # Check that env_var_list has names, that those names are valid POSIX
  # environment variables, and that none of the values contain `'`.
  stopifnot(
    length(env_var_list) == length(names(env_var_list)),
    all(grepl("^[^0-9]", names(env_var_list))),
    all(grepl("^[A-Z0-9_]+$", names(env_var_list))),
    !any(grepl("'", env_var_list, fixed = TRUE))
  )
  env_var_string <- paste0(names(env_var_list), "='", env_var_list, "'", collapse = " ")
  if (nchar(env_var_string) > 30000) {
    # This could happen if the full paths in *_SOURCE_URL were *very* long.
    # A more formal check would look at getconf ARG_MAX, but this shouldn't matter
    cat("*** Warning: Environment variables are very long. This could cause issues on some shells.\n")
  }
  env_var_string
}

build_libarrow <- function(src_dir, dst_dir) {
  # We'll need to compile R bindings with these libs, so delete any .o files
  system("rm src/*.o", ignore.stdout = TRUE, ignore.stderr = TRUE)
  # Set up make for parallel building
  makeflags <- Sys.getenv("MAKEFLAGS")
  if (makeflags == "") {
    # CRAN policy says not to use more than 2 cores during checks
    # If you have more and want to use more, set MAKEFLAGS
    ncores <- min(parallel::detectCores(), 2)
    makeflags <- sprintf("-j%s", ncores)
    Sys.setenv(MAKEFLAGS = makeflags)
  }
  if (!quietly) {
    cat("*** Building with MAKEFLAGS=", makeflags, "\n")
  }
  # Check for libarrow build dependencies:
  # * cmake
  cmake <- ensure_cmake()

  # Optionally build somewhere not in tmp so we can dissect the build if it fails
  debug_dir <- Sys.getenv("LIBARROW_DEBUG_DIR")
  if (nzchar(debug_dir)) {
    build_dir <- debug_dir
  } else {
    # But normally we'll just build in a tmp dir
    build_dir <- tempfile()
  }
  options(.arrow.cleanup = c(getOption(".arrow.cleanup"), build_dir))

  R_CMD_config <- function(var) {
    tools::Rcmd(paste("config", var), stdout = TRUE)
  }
  env_var_list <- c(
    SOURCE_DIR = src_dir,
    BUILD_DIR = build_dir,
    DEST_DIR = dst_dir,
    CMAKE = cmake,
    # EXTRA_CMAKE_FLAGS will often be "", but it's convenient later to have it defined
    EXTRA_CMAKE_FLAGS = Sys.getenv("EXTRA_CMAKE_FLAGS"),
    # Make sure we build with the same compiler settings that R is using
    # Exception: if you've added ccache to CC and CXX following
    # http://dirk.eddelbuettel.com/blog/2017/11/27/, some libarrow
    # third party dependencies will error on compilation. But don't
    # worry, `ARROW_USE_CCACHE=ON` by default, so if ccache
    # is found, it will be used by the libarrow build, and this does
    # not affect how R compiles the arrow bindings.
    CC = sub("^.*ccache", "", R_CMD_config("CC")),
    CXX = paste(sub("^.*ccache", "", R_CMD_config("CXX11")), R_CMD_config("CXX11STD")),
    # CXXFLAGS = R_CMD_config("CXX11FLAGS"), # We don't want the same debug symbols
    LDFLAGS = R_CMD_config("LDFLAGS")
  )
  env_var_list <- with_s3_support(env_var_list)
  env_var_list <- with_mimalloc(env_var_list)

  # turn_off_all_optional_features() needs to happen after with_mimalloc() and
  # with_s3_support(), since those might turn features ON.
  thirdparty_deps_unavailable <- !download_ok &&
    !dir.exists(thirdparty_dependency_dir) &&
    !env_is("ARROW_DEPENDENCY_SOURCE", "system")
  do_minimal_build <- env_is("LIBARROW_MINIMAL", "true")

  if (do_minimal_build) {
    env_var_list <- turn_off_all_optional_features(env_var_list)
  } else if (thirdparty_deps_unavailable) {
    cat(paste0(
      "*** Building C++ library from source, but downloading thirdparty dependencies\n",
      "    is not possible, so this build will turn off all thirdparty features.\n",
      "    See install vignette for details:\n",
      "    https://cran.r-project.org/web/packages/arrow/vignettes/install.html\n"
    ))
    env_var_list <- turn_off_all_optional_features(env_var_list)
  } else if (dir.exists(thirdparty_dependency_dir)) {
    # Add the *_SOURCE_URL env vars
    env_var_list <- set_thirdparty_urls(env_var_list)
  }
  env_vars <- env_vars_as_string(env_var_list)

  cat("**** arrow", ifelse(quietly, "", paste("with", env_vars)), "\n")
  status <- suppressWarnings(system(
    paste(env_vars, "inst/build_arrow_static.sh"),
    ignore.stdout = quietly, ignore.stderr = quietly
  ))
  if (status != 0) {
    # It failed :(
    cat(
      "**** Error building Arrow C++.",
      ifelse(env_is("ARROW_R_DEV", "true"), "", "Re-run with ARROW_R_DEV=true for debug information."),
      "\n"
    )
  }
  invisible(status)
}

ensure_cmake <- function() {
  cmake <- find_cmake(c(
    Sys.getenv("CMAKE"),
    Sys.which("cmake"),
    Sys.which("cmake3")
  ))

  if (is.null(cmake)) {
    # If not found, download it
    cat("**** cmake\n")
    CMAKE_VERSION <- Sys.getenv("CMAKE_VERSION", "3.21.4")
    if (tolower(Sys.info()[["sysname"]]) %in% "darwin") {
      postfix <- "-macos-universal.tar.gz"
    } else if (tolower(Sys.info()[["machine"]]) == "arm64") {
      postfix <- "-linux-aarch64.tar.gz"
    } else if (tolower(Sys.info()[["machine"]]) == "x86_64") {
      postfix <- "-linux-x86_64.tar.gz"
    } else {
      stop(paste0(
        "*** cmake was not found locally.\n",
        "    Please make sure cmake >= 3.10 is installed and available on your PATH.\n"
      ))
    }
    cmake_binary_url <- paste0(
      "https://github.com/Kitware/CMake/releases/download/v", CMAKE_VERSION,
      "/cmake-", CMAKE_VERSION, postfix
    )
    cmake_tar <- tempfile()
    cmake_dir <- tempfile()
    download_successful <- try_download(cmake_binary_url, cmake_tar)
    if (!download_successful) {
      cat(paste0(
        "*** cmake was not found locally and download failed.\n",
        "    Make sure cmake >= 3.10 is installed and available on your PATH,\n",
        "    or download ", cmake_binary_url, "\n",
        "    and define the CMAKE environment variable.\n"
      ))
    }
    untar(cmake_tar, exdir = cmake_dir)
    unlink(cmake_tar)
    options(.arrow.cleanup = c(getOption(".arrow.cleanup"), cmake_dir))
    cmake <- paste0(
      cmake_dir,
      "/cmake-", CMAKE_VERSION, sub(".tar.gz", "", postfix, fixed = TRUE),
      "/bin/cmake"
    )
  }
  cmake
}

find_cmake <- function(paths, version_required = "3.10") {
  # Given a list of possible cmake paths, return the first one that exists and is new enough
  # version_required should be a string or packageVersion; numeric version
  # can be misleading (e.g. 3.10 is actually 3.1)
  for (path in paths) {
    if (nzchar(path) && cmake_version(path) >= version_required) {
      # Sys.which() returns a named vector, but that plays badly with c() later
      names(path) <- NULL
      return(path)
    }
  }
  # If none found, return NULL
  NULL
}

cmake_version <- function(cmd = "cmake") {
  tryCatch(
    {
      raw_version <- system(paste(cmd, "--version"), intern = TRUE, ignore.stderr = TRUE)
      pat <- ".* ([0-9\\.]+).*?"
      which_line <- grep(pat, raw_version)
      package_version(sub(pat, "\\1", raw_version[which_line]))
    },
    error = function(e) {
      return(0)
    }
  )
}

turn_off_all_optional_features <- function(env_var_list) {
  # Because these are done as environment variables (as opposed to build flags),
  # setting these to "OFF" overrides any previous setting. We don't need to
  # check the existing value.
  # Some features turn on other features (e.g. substrait -> protobuf),
  # So the list of things to turn off is long. See:
  # https://github.com/apache/arrow/blob/master/cpp/cmake_modules/ThirdpartyToolchain.cmake#L275
  turn_off <- c(
    "ARROW_MIMALLOC" = "OFF",
    "ARROW_JEMALLOC" = "OFF",
    "ARROW_JSON" = "OFF",
    "ARROW_PARQUET" = "OFF", # depends on thrift
    "ARROW_DATASET" = "OFF", # depends on parquet
    "ARROW_S3" = "OFF",
    "ARROW_GCS" = "OFF",
    "ARROW_WITH_GOOGLE_CLOUD_CPP" = "OFF",
    "ARROW_WITH_NLOHMANN_JSON" = "OFF",
    "ARROW_SUBSTRAIT" = "OFF",
    "ARROW_WITH_PROTOBUF" = "OFF",
    "ARROW_WITH_BROTLI" = "OFF",
    "ARROW_WITH_BZ2" = "OFF",
    "ARROW_WITH_LZ4" = "OFF",
    "ARROW_WITH_SNAPPY" = "OFF",
    "ARROW_WITH_ZLIB" = "OFF",
    "ARROW_WITH_ZSTD" = "OFF",
    "ARROW_WITH_RE2" = "OFF",
    "ARROW_WITH_UTF8PROC" = "OFF",
    # The syntax to turn off XSIMD is different.
    # Pull existing value of EXTRA_CMAKE_FLAGS first (must be defined)
    "EXTRA_CMAKE_FLAGS" = paste(
      env_var_list[["EXTRA_CMAKE_FLAGS"]],
      "-DARROW_SIMD_LEVEL=NONE -DARROW_RUNTIME_SIMD_LEVEL=NONE"
    )
  )
  # Create a new env_var_list, with the values of turn_off set.
  # replace() also adds new values if they didn't exist before
  replace(env_var_list, names(turn_off), turn_off)
}

get_component_names <- function() {
  if (!isTRUE(Sys.which("bash") != "")) {
    stop("nixlibs.R requires bash to be installed and available in your PATH")
  }
  deps_bash <- "tools/download_dependencies_R.sh"
  csv_tempfile <- tempfile(fileext = ".csv")
  deps_bash_success <- system2("bash", deps_bash, stdout = csv_tempfile) == 0
  if (!deps_bash_success) {
    stop("Failed to run download_dependencies_R.sh")
  }
  deps_df <- read.csv(csv_tempfile,
    stringsAsFactors = FALSE, row.names = NULL, quote = "'"
  )
  stopifnot(
    names(deps_df) == c("env_varname", "filename"),
    nrow(deps_df) > 0
  )
  deps_df
}

set_thirdparty_urls <- function(env_var_list) {
  # This function does *not* check if existing *_SOURCE_URL variables are set.
  # The directory tools/thirdparty_dependencies is created by
  # create_package_with_all_dependencies() and saved in the tar file.
  deps_df <- get_component_names()
  dep_dir <- normalizePath(thirdparty_dependency_dir, mustWork = TRUE)
  deps_df$full_filenames <- file.path(dep_dir, deps_df$filename)
  files_exist <- file.exists(deps_df$full_filenames)
  if (!any(files_exist)) {
    stop("Dependency tar files did not exist in '", dep_dir, "'")
  }
  # Only set env var for files that are in thirdparty_dependency_dir
  # (allows for a user to download a limited set of tar files, if they wanted)
  deps_df <- deps_df[files_exist, ]
  env_var_list <- replace(env_var_list, deps_df$env_varname, deps_df$full_filenames)
  if (!quietly) {
    env_var_list <- replace(env_var_list, "ARROW_VERBOSE_THIRDPARTY_BUILD", "ON")
  }
  env_var_list
}

is_feature_requested <- function(env_varname, default = env_is("LIBARROW_MINIMAL", "false")) {
  env_value <- tolower(Sys.getenv(env_varname))
  if (identical(env_value, "off")) {
    # If e.g. ARROW_MIMALLOC=OFF explicitly, override default
    requested <- FALSE
  } else if (identical(env_value, "on")) {
    requested <- TRUE
  } else {
    requested <- default
  }
  requested
}

with_mimalloc <- function(env_var_list) {
  arrow_mimalloc <- is_feature_requested("ARROW_MIMALLOC")
  if (arrow_mimalloc) {
    # User wants mimalloc. If they're using gcc, let's make sure the version is >= 4.9
    if (isTRUE(cmake_gcc_version(env_var_list) < "4.9")) {
      cat("**** mimalloc support not available for gcc < 4.9; building with ARROW_MIMALLOC=OFF\n")
      arrow_mimalloc <- FALSE
    }
  }
  replace(env_var_list, "ARROW_MIMALLOC", ifelse(arrow_mimalloc, "ON", "OFF"))
}

with_s3_support <- function(env_var_list) {
  arrow_s3 <- is_feature_requested("ARROW_S3")
  if (arrow_s3) {
    # User wants S3 support. If they're using gcc, let's make sure the version is >= 4.9
    # and make sure that we have curl and openssl system libs
    if (isTRUE(cmake_gcc_version(env_var_list) < "4.9")) {
      cat("**** S3 support not available for gcc < 4.9; building with ARROW_S3=OFF\n")
      arrow_s3 <- FALSE
    } else if (!cmake_find_package("CURL", NULL, env_var_list)) {
      # curl on macos should be installed, so no need to alter this for macos
      cat("**** S3 support requires libcurl-devel (rpm) or libcurl4-openssl-dev (deb); building with ARROW_S3=OFF\n")
      arrow_s3 <- FALSE
    } else if (!cmake_find_package("OpenSSL", "1.0.2", env_var_list)) {
      cat("**** S3 support requires version >= 1.0.2 of openssl-devel (rpm), libssl-dev (deb), or openssl (brew); building with ARROW_S3=OFF\n")
      arrow_s3 <- FALSE
    }
  }
  replace(env_var_list, "ARROW_S3", ifelse(arrow_s3, "ON", "OFF"))
}

cmake_gcc_version <- function(env_var_list) {
  # This function returns NA if using a non-gcc compiler
  # Always enclose calls to it in isTRUE() or isFALSE()
  vals <- cmake_cxx_compiler_vars(env_var_list)
  if (!identical(vals[["CMAKE_CXX_COMPILER_ID"]], "GNU")) {
    return(NA)
  }
  package_version(vals[["CMAKE_CXX_COMPILER_VERSION"]])
}

cmake_cxx_compiler_vars <- function(env_var_list) {
  env_vars <- env_vars_as_string(env_var_list)
  info <- system(paste("export", env_vars, "&& $CMAKE --system-information"), intern = TRUE)
  info <- grep("^[A-Z_]* .*$", info, value = TRUE)
  vals <- as.list(sub('^.*? "?(.*?)"?$', "\\1", info))
  names(vals) <- sub("^(.*?) .*$", "\\1", info)
  vals[grepl("^CMAKE_CXX_COMPILER_?", names(vals))]
}

cmake_find_package <- function(pkg, version = NULL, env_var_list) {
  td <- tempfile()
  dir.create(td)
  options(.arrow.cleanup = c(getOption(".arrow.cleanup"), td))
  find_package <- paste0("find_package(", pkg, " ", version, " REQUIRED)")
  writeLines(find_package, file.path(td, "CMakeLists.txt"))
  env_vars <- env_vars_as_string(env_var_list)
  cmake_cmd <- paste0(
    "export ", env_vars,
    " && cd ", td,
    " && $CMAKE ",
    " -DCMAKE_EXPORT_NO_PACKAGE_REGISTRY=ON",
    " -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    " ."
  )
  system(cmake_cmd, ignore.stdout = TRUE, ignore.stderr = TRUE) == 0
}

#####

if (!file.exists(paste0(dst_dir, "/include/arrow/api.h"))) {
  # If we're working in a local checkout and have already built the libs, we
  # don't need to do anything. Otherwise,
  # (1) Look for a prebuilt binary for this version
  bin_file <- src_dir <- NULL
  if (download_ok && binary_ok) {
    bin_file <- download_binary()
  }
  if (!is.null(bin_file)) {
    # Extract them
    dir.create(dst_dir, showWarnings = !quietly, recursive = TRUE)
    unzip(bin_file, exdir = dst_dir)
    unlink(bin_file)
  } else if (build_ok) {
    # (2) Find source and build it
    src_dir <- find_local_source()
    if (!is.null(src_dir)) {
      cat(paste0(
        "*** Building libarrow from source\n",
        "    For a faster, more complete installation, set the environment variable NOT_CRAN=true before installing\n",
        "    See install vignette for details:\n",
        "    https://cran.r-project.org/web/packages/arrow/vignettes/install.html\n"
      ))
      build_libarrow(src_dir, dst_dir)
    } else {
      cat("*** Proceeding without libarrow\n")
    }
  } else {
    cat("*** Proceeding without libarrow\n")
  }
}
