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

# This file is used to generate code in the files
# src/arrowExports.cpp and R/arrowExports.R
#
# This is similar to what compileAttributes() would do,
# with some arrow specific changes.
#
# Core functions are decorated with [[arrow::export]].
# Different decorators can be used to export features that are conditionally
# available.
# Use [[feature::export]] to decorate the functions, and be sure to wrap them in
# #if defined(ARROW_R_WITH_FEATURE)

cat("DEBUG: Starting codegen.R - VERY FIRST LINE\n", file = stderr())
flush(stderr())

# Ensure that all machines are sorting the same way
cat("DEBUG: About to call Sys.setlocale...\n", file = stderr())
flush(stderr())
invisible(Sys.setlocale("LC_COLLATE", "C"))

cat("DEBUG: After setlocale\n", file = stderr())
flush(stderr())

cat("DEBUG: R version: ", R.version.string, "\n", file = stderr())
cat("DEBUG: Platform: ", .Platform$OS.type, "\n", file = stderr())
flush(stderr())

features <- c("acero", "dataset", "substrait", "parquet", "s3", "gcs", "json")

cat("DEBUG: About to load decor...\n", file = stderr())
flush(stderr())
suppressPackageStartupMessages(library(decor))

cat("DEBUG: Loaded decor, about to load dplyr...\n", file = stderr())
flush(stderr())
suppressPackageStartupMessages(library(dplyr))

cat("DEBUG: Loaded dplyr, about to load purrr...\n", file = stderr())
flush(stderr())
suppressPackageStartupMessages(library(purrr))

cat("DEBUG: Loaded purrr, about to load glue...\n", file = stderr())
flush(stderr())
suppressPackageStartupMessages(library(glue))

cat("DEBUG: Loaded glue, about to load vctrs...\n", file = stderr())
flush(stderr())
suppressPackageStartupMessages(library(vctrs))

cat("DEBUG: All packages loaded successfully\n", file = stderr())
flush(stderr())

combine_functions <- function(data) {
  message("DEBUG: In combine_functions, nrow(data) = ", nrow(data))
  result <- vec_cbind(data, vec_rbind(!!!pull(data, functions)))
  message("DEBUG: combine_functions completed, nrow(result) = ", nrow(result))
  result
}

get_exported_functions <- function(decorations, export_tag) {
  message("DEBUG: Starting get_exported_functions")
  message("DEBUG: nrow(decorations) = ", nrow(decorations))
  message("DEBUG: export_tag = ", paste(export_tag, collapse = ", "))

  message("DEBUG: About to filter decorations...")
  step1 <- filter(decorations, decoration %in% paste0(export_tag, "::export"))
  message("DEBUG: After filter, nrow = ", nrow(step1))

  message("DEBUG: About to mutate with map()...")
  step2 <- mutate(step1, functions = map(context, decor:::parse_cpp_function))
  message("DEBUG: After mutate, nrow = ", nrow(step2))

  message("DEBUG: About to call combine_functions()...")
  step3 <- combine_functions(step2)
  message("DEBUG: After combine_functions, nrow = ", nrow(step3))

  message("DEBUG: About to select(-functions)...")
  step4 <- select(step3, -functions)
  message("DEBUG: After select, nrow = ", nrow(step4))

  message("DEBUG: About to mutate decoration...")
  out <- mutate(step4, decoration = sub("::export", "", decoration))
  message("DEBUG: After final mutate, nrow = ", nrow(out))

  message(glue(
    "*** > {n} functions decorated with [[{tags}::export]]",
    n = nrow(out),
    tags = paste0(export_tag, collapse = "|")
  ))
  out
}

glue_collapse_data <- function(data, ..., sep = ", ", last = "") {
  res <- glue_collapse(glue_data(data, ...), sep = sep, last = last)
  if (length(res) == 0) {
    res <- ""
  }
  res
}

wrap_call <- function(name, return_type, args) {
  call <- glue::glue("{name}({list_params})", list_params = glue_collapse_data(args, "{name}"))
  if (return_type == "void") {
    glue::glue("\t{call};\n\treturn R_NilValue;", .trim = FALSE)
  } else {
    glue::glue("\treturn cpp11::as_sexp({call});")
  }
}

feature_available <- function(feat) {
  glue::glue(
    'extern "C" SEXP _{feat}_available() {{
return Rf_ScalarLogical(
#if defined(ARROW_R_WITH_{toupper(feat)})
  TRUE
#else
  FALSE
#endif
);
}}
'
  )
}

write_if_modified <- function(code, file) {
  old <- try(readLines(file), silent = TRUE)
  new <- unclass(unlist(strsplit(code, "\n")))
  # We don't care about changes in empty lines
  if (!identical(old[nzchar(old)], new[nzchar(new)])) {
    writeLines(con = file, code)
    # To debug why they're different if you think they shouldn't be:
    # print(waldo::compare(old[nzchar(old)], new[nzchar(new)]))
    message(glue::glue("*** > generated file `{file}`"))
  } else {
    message(glue::glue("*** > `{file}` not modified"))
  }
}

message("DEBUG: About to call cpp_decorations()...")
all_decorations <- cpp_decorations()
message("DEBUG: cpp_decorations() completed, nrow = ", nrow(all_decorations))

# Include "arrow" with features here
message("DEBUG: About to call get_exported_functions()...")
arrow_exports <- get_exported_functions(all_decorations, c("arrow", features))
message("DEBUG: get_exported_functions() completed, nrow = ", nrow(arrow_exports))

arrow_classes <- c(
  "Table" = "arrow::Table",
  "RecordBatch" = "arrow::RecordBatch"
)

# This takes a cpp11 C wrapper and conditionally makes it available based on
# a feature decoration
ifdef_wrap <- function(cpp11_wrapped, name, sexp_signature, decoration) {
  if (identical(decoration, "arrow")) {
    # Arrow is now required so don't wrap them
    return(cpp11_wrapped)
  }
  glue(
    '
#if defined(ARROW_R_WITH_{toupper(decoration)})
{cpp11_wrapped}
#else
extern "C" SEXP {sexp_signature}{{
\tRf_error("Cannot call {name}(). See https://arrow.apache.org/docs/r/articles/install.html for help installing Arrow C++ libraries. ");
}}
#endif\n\n'
  )
}

message("DEBUG: About to generate cpp_functions_definitions...")
cpp_functions_definitions <- arrow_exports |>
  select(name, return_type, args, file, line, decoration) |>
  pmap_chr(function(name, return_type, args, file, line, decoration) {
    sexp_params <- glue_collapse_data(args, "SEXP {name}_sexp")
    sexp_signature <- glue("_arrow_{name}({sexp_params})")
    cpp11_wrapped <- glue(
      '
      {return_type} {name}({real_params});
      extern "C" SEXP {sexp_signature}{{
      BEGIN_CPP11
      {input_params}{return_line}{wrap_call(name, return_type, args)}
      END_CPP11
      }}',
      sep = "\n",
      real_params = glue_collapse_data(args, "{type} {name}"),
      input_params = glue_collapse_data(args, "\tarrow::r::Input<{type}>::type {name}({name}_sexp);", sep = "\n"),
      return_line = if (nrow(args)) "\n" else ""
    )

    glue::glue(
      "
    // {basename(file)}
    {ifdef_wrap(cpp11_wrapped, name, sexp_signature, decoration)}
    ",
      sep = "\n"
    )
  }) |>
  glue_collapse(sep = "\n")
message("DEBUG: Finished generating cpp_functions_definitions")

message("DEBUG: About to generate cpp_functions_registration...")
cpp_functions_registration <- arrow_exports |>
  select(name, return_type, args) |>
  pmap_chr(function(name, return_type, args) {
    glue('\t\t{{ "_arrow_{name}", (DL_FUNC) &_arrow_{name}, {nrow(args)}}}, ')
  }) |>
  glue_collapse(sep = "\n")
message("DEBUG: Finished generating cpp_functions_registration")

cpp_file_header <- '// Generated by using data-raw/codegen.R -> do not edit by hand
#include <cpp11.hpp>
#include <cpp11/declarations.hpp>

#include "./arrow_types.h"
'

arrow_exports_cpp <- paste0(
  glue::glue(
    "
{cpp_file_header}
{cpp_functions_definitions}
\n"
  ),
  glue::glue_collapse(
    glue::glue(
      "
{feature_available({features})}
"
    ),
    sep = "\n"
  ),
  "
static const R_CallMethodDef CallEntries[] = {
",
  glue::glue_collapse(
    glue::glue(
      '\t\t{{ "_{features}_available", (DL_FUNC)& _{features}_available, 0 }},'
    ),
    sep = "\n"
  ),
  glue::glue(
    "\n
{cpp_functions_registration}
\t\t{{NULL, NULL, 0}}
}};
\n"
  ),
  'extern "C" void R_init_arrow(DllInfo* dll){
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);

  #if defined(HAS_ALTREP)
  arrow::r::altrep::Init_Altrep_classes(dll);
  #endif

  _arrow_compute__Initialize();
}
\n'
)

message("DEBUG: About to write src/arrowExports.cpp...")
write_if_modified(arrow_exports_cpp, "src/arrowExports.cpp")
message("DEBUG: Finished writing src/arrowExports.cpp")

message("DEBUG: About to generate R functions...")
r_functions <- arrow_exports |>
  select(name, return_type, args) |>
  pmap_chr(function(name, return_type, args) {
    params <- if (nrow(args)) {
      paste0(", ", glue_collapse_data(args, "{name}"))
    } else {
      ""
    }
    call <- glue::glue(".Call(`_arrow_{name}`{params})")
    if (return_type == "void") {
      call <- glue::glue("invisible({call})")
    }

    glue::glue(
      "
    {name} <- function({list_params}) {{
      {call}
    }}

    ",
      list_params = glue_collapse_data(args, "{name}"),
      sep = "\n"
    )
  }) |>
  glue_collapse(sep = "\n")

arrow_exports_r <- glue::glue(
  "
# Generated by using data-raw/codegen.R -> do not edit by hand

{r_functions}
"
)

message("DEBUG: About to write R/arrowExports.R...")
write_if_modified(arrow_exports_r, "R/arrowExports.R")
message("DEBUG: Finished writing R/arrowExports.R")

message("DEBUG: codegen.R completed successfully!")
