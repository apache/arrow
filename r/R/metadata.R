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

#' @importFrom utils object.size
.serialize_arrow_r_metadata <- function(x) {
  assert_is(x, "list")

  # drop problems attributes (most likely from readr)
  x[["attributes"]][["problems"]] <- NULL

  out <- serialize(x, NULL, ascii = TRUE)

  # if the metadata is over 100 kB, compress
  if (option_compress_metadata() && object.size(out) > 100000) {
    out_comp <- serialize(memCompress(out, type = "gzip"), NULL, ascii = TRUE)

    # but ensure that the compression+serialization is effective.
    if (object.size(out) > object.size(out_comp)) out <- out_comp
  }

  rawToChar(out)
}

.unserialize_arrow_r_metadata <- function(x) {
  tryCatch(
    expr = {
      out <- unserialize(charToRaw(x))

      # if this is still raw, try decompressing
      if (is.raw(out)) {
        out <- unserialize(memDecompress(out, type = "gzip"))
      }
      out
    },
    error = function(e) {
      warning("Invalid metadata$r", call. = FALSE)
      NULL
    }
  )
}

#' @importFrom rlang trace_back
apply_arrow_r_metadata <- function(x, r_metadata, restore_class_attr = NULL) {
  tryCatch(
    expr = {
      columns_metadata <- r_metadata$columns
      restore_class_attr <- is.null(r_metadata$arrow_version) ||
        (r_metadata$arrow_version >= package_version("7.0.0.9000"))

      if (is.data.frame(x)) {
        if (length(names(x)) && !is.null(columns_metadata)) {
          for (name in intersect(names(columns_metadata), names(x))) {
            x[[name]] <- apply_arrow_r_metadata(
              x[[name]],
              columns_metadata[[name]],
              restore_class_attr = restore_class_attr
            )
          }
        }
      } else if (is.list(x) && !inherits(x, "POSIXlt") && !is.null(columns_metadata)) {
        # If we have a list and "columns_metadata" this applies row-level metadata
        # inside of a column in a dataframe.

        # However, if we are inside of a dplyr collection (including all datasets),
        # we cannot apply this row-level metadata, since the order of the rows is
        # not guaranteed to be the same, so don't even try, but warn what's going on
        trace <- trace_back()
        # TODO: remove `trace$calls %||% trace$call` once rlang > 0.4.11 is released
        in_dplyr_collect <- any(map_lgl(trace$calls %||% trace$call, function(x) {
          grepl("collect.arrow_dplyr_query", x, fixed = TRUE)[[1]]
        }))
        if (in_dplyr_collect) {
          warning(
            "Row-level metadata is not compatible with this operation and has ",
            "been ignored",
            call. = FALSE
          )
        } else {
          x <- map2(x, columns_metadata, function(.x, .y) {
            apply_arrow_r_metadata(.x, .y)
          })
        }
        x
      }

      # Restore the class attribute for metadata written before extension
      # types were implemented or for columns that were wrapped in I()
      saved_class <- r_metadata$attributes$class
      if (any(c("data.frame", "AsIs") %in% saved_class) || restore_class_attr) {
        class(x) <-saved_class %||% class(x)
      }

      # ...but warn if it's different than what was saved
      if (!is.null(saved_class) && !identical(class(x), saved_class)) {
        warn(
          sprintf(
            "Expected to restore object of class %s but got object of class %s",
            paste(r_metadata$attributes$class, collapse = " / "),
            paste(class(x), collapse = " / ")
          )
        )
      }

      if (!is.null(r_metadata$attributes)) {
        attr_names <- setdiff(names(r_metadata$attributes), "class")
        attributes(x)[attr_names] <- r_metadata$attributes[attr_names]

        # Make sure we can roundtrip a POSIXlt saved by versions before
        # vctrs extension types were implemented
        if (identical(r_metadata$attributes$class, c("POSIXlt", "POSIXlt"))) {
          # We store POSIXlt as a StructArray, which was translated back to R
          # as a data.frame, but while data frames have a row.names = c(NA, nrow(x))
          # attribute, POSIXlt does not, so since this is now no longer an object
          # of class data.frame, remove the extraneous attribute
          attr(x, "row.names") <- NULL
        }
        if (!is.null(attr(x, ".group_vars")) && requireNamespace("dplyr", quietly = TRUE)) {
          x <- dplyr::group_by(x, !!!syms(attr(x, ".group_vars")))
          attr(x, ".group_vars") <- NULL
        }
      }
    },
    error = function(e) {
      warning("Invalid metadata$r", call. = FALSE)
    }
  )
  x
}

remove_attributes <- function(x) {
  removed_attributes <- character()
  if (identical(class(x), c("tbl_df", "tbl", "data.frame"))) {
    removed_attributes <- c("class", "row.names", "names")
  } else if (inherits(x, "data.frame")) {
    removed_attributes <- c("row.names", "names")
  } else if (inherits(x, "factor")) {
    removed_attributes <- c("class", "levels")
  } else if (inherits(x, c("integer64", "Date", "arrow_binary", "arrow_large_binary"))) {
    removed_attributes <- c("class")
  } else if (inherits(x, "arrow_fixed_size_binary")) {
    removed_attributes <- c("class", "byte_width")
  } else if (inherits(x, "POSIXct")) {
    removed_attributes <- c("class", "tzone")
  } else if (inherits(x, "hms") || inherits(x, "difftime")) {
    removed_attributes <- c("class", "units")
  }
  removed_attributes
}

arrow_attributes <- function(x, only_top_level = FALSE) {
  att <- attributes(x)
  removed_attributes <- remove_attributes(x)

  if (inherits(x, "grouped_df")) {
    # Keep only the group var names, not the rest of the cached data that dplyr
    # uses, which may be large
    if (requireNamespace("dplyr", quietly = TRUE)) {
      gv <- dplyr::group_vars(x)
      x <- dplyr::ungroup(x)
      # ungroup() first, then set attribute, bc ungroup() would erase it
      att[[".group_vars"]] <- gv
      removed_attributes <- c(removed_attributes, "groups", "class")
    }
  }

  att <- att[setdiff(names(att), removed_attributes)]

  if (isTRUE(only_top_level)) {
    return(att)
  }

  if (is.data.frame(x)) {
    columns <- map(x, arrow_attributes)
    out <- if (length(att) || !all(map_lgl(columns, is.null))) {
      list(attributes = att, columns = columns)
    }

    # keep a record of the arrow version that wrote the metadata
    out[["version"]] <- as.character(packageVersion("arrow"))

    return(out)
  }

  columns <- NULL
  attempt_to_save_row_level <- getOption("arrow.preserve_row_level_metadata", FALSE) &&
    is.list(x) && !inherits(x, "POSIXlt")
  if (attempt_to_save_row_level) {
    # However, if we are inside of a dplyr collection (including all datasets),
    # we cannot apply this row-level metadata, since the order of the rows is
    # not guaranteed to be the same, so don't even try, but warn what's going on
    trace <- trace_back()
    # TODO: remove `trace$calls %||% trace$call` once rlang > 0.4.11 is released
    in_dataset_write <- any(map_lgl(trace$calls %||% trace$call, function(x) {
      grepl("write_dataset", x, fixed = TRUE)[[1]]
    }))
    if (in_dataset_write) {
      warning(
        "Row-level metadata is not compatible with datasets and will be discarded",
        call. = FALSE
      )
    } else {
      # for list columns, we also keep attributes of each
      # element in columns
      columns <- map(x, arrow_attributes)
    }
    if (all(map_lgl(columns, is.null))) {
      columns <- NULL
    }
  }

  if (length(att) || !is.null(columns)) {
    list(attributes = att, columns = columns)
  } else {
    NULL
  }
}

get_r_metadata_from_old_schema <- function(new_schema,
                                           old_schema,
                                           drop_attributes = FALSE) {
  # TODO: do we care about other (non-R) metadata preservation?
  # How would we know if it were meaningful?
  r_meta <- old_schema$r_metadata
  if (!is.null(r_meta)) {
    # Filter r_metadata$columns on columns with name _and_ type match
    common_names <- intersect(names(r_meta$columns), names(new_schema))
    keep <- common_names[
      map_lgl(common_names, ~ old_schema[[.]] == new_schema[[.]])
    ]
    r_meta$columns <- r_meta$columns[keep]
    if (drop_attributes) {
      # dplyr drops top-level attributes if you do summarize
      r_meta$attributes <- NULL
    }
  }
  r_meta
}
