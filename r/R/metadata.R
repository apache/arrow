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

  # remove the class if it's just data.frame
  if (identical(x$attributes$class, "data.frame")) {
    x$attributes <- x$attributes[names(x$attributes) != "class"]
    if (is_empty(x$attributes)) {
      x <- x[names(x) != "attributes"]
    }
  }

  out <- serialize(safe_r_metadata(x, on_save = TRUE), NULL, ascii = TRUE)

  # if the metadata is over 100 kB, compress
  if (option_compress_metadata() && object.size(out) > 100000) {
    out_comp <- serialize(memCompress(out, type = "gzip"), NULL, ascii = TRUE)

    # but ensure that the compression+serialization is effective.
    if (object.size(out) > object.size(out_comp)) out <- out_comp
  }

  rawToChar(out)
}

.deserialize_arrow_r_metadata <- function(x) {
  tryCatch(unserialize_r_metadata(x),
    error = function(e) {
      if (getOption("arrow.debug", FALSE)) {
        print(conditionMessage(e))
      }
      warning("Invalid metadata$r", call. = FALSE)
      NULL
    }
  )
}

unserialize_r_metadata <- function(x) {
  # Check that this is ASCII serialized data (as in, what we wrote)
  if (!identical(substr(unclass(x), 1, 1), "A")) {
    stop("Invalid serialized data")
  }
  out <- safe_unserialize(charToRaw(x))
  # If it's still raw, decompress and unserialize again
  if (is.raw(out)) {
    decompressed <- memDecompress(out, type = "gzip")
    if (!identical(rawToChar(decompressed[1]), "A")) {
      stop("Invalid serialized compressed data")
    }
    out <- safe_unserialize(decompressed)
  }
  if (!is.list(out)) {
    stop("Invalid serialized data: must be a list")
  }
  safe_r_metadata(out)
}

safe_unserialize <- function(x) {
  # By capturing the data in a list, we can inspect it for promises without
  # triggering their evaluation.
  out <- list(unserialize(x))
  if (typeof(out[[1]]) == "promise") {
    stop("Serialized data contains a promise object")
  }
  out[[1]]
}

safe_r_metadata <- function(metadata, on_save = FALSE) {
  # This function recurses through the metadata list and checks that all
  # elements are of types that are allowed in R metadata.
  # If it finds an element that is not allowed, it removes it.
  #
  # This function is used both when saving and loading metadata.
  # @param on_save: If TRUE, the function will not warn if it removes elements:
  # we're just cleaning up the metadata for saving. If FALSE, it means we're
  # loading the metadata, and we'll warn if we find invalid elements.
  #
  # When loading metadata, you can optionally keep the invalid elements by
  # setting `options(arrow.unsafe_metadata = TRUE)`. It will still check
  # for invalid elements and warn if any are found, though.

  # This variable will be used to store the types of elements that were removed,
  # if any, so we can give an informative warning if needed.
  types_removed <- c()

  # Internal function that we'll recursively apply,
  # and mutate the `types_removed` variable outside of it.
  check_r_metadata_types_recursive <- function(x) {
    allowed_types <- c("character", "double", "integer", "logical", "complex", "list", "NULL")
    # Pull out the attributes so we can also check them
    x_attrs <- attributes(x)

    if (is.list(x)) {
      # Add special handling for some base R classes that are list but
      # their [[ methods leads to infinite recursion.
      # We unclass here and then reapply attributes after.
      x <- unclass(x)

      types <- map_chr(x, typeof)
      ok <- types %in% allowed_types
      if (!all(ok)) {
        # Record the invalid types, then remove the offending elements
        types_removed <<- c(types_removed, setdiff(types, allowed_types))
        x <- x[ok]
        if ("names" %in% names(x_attrs)) {
          # Also prune from the attributes since we'll re-add later
          x_attrs[["names"]] <- x_attrs[["names"]][ok]
        }
      }
      # For the rest, recurse
      x <- map(x, check_r_metadata_types_recursive)
    }

    # attributes() of a named list will return a list with a "names" attribute,
    # so it will recurse indefinitely.
    if (!is.null(x_attrs) && !identical(x_attrs, list(names = names(x)))) {
      attributes(x) <- check_r_metadata_types_recursive(x_attrs)
    }
    x
  }
  new <- check_r_metadata_types_recursive(metadata)

  # On save: don't warn, just save the filtered metadata
  if (on_save) {
    return(new)
  }
  # On load: warn if any elements were removed
  if (length(types_removed)) {
    types_msg <- paste("Type:", oxford_paste(unique(types_removed)))
    if (getOption("arrow.unsafe_metadata", FALSE)) {
      # We've opted-in to unsafe metadata, so warn but return the original metadata
      rlang::warn(
        "R metadata may have unsafe or invalid elements",
        body = c("i" = types_msg)
      )
      new <- metadata
    } else {
      rlang::warn(
        "Potentially unsafe or invalid elements have been discarded from R metadata.",
        body = c(
          "i" = types_msg,
          ">" = "If you trust the source, you can set `options(arrow.unsafe_metadata = TRUE)` to preserve them."
        )
      )
    }
  }
  new
}

#' @importFrom rlang trace_back
apply_arrow_r_metadata <- function(x, r_metadata) {
  if (is.null(r_metadata)) {
    return(x)
  }
  tryCatch(
    expr = {
      columns_metadata <- r_metadata$columns
      if (is.data.frame(x)) {
        # if columns metadata exists, apply it here
        if (length(names(x)) && !is.null(columns_metadata)) {
          for (name in intersect(names(columns_metadata), names(x))) {
            x[[name]] <- apply_arrow_r_metadata(x[[name]], columns_metadata[[name]])
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
          grepl("collect\\.([aA]rrow|Dataset)", x)[[1]]
        }))
        if (in_dplyr_collect) {
          warning(
            "Row-level metadata is not compatible with this operation and has ",
            "been ignored",
            call. = FALSE
          )
        } else {
          if (length(x) > 0) {
            x <- map2(x, columns_metadata, function(.x, .y) {
              apply_arrow_r_metadata(.x, .y)
            })
          }
        }
        x
      }

      if (!is.null(r_metadata$attributes)) {
        attributes(x)[names(r_metadata$attributes)] <- r_metadata$attributes
        if (inherits(x, "POSIXlt")) {
          # We store POSIXlt as a StructArray, which is translated back to R
          # as a data.frame, but while data frames have a row.names = c(NA, nrow(x))
          # attribute, POSIXlt does not, so since this is now no longer an object
          # of class data.frame, remove the extraneous attribute
          attr(x, "row.names") <- NULL
        }
        if (!is.null(attr(x, ".group_vars")) && requireNamespace("dplyr", quietly = TRUE)) {
          x <- dplyr::group_by(
            x,
            !!!syms(attr(x, ".group_vars")),
            .drop = attr(x, ".group_by_drop") %||% TRUE
          )
          attr(x, ".group_vars") <- NULL
          attr(x, ".group_by_drop") <- NULL
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
      drop <- dplyr::group_by_drop_default(x)
      x <- dplyr::ungroup(x)
      # ungroup() first, then set attributes, bc ungroup() would erase it
      att[[".group_vars"]] <- gv
      att[[".group_by_drop"]] <- drop
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

get_r_metadata_from_old_schema <- function(new_schema, old_schema) {
  # TODO: do we care about other (non-R) metadata preservation?
  # How would we know if it were meaningful?
  r_meta <- old_schema$metadata$r
  if (!is.null(r_meta)) {
    # Filter r_metadata$columns on columns with name _and_ type match
    common_names <- intersect(names(r_meta$columns), names(new_schema))
    keep <- common_names[
      map_lgl(common_names, ~ old_schema[[.]] == new_schema[[.]])
    ]
    r_meta$columns <- r_meta$columns[keep]
  }
  r_meta
}
