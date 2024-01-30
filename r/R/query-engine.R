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
# nolint start: cyclocomp_linter,
ExecPlan <- R6Class("ExecPlan",
  inherit = ArrowObject,
  public = list(
    Scan = function(dataset) {
      if (inherits(dataset, c("RecordBatchReader", "ArrowTabular"))) {
        return(self$SourceNode(dataset))
      } else if (inherits(dataset, "arrow_dplyr_query")) {
        if (inherits(dataset$.data, c("RecordBatchReader", "ArrowTabular"))) {
          # There's no predicate pushdown to do, so no need to deal with other
          # arrow_dplyr_query attributes here. They'll be handled by other
          # ExecNodes
          return(self$SourceNode(dataset$.data))
        }
        # Else, we're scanning a Dataset, and we need to pull out the filter
        # and projection (column selection) to push down into the scanner
        filter <- dataset$filtered_rows
        if (isTRUE(filter)) {
          filter <- Expression$scalar(TRUE)
        }
        projection <- dataset$selected_columns
        dataset <- dataset$.data
        assert_is(dataset, "Dataset")
      } else {
        assert_is(dataset, "Dataset")
        # Just a dataset, not a query, so there's no predicates to push down
        # so set some defaults
        filter <- Expression$scalar(TRUE)
        projection <- make_field_refs(colnames)
      }

      out <- ExecNode_Scan(self, dataset, filter, projection)
      # Hold onto the source data's schema so we can preserve schema metadata
      # in the resulting Scan/Write
      out$extras$source_schema <- dataset$schema
      out
    },
    SourceNode = function(.data) {
      if (inherits(.data, "RecordBatchReader")) {
        out <- ExecNode_SourceNode(self, .data)
      } else {
        assert_is(.data, "ArrowTabular")
        out <- ExecNode_TableSourceNode(self, as_arrow_table(.data))
      }
      # Hold onto the source data's schema so we can preserve schema metadata
      # in the resulting Scan/Write
      out$extras$source_schema <- .data$schema
      out
    },
    Build = function(.data) {
      # This method takes an arrow_dplyr_query and chains together the
      # ExecNodes that they produce. It does not evaluate them--that is Run().
      group_vars <- dplyr::group_vars(.data)
      grouped <- length(group_vars) > 0

      .data <- ensure_group_vars(.data)
      .data <- ensure_arrange_vars(.data) # this sets .data$temp_columns

      if (is_collapsed(.data)) {
        # We have a nested query.
        if (has_unordered_head(.data$.data)) {
          # TODO(GH-34941): FetchNode should do non-deterministic fetch
          # Instead, we need to evaluate the query up to here,
          # and then do a new query for the rest.
          # as_record_batch_reader() will build and run an ExecPlan and do head() on it
          reader <- as_record_batch_reader(.data$.data)
          on.exit(reader$.unsafe_delete())
          node <- self$SourceNode(reader)
        } else {
          # Recurse
          node <- self$Build(.data$.data)
        }
      } else {
        node <- self$Scan(.data)
      }

      # ARROW-13498: Even though Scan takes the filter (if you have a Dataset),
      # we have to do it again
      if (inherits(.data$filtered_rows, "Expression")) {
        node <- node$Filter(.data$filtered_rows)
      }

      if (!is.null(.data$aggregations)) {
        # Project to include just the data required for each aggregation,
        # plus group_by_vars (last)
        # TODO: validate that none of names(aggregations) are the same as names(group_by_vars)
        # dplyr does not error on this but the result it gives isn't great
        projection <- summarize_projection(.data)
        # skip projection if no grouping and all aggregate functions are nullary
        if (length(projection)) {
          node <- node$Project(projection)
        }

        if (grouped) {
          # We need to prefix all of the aggregation function names with "hash_"
          .data$aggregations <- lapply(.data$aggregations, function(x) {
            x[["fun"]] <- paste0("hash_", x[["fun"]])
            x
          })
        }

        .data$aggregations <- imap(.data$aggregations, function(x, name) {
          # Embed `name` and `targets` inside the aggregation objects
          x[["name"]] <- name
          x[["targets"]] <- aggregate_target_names(x$data, name)
          x
        })

        node <- node$Aggregate(
          options = .data$aggregations,
          key_names = group_vars
        )
      } else {
        # If any columns are derived, reordered, or renamed we need to Project
        # If there are aggregations, the projection was already handled above.
        # We have to project at least once to eliminate some junk columns
        # that the ExecPlan adds:
        # __fragment_index, __batch_index, __last_in_fragment
        #
        # $Project() will check whether we actually need to project, so that
        # repeated projection of the same thing
        # (as when we've done collapse() and not projected after) is avoided
        projection <- c(.data$selected_columns, .data$temp_columns)
        node <- node$Project(projection)
        if (!is.null(.data$join)) {
          right_node <- self$Build(.data$join$right_data)

          node <- node$Join(
            type = .data$join$type,
            right_node = right_node,
            by = .data$join$by,
            left_output = .data$join$left_output,
            right_output = .data$join$right_output,
            left_suffix = .data$join$suffix[[1]],
            right_suffix = .data$join$suffix[[2]]
          )
        }

        if (!is.null(.data$union_all)) {
          node <- node$Union(self$Build(.data$union_all$right_data))
        }
      }

      # Apply sorting and head/tail
      head_or_tail <- .data$head %||% .data$tail
      if (length(.data$arrange_vars)) {
        if (!is.null(.data$tail)) {
          # Handle tail first: Reverse sort, take head
          # TODO(GH-34942): FetchNode support for tail
          node <- node$OrderBy(list(
            names = names(.data$arrange_vars),
            orders = as.integer(!.data$arrange_desc)
          ))
          node <- node$Fetch(.data$tail)
        }
        # Apply sorting
        node <- node$OrderBy(list(
          names = names(.data$arrange_vars),
          orders = as.integer(.data$arrange_desc)
        ))

        if (length(.data$temp_columns)) {
          # If we sorted on ad-hoc derived columns, Project to drop them
          temp_schema <- node$schema
          cols_to_keep <- setdiff(names(temp_schema), names(.data$temp_columns))
          node <- node$Project(make_field_refs(cols_to_keep))
        }

        if (!is.null(.data$head)) {
          # Take the head now
          node <- node$Fetch(.data$head)
        }
      } else if (!is.null(head_or_tail)) {
        # Unsorted head/tail
        # Handle a couple of special cases here:
        if (node$has_ordered_batches()) {
          # Data that has order, even implicit order from an in-memory table, is supported
          # in FetchNode
          if (!is.null(.data$head)) {
            node <- node$Fetch(.data$head)
          } else {
            # TODO(GH-34942): FetchNode support for tail
            # FetchNode currently doesn't support tail, but it has limit + offset
            # So if we know how many rows the query will result in, we can offset
            data_without_tail <- .data
            data_without_tail$tail <- NULL
            row_count <- nrow(data_without_tail)
            if (!is.na(row_count)) {
              node <- node$Fetch(.data$tail, offset = row_count - .data$tail)
            } else {
              # Workaround: non-deterministic tail
              node$extras$slice_size <- head_or_tail
            }
          }
        } else {
          # TODO(GH-34941): non-deterministic FetchNode
          # Data has non-deterministic order, so head/tail means "just show me any N rows"
          # FetchNode does not support non-deterministic scans, so we have to handle outside
          node$extras$slice_size <- head_or_tail
        }
      }
      node
    },
    Run = function(node) {
      assert_is(node, "ExecNode")
      out <- ExecPlan_run(
        self,
        node,
        prepare_key_value_metadata(node$final_metadata())
      )

      if (!is.null(node$extras$slice_size)) {
        # For non-deterministic scans, head/tail are
        # essentially taking a random slice from somewhere in the dataset.
        # And since the head() implementation is way more efficient than tail(),
        # just use it to take the random slice
        out <- head(out, node$extras$slice_size)
      }
      out
    },
    Write = function(node, ...) {
      # TODO(ARROW-16200): take FileSystemDatasetWriteOptions not ...
      final_metadata <- prepare_key_value_metadata(node$final_metadata())

      ExecPlan_Write(
        self,
        node,
        node$schema$WithMetadata(final_metadata),
        ...
      )
    },
    ToString = function() {
      ExecPlan_ToString(self)
    },
    .unsafe_delete = function() {
      ExecPlan_UnsafeDelete(self)
      super$.unsafe_delete()
    }
  )
)
# nolint end.

ExecPlan$create <- function(use_threads = option_use_threads()) {
  ExecPlan_create(use_threads)
}

ExecNode <- R6Class("ExecNode",
  inherit = ArrowObject,
  public = list(
    extras = list(
      # Workaround for non-deterministic head/tail
      slice_size = NULL,
      # `source_schema` is put here in Scan() so that at Run/Write, we can
      # extract the relevant metadata and keep it in the result
      source_schema = NULL
    ),
    preserve_extras = function(new_node) {
      new_node$extras <- self$extras
      new_node
    },
    final_metadata = function() {
      # Copy metadata from source schema and trim R column metadata to match
      # which columns are included in the result
      old_schema <- self$extras$source_schema
      old_meta <- old_schema$metadata
      old_meta$r <- get_r_metadata_from_old_schema(self$schema, old_schema)
      old_meta
    },
    has_ordered_batches = function() ExecNode_has_ordered_batches(self),
    Project = function(cols) {
      if (length(cols)) {
        assert_is_list_of(cols, "Expression")
        if (needs_projection(cols, self$schema)) {
          self$preserve_extras(ExecNode_Project(self, cols, names(cols)))
        } else {
          self
        }
      } else {
        self$preserve_extras(ExecNode_Project(self, character(0), character(0)))
      }
    },
    Filter = function(expr) {
      assert_is(expr, "Expression")
      self$preserve_extras(ExecNode_Filter(self, expr))
    },
    Aggregate = function(options, key_names) {
      out <- self$preserve_extras(
        ExecNode_Aggregate(self, options, key_names)
      )
      # dplyr drops top-level attributes when you call summarize()
      out$extras$source_schema$metadata[["r"]]$attributes <- NULL
      out
    },
    Join = function(type, right_node, by, left_output, right_output, left_suffix, right_suffix) {
      self$preserve_extras(
        ExecNode_Join(
          self,
          type,
          right_node,
          left_keys = names(by),
          right_keys = by,
          left_output = left_output,
          right_output = right_output,
          output_suffix_for_left = left_suffix,
          output_suffix_for_right = right_suffix
        )
      )
    },
    Union = function(right_node) {
      self$preserve_extras(ExecNode_Union(self, right_node))
    },
    Fetch = function(limit, offset = 0L) {
      self$preserve_extras(
        ExecNode_Fetch(self, offset, limit)
      )
    },
    OrderBy = function(sorting) {
      self$preserve_extras(
        ExecNode_OrderBy(self, sorting)
      )
    }
  ),
  active = list(
    schema = function() ExecNode_output_schema(self)
  )
)

ExecPlanReader <- R6Class("ExecPlanReader",
  inherit = RecordBatchReader,
  public = list(
    batches = function() ExecPlanReader__batches(self),
    read_table = function() Table__from_ExecPlanReader(self),
    Plan = function() ExecPlanReader__Plan(self),
    PlanStatus = function() ExecPlanReader__PlanStatus(self),
    ToString = function() {
      sprintf(
        "<Status: %s>\n\n%s\n\nSee $Plan() for details.",
        self$PlanStatus(),
        super$ToString()
      )
    }
  )
)

#' @export
head.ExecPlanReader <- function(x, n = 6L, ...) {
  # We need to make sure that the head() of an ExecPlanReader
  # is also an ExecPlanReader so that the evaluation takes place
  # in a way that supports calls into R.
  as_record_batch_reader(as_adq(RecordBatchReader__Head(x, n)))
}

do_exec_plan_substrait <- function(substrait_plan) {
  if (is.string(substrait_plan)) {
    substrait_plan <- substrait__internal__SubstraitFromJSON(substrait_plan)
  } else if (is.raw(substrait_plan)) {
    substrait_plan <- buffer(substrait_plan)
  } else {
    abort("`substrait_plan` must be a JSON string or raw() vector")
  }

  plan <- ExecPlan$create()
  on.exit(plan$.unsafe_delete())

  ExecPlan_run_substrait(plan, substrait_plan)
}

needs_projection <- function(projection, schema) {
  # Check whether `projection` would do anything to data with the given `schema`
  field_names <- set_names(map_chr(projection, ~ .$field_name), NULL)

  # We need to apply `projection` if:
  !all(nzchar(field_names)) || # Any of the Expressions are not FieldRefs
    !identical(field_names, names(projection)) || # Any fields are renamed
    !identical(field_names, names(schema)) # The fields are reordered
}
