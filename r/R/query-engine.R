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
        if (has_head_tail(.data$.data)) {
          # head and tail are not ExecNodes; at best we can handle them via
          # SinkNode, so if there are any steps done after head/tail, we need to
          # evaluate the query up to then and then do a new query for the rest.
          # as_record_batch_reader() will build and run an ExecPlan
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

        if (grouped) {
          # The result will have result columns first then the grouping cols.
          # dplyr orders group cols first, so adapt the result to meet that expectation.
          node <- node$Project(
            make_field_refs(c(group_vars, names(.data$aggregations)))
          )
          if (getOption("arrow.summarise.sort", FALSE)) {
            # Add sorting instructions for the rows too to match dplyr
            # (see below about why sorting isn't itself a Node)
            node$extras$sort <- list(
              names = group_vars,
              orders = rep(0L, length(group_vars))
            )
          }
        }
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

      # Apply sorting: this is currently not an ExecNode itself, it is a
      # sink node option.
      # TODO: handle some cases:
      # (1) arrange > summarize > arrange
      # (2) ARROW-13779: arrange then operation where order matters (e.g. cumsum)
      if (length(.data$arrange_vars)) {
        node$extras$sort <- list(
          names = names(.data$arrange_vars),
          orders = .data$arrange_desc,
          temp_columns = names(.data$temp_columns)
        )
      }
      # This is only safe because we are going to evaluate queries that end
      # with head/tail first, then evaluate any subsequent query as a new query
      if (!is.null(.data$head)) {
        node$extras$head <- .data$head
      }
      if (!is.null(.data$tail)) {
        node$extras$tail <- .data$tail
      }
      node
    },
    Run = function(node) {
      assert_is(node, "ExecNode")

      # Sorting and head/tail (if sorted) are handled in the SinkNode,
      # created in ExecPlan_build
      sorting <- node$extras$sort %||% list()
      select_k <- node$extras$head %||% -1L
      has_sorting <- length(sorting) > 0
      if (has_sorting) {
        if (!is.null(node$extras$tail)) {
          # Reverse the sort order and take the top K, then after we'll reverse
          # the resulting rows so that it is ordered as expected
          sorting$orders <- !sorting$orders
          select_k <- node$extras$tail
        }
        sorting$orders <- as.integer(sorting$orders)
      }

      out <- ExecPlan_run(
        self,
        node,
        sorting,
        prepare_key_value_metadata(node$final_metadata()),
        select_k
      )

      if (!has_sorting) {
        # Since ExecPlans don't scan in deterministic order, head/tail are both
        # essentially taking a random slice from somewhere in the dataset.
        # And since the head() implementation is way more efficient than tail(),
        # just use it to take the random slice
        # TODO(ARROW-16628): handle limit in ExecNode
        slice_size <- node$extras$head %||% node$extras$tail
        if (!is.null(slice_size)) {
          out <- head(out, slice_size)
        }
      } else if (!is.null(node$extras$tail)) {
        # TODO(ARROW-16630): proper BottomK support
        # Reverse the row order to get back what we expect
        out <- as_arrow_table(out)
        out <- out[rev(seq_len(nrow(out))), , drop = FALSE]
        out <- as_record_batch_reader(out)
      }

      # If arrange() created $temp_columns, make sure to omit them from the result
      # We can't currently handle this in ExecPlan_run itself because sorting
      # happens in the end (SinkNode) so nothing comes after it.
      # TODO(ARROW-16631): move into ExecPlan
      if (length(node$extras$sort$temp_columns) > 0) {
        tab <- as_arrow_table(out)
        tab <- tab[, setdiff(names(tab), node$extras$sort$temp_columns), drop = FALSE]
        out <- as_record_batch_reader(tab)
      }

      out
    },
    Write = function(node, ...) {
      # TODO(ARROW-16200): take FileSystemDatasetWriteOptions not ...
      ExecPlan_Write(
        self,
        node,
        prepare_key_value_metadata(node$final_metadata()),
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
      # `sort` is a slight hack to be able to keep around arrange() params,
      # which don't currently yield their own ExecNode but rather are consumed
      # in the SinkNode (in ExecPlan$run())
      sort = NULL,
      # Similar hacks for head and tail
      head = NULL,
      tail = NULL,
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
