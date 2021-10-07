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

do_exec_plan <- function(.data) {
  plan <- ExecPlan$create()
  final_node <- plan$Build(.data)
  tab <- plan$Run(final_node)

  # TODO (ARROW-14289): make the head/tail methods return RBR not Table
  if (inherits(tab, "RecordBatchReader")) {
    tab <- tab$read_table()
  }

  # If arrange() created $temp_columns, make sure to omit them from the result
  # We can't currently handle this in the ExecPlan itself because sorting
  # happens in the end (SinkNode) so nothing comes after it.
  if (length(final_node$sort$temp_columns) > 0) {
    tab <- tab[, setdiff(names(tab), final_node$sort$temp_columns), drop = FALSE]
  }

  if (ncol(tab)) {
    # Apply any column metadata from the original schema, where appropriate
    original_schema <- source_data(.data)$schema
    # TODO: do we care about other (non-R) metadata preservation?
    # How would we know if it were meaningful?
    r_meta <- original_schema$r_metadata
    if (!is.null(r_meta)) {
      # Filter r_metadata$columns on columns with name _and_ type match
      new_schema <- tab$schema
      common_names <- intersect(names(r_meta$columns), names(tab))
      keep <- common_names[
        map_lgl(common_names, ~ original_schema[[.]] == new_schema[[.]])
      ]
      r_meta$columns <- r_meta$columns[keep]
      if (has_aggregation(.data)) {
        # dplyr drops top-level attributes if you do summarize
        r_meta$attributes <- NULL
      }
      tab$r_metadata <- r_meta
    }
  }

  tab
}

ExecPlan <- R6Class("ExecPlan",
  inherit = ArrowObject,
  public = list(
    Scan = function(dataset) {
      # Handle arrow_dplyr_query
      if (inherits(dataset, "arrow_dplyr_query")) {
        if (inherits(dataset$.data, "RecordBatchReader")) {
          return(ExecNode_ReadFromRecordBatchReader(self, dataset$.data))
        }

        filter <- dataset$filtered_rows
        if (isTRUE(filter)) {
          filter <- Expression$scalar(TRUE)
        }
        # Use FieldsInExpression to find all from dataset$selected_columns
        colnames <- unique(unlist(map(
          dataset$selected_columns,
          field_names_in_expression
        )))
        dataset <- dataset$.data
        assert_is(dataset, "Dataset")
      } else {
        if (inherits(dataset, "ArrowTabular")) {
          dataset <- InMemoryDataset$create(dataset)
        }
        assert_is(dataset, "Dataset")
        # Set some defaults
        filter <- Expression$scalar(TRUE)
        colnames <- names(dataset)
      }
      # ScanNode needs the filter to do predicate pushdown and skip partitions,
      # and it needs to know which fields to materialize (and which are unnecessary)
      ExecNode_Scan(self, dataset, filter, colnames %||% character(0))
    },
    Build = function(.data) {
      # This method takes an arrow_dplyr_query and chains together the
      # ExecNodes that they produce. It does not evaluate them--that is Run().
      group_vars <- dplyr::group_vars(.data)
      grouped <- length(group_vars) > 0

      # Collect the target names first because we have to add back the group vars
      target_names <- names(.data)
      .data <- ensure_group_vars(.data)
      .data <- ensure_arrange_vars(.data) # this sets .data$temp_columns

      if (inherits(.data$.data, "arrow_dplyr_query")) {
        # We have a nested query. Recurse.
        node <- self$Build(.data$.data)
      } else {
        node <- self$Scan(.data)
      }

      # ARROW-13498: Even though Scan takes the filter, apparently we have to do it again
      if (inherits(.data$filtered_rows, "Expression")) {
        node <- node$Filter(.data$filtered_rows)
      }

      if (!is.null(.data$aggregations)) {
        # Project to include just the data required for each aggregation,
        # plus group_by_vars (last)
        # TODO: validate that none of names(aggregations) are the same as names(group_by_vars)
        # dplyr does not error on this but the result it gives isn't great
        node <- node$Project(summarize_projection(.data))

        if (grouped) {
          # We need to prefix all of the aggregation function names with "hash_"
          .data$aggregations <- lapply(.data$aggregations, function(x) {
            x[["fun"]] <- paste0("hash_", x[["fun"]])
            x
          })
        }

        node <- node$Aggregate(
          options = map(.data$aggregations, ~ .[c("fun", "options")]),
          target_names = names(.data$aggregations),
          out_field_names = names(.data$aggregations),
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
            node$sort <- list(
              names = group_vars,
              orders = rep(0L, length(group_vars))
            )
          }
        }
      } else if (!is.null(.data$join)) {
        node <- node$Join(
          type = .data$join$type,
          right_node = self$Build(.data$join$right_data),
          by = .data$join$by,
          left_output = names(.data),
          right_output = setdiff(names(.data$join$right_data), .data$join$by)
        )
      } else if (length(node$schema)) {
        # If any columns are derived, reordered, or renamed we need to Project
        # If there are aggregations, the projection was already handled above
        # We have to project at least once to eliminate some junk columns
        # that the ExecPlan adds:
        # __fragment_index, __batch_index, __last_in_fragment
        # Presumably extraneous repeated projection of the same thing
        # (as when we've done collapse() and not projected after) is cheap/no-op
        projection <- c(.data$selected_columns, .data$temp_columns)
        node <- node$Project(projection)
      }

      # Apply sorting: this is currently not an ExecNode itself, it is a
      # sink node option.
      # TODO: handle some cases:
      # (1) arrange > summarize > arrange
      # (2) ARROW-13779: arrange then operation where order matters (e.g. cumsum)
      if (length(.data$arrange_vars)) {
        node$sort <- list(
          names = names(.data$arrange_vars),
          orders = as.integer(.data$arrange_desc),
          temp_columns = names(.data$temp_columns)
        )
      }

      # This is only safe because we are going to evaluate queries that end
      # with head/tail first, then evaluate any subsequent query as a new query
      if (!is.null(.data$head)) {
        node$head <- .data$head
      }
      if (!is.null(.data$tail)) {
        node$tail <- .data$tail
      }

      node
    },
    Run = function(node) {
      assert_is(node, "ExecNode")
      # TODO (ARROW-12763): pass head/tail to ExecPlan_run so we can maybe TopK
      out <- ExecPlan_run(self, node, node$sort %||% list())

      if (is.null(node$sort)) {
        # Since ExecPlans don't scan in deterministic order, head/tail are both
        # essentially taking a random slice from somewhere in the dataset.
        # And since the head() implementation is way more efficient than tail(),
        # just use it to take the random slice
        slice_size <- node$head %||% node$tail
        if (!is.null(slice_size)) {
          out <- head(out, slice_size)
        }
        # TODO (ARROW-12763): delete these else cases because they'll be handled
        # with SelectK
      } else if (!is.null(node$head)) {
        # These methods are on RecordBatchReader (but return Table)
        # TODO (ARROW-14289): make the head/tail methods return RBR not Table
        out <- head(out, node$head)
        # We can now tell `self` to StopProducing: we already have
        # everything we need for the head
        self$Stop()
      } else if (!is.null(node$tail)) {
        out <- tail(out, node$tail)
      }

      out
    },
    Stop = function() ExecPlan_StopProducing(self)
  )
)
ExecPlan$create <- function(use_threads = option_use_threads()) {
  ExecPlan_create(use_threads)
}

ExecNode <- R6Class("ExecNode",
  inherit = ArrowObject,
  public = list(
    # `sort` is a slight hack to be able to keep around arrange() params,
    # which don't currently yield their own ExecNode but rather are consumed
    # in the SinkNode (in ExecPlan$run())
    sort = NULL,
    # Similar hacks for head and tail
    head = NULL,
    tail = NULL,
    preserve_sort = function(new_node) {
      new_node$sort <- self$sort
      new_node$head <- self$head
      new_node$tail <- self$tail
      new_node
    },
    Project = function(cols) {
      if (length(cols)) {
        assert_is_list_of(cols, "Expression")
        self$preserve_sort(ExecNode_Project(self, cols, names(cols)))
      } else {
        self$preserve_sort(ExecNode_Project(self, character(0), character(0)))
      }
    },
    Filter = function(expr) {
      assert_is(expr, "Expression")
      self$preserve_sort(ExecNode_Filter(self, expr))
    },
    Aggregate = function(options, target_names, out_field_names, key_names) {
      self$preserve_sort(
        ExecNode_Aggregate(self, options, target_names, out_field_names, key_names)
      )
    },
    Join = function(type, right_node, by, left_output, right_output) {
      self$preserve_sort(
        ExecNode_Join(
          self,
          type,
          right_node,
          left_keys = names(by),
          right_keys = by,
          left_output = left_output,
          right_output = right_output
        )
      )
    }
  ),
  active = list(
    schema = function() ExecNode_output_schema(self)
  )
)
