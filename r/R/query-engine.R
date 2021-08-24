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

  if (length(final_node$sort$temp_columns) > 0) {
    # If arrange() created $temp_columns, make sure to omit them from the result
    tab <- tab[, setdiff(names(tab), final_node$sort$temp_columns), drop = FALSE]
  }

  tab
}

ExecPlan <- R6Class("ExecPlan",
  inherit = ArrowObject,
  public = list(
    Scan = function(dataset) {
      # Handle arrow_dplyr_query
      if (inherits(dataset, "arrow_dplyr_query")) {
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
      group_vars <- dplyr::group_vars(.data)
      grouped <- length(group_vars) > 0

      # Collect the target names first because we have to add back the group vars
      target_names <- names(.data)
      .data <- ensure_group_vars(.data)
      .data <- ensure_arrange_vars(.data) # this sets x$temp_columns

      node <- self$Scan(.data)
      # ARROW-13498: Even though Scan takes the filter, apparently we have to do it again
      if (inherits(.data$filtered_rows, "Expression")) {
        node <- node$Filter(.data$filtered_rows)
      }
      # If any columns are derived we need to Project (otherwise this may be no-op)
      node <- node$Project(c(.data$selected_columns, .data$temp_columns))

      if (length(.data$aggregations)) {
        if (grouped) {
          # We need to prefix all of the aggregation function names with "hash_"
          .data$aggregations <- lapply(.data$aggregations, function(x) {
            x[["fun"]] <- paste0("hash_", x[["fun"]])
            x
          })
        }

        node <- node$Aggregate(
          options = .data$aggregations,
          target_names = target_names,
          out_field_names = names(.data$aggregations),
          key_names = group_vars
        )

        if (grouped) {
          # The result will have result columns first then the grouping cols.
          # dplyr orders group cols first, so adapt the result to meet that expectation.
          node <- node$Project(
            make_field_refs(c(group_vars, names(.data$aggregations)))
          )
        }
      }

      # tab <- tab[
      #   tab$SortIndices(names(x$arrange_vars), x$arrange_desc),
      #   names(x$selected_columns), # this omits x$temp_columns from the result
      #   drop = FALSE
      # ]

      # Apply sorting: this is currently not an ExecNode itself, it is a
      # sink node option.
      # TODO: error if doing a subsequent operation that would throw away sorting!
      if (length(.data$arrange_vars)) {
        node$sort <- list(
          names = names(.data$arrange_vars),
          orders = as.integer(.data$arrange_desc),
          temp_columns = names(.data$temp_columns)
        )
      } else if (length(.data$aggregations) && grouped) {
        node$sort <- list(
          names = group_vars,
          orders = rep(0L, length(group_vars))
        )
      }
      node
    },
    Run = function(node) {
      assert_is(node, "ExecNode")
      ExecPlan_run(self, node, node$sort %||% list())
    }
  )
)
ExecPlan$create <- function(use_threads = option_use_threads()) {
  ExecPlan_create(use_threads)
}

ExecNode <- R6Class("ExecNode",
  inherit = ArrowObject,
  public = list(
    sort = NULL,
    Project = function(cols) {
      if (length(cols)) {
        assert_is_list_of(cols, "Expression")
        ExecNode_Project(self, cols, names(cols))
      } else {
        ExecNode_Project(self, character(0), character(0))
      }
    },
    Filter = function(expr) {
      assert_is(expr, "Expression")
      ExecNode_Filter(self, expr)
    },
    Aggregate = function(options, target_names, out_field_names, key_names) {
      ExecNode_Aggregate(self, options, target_names, out_field_names, key_names)
    }
  )
)