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
      ExecNode_Scan(self, dataset, filter, colnames)
    },
    Run = function(node) {
      assert_is(node, "ExecNode")
      ExecPlan_run(self, node)
    }
  )
)
ExecPlan$create <- function(use_threads = option_use_threads()) {
  ExecPlan_create(use_threads)
}

ExecNode <- R6Class("ExecNode",
  inherit = ArrowObject,
  public = list(
    Project = function(cols) {
      assert_is_list_of(cols, "Expression")
      ExecNode_Project(self, cols, names(cols))
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
