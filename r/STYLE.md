<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Style

This is a style guide to writing documentation for arrow.

## Coding style

Please use the [tidyverse coding style](https://style.tidyverse.org/).

## Referring to external packages

When referring to external packages, include a link to the package at the first mention, and subsequently refer to it in plain text, e.g.

* "The arrow R package provides a [dplyr](https://dplyr.tidyverse.org/) interface to Arrow Datasets.  This article introduces Datasets and shows how to use dplyr to analyze them."

## Data frames

When referring to the concept, use the phrase "data frame", whereas when referring to an object of that class or when the class is important, write `data.frame`, e.g.

* "You can call `write_dataset()` on tabular data objects such as Arrow Tables or RecordBatches, or R data frames. If working with data frames you might want to use a `tibble` instead of a `data.frame` to take advantage of the default behaviour of partitioning data based on grouped variables."
