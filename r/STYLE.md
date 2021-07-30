# Style

This is a style guide to writing documentation for arrow.

## Coding style

Please use the [tidyverse coding style](https://style.tidyverse.org/).

## Referring to external packages

When referring to external packages, include a link to the package at the first mention, and subsequently refer to it in plain text, e.g.

* "The arrow R package provides a [dplyr](https://dplyr.tidyverse.org/) interface to Arrow Datasets.  This vignette introduces Datasets and shows how to use dplyr to analyze them."

## Data frames

When referring to the concept, use the phrase "data frame", whereas when referring to an object of that class or when the class is important, write `data.frame`, e.g.

* "You can call `write_dataset()` on tabular data objects such as Arrow Tables or RecordBatchs, or R data frames. If working with data frames you might want to use a `tibble` instead of a `data.frame` to take advantage of the default behaviour of partitioning data based on grouped variables."
