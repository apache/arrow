## ----setup, include = FALSE----------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)
library(arrow, warn.conflicts = FALSE)

## ---- eval = FALSE-------------------------------------------------------
#  # install.packages("remotes")
#  remotes::install_github("apache/arrow/r")

## ------------------------------------------------------------------------
library(arrow, warn.conflicts = FALSE)
t1 <- int32()
t2 <- utf8()
t5 <- timestamp(TimeUnit$MILLI)

t1
t2
t5

## ------------------------------------------------------------------------
t6 <- list_of(t1)
t6

## ------------------------------------------------------------------------
t7 <- struct(s0 = int32(), s3 = list_of(int16()))
t7

## ------------------------------------------------------------------------
s <- schema(
  field0 = int32(), 
  field1 = utf8(), 
  field3 = list_of(int32())
)
s

## ------------------------------------------------------------------------
a <- array(1:10)
a

## ------------------------------------------------------------------------
# TODO: should this be an active like in python ?
# a$type rather than a$type()
a$type()

## ------------------------------------------------------------------------
a$length()
length(a)

# TODO: should this be an active like in python ?
# a$null_count rather than a$null_count()
a$null_count()

## ------------------------------------------------------------------------
f <- factor(c("a", "b"), levels = c("a", "b", "c"))
a <- array(f)
a$type()
a$indices()
a$dictionary()
a

## ------------------------------------------------------------------------
tbl <- tibble::tibble(
  f0 = 1:4, 
  f1 = c("foo", "bar", "baz", NA), 
  f2 = c(TRUE, NA, FALSE, NA)
)
batch <- record_batch(tbl)
batch$num_columns()
batch$num_rows()

# convert a record batch back to a tibble
as_tibble(batch)

## ------------------------------------------------------------------------
batch$Slice(2)
batch$Slice(2, 1)

## ------------------------------------------------------------------------
tab <- table(tbl)
tab
tab$num_columns()
tab$num_rows()

## ------------------------------------------------------------------------
tab$column(0L)
tab$column(0L)$data()
tab$column(0L)$data()$chunks()

