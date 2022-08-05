library(dplyr, warn.conflicts = FALSE)

test_that("binding translation works", {
  nchar2 <- function(x) {
    1 + nchar(x)
  }

  # simple expression
  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var2 = nchar2(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  # a slightly more complicated expression
  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var2 = 1 + nchar2(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  nchar3 <- function(x) {
    2 + nchar(x)
  }
  # multiple unknown calls in the same expression (to test the iteration)
  tibble::tibble(my_string = "1234") %>%
    arrow_table() %>%
    mutate(
      var1 = nchar(my_string),
      var2 = nchar2(my_string) + nchar3(my_string)) %>%
    collect()

  # user function defined using namespacing
  nchar4 <- function(x) {
    2 + base::nchar(x)
  }

  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var2 = 1 + nchar4(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )
})
