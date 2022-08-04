library(dplyr, warn.conflicts = FALSE)

test_that("binding translation works", {
  nchar2 <- function(x) {
    1 + nchar(x)
  }

  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var2 = nchar2(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )

  compare_dplyr_binding(
    .input %>%
      mutate(
        var1 = nchar(my_string),
        var2 = 1 + nchar2(my_string)) %>%
      collect(),
    tibble::tibble(my_string = "1234")
  )
})
