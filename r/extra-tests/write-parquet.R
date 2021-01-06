library(arrow)

if (!dir.exists("extra-tests/files")) {
  dir.create("extra-tests/files")
}

example_with_metadata <- tibble::tibble(
  a = structure("one", class = "special_string"),
  b = 2,
  c = tibble::tibble(
    c1 = structure("inner", extra_attr = "something"),
    c2 = 4,
    c3 = 50
  ),
  d = "four"
)

attr(example_with_metadata, "top_level") <- list(
  field_one = 12,
  field_two = "more stuff"
)

write_parquet(example_with_metadata, "extra-tests/files/ex_data.parquet")
