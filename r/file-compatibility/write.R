library(arrow)

if (!dir.exists("file-compatibility/files")) {
  dir.create("file-compatibility/files")
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

write_parquet(example_with_metadata, "file-compatibility/files/ex_data.parquet")
