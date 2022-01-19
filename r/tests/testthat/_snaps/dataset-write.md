# write_dataset checks for format-specific arguments

    Code
      write_dataset(df, dst_dir, format = "feather", compression = "snappy")
    Error <rlang_error>
      "compression" is not a valid argument for your chosen `format`.
      i You could try using `codec` instead of `compression`.

---

    Code
      write_dataset(df, dst_dir, format = "feather", nonsensical_arg = "blah-blah")
    Error <rlang_error>
      "nonsensical_arg" is not a valid argument for your chosen `format`.

---

    Code
      write_dataset(df, dst_dir, format = "arrow", nonsensical_arg = "blah-blah")
    Error <rlang_error>
      "nonsensical_arg" is not a valid argument for your chosen `format`.

---

    Code
      write_dataset(df, dst_dir, format = "ipc", nonsensical_arg = "blah-blah")
    Error <rlang_error>
      "nonsensical_arg" is not a valid argument for your chosen `format`.

---

    Code
      write_dataset(df, dst_dir, format = "csv", nonsensical_arg = "blah-blah")
    Error <rlang_error>
      "nonsensical_arg" is not a valid argument for your chosen `format`.

---

    Code
      write_dataset(df, dst_dir, format = "parquet", nonsensical_arg = "blah-blah")
    Error <rlang_error>
      "nonsensical_arg" is not a valid argument for your chosen `format`.

