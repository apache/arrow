
test_that("extension types can be created", {
  type <- MakeExtensionType(
    int32(),
    "arrow_r.simple_extension",
    charToRaw("some custom metadata"),
  )

  expect_r6_class(type, "ExtensionType")
  expect_identical(type$extension_name(), "arrow_r.simple_extension")
  expect_true(type$storage_type() == int32())
  expect_identical(type$storage_id(), int32()$id)
  expect_identical(type$Serialize(), charToRaw("some custom metadata"))
  expect_identical(type$ToString(), "ExtensionType <some custom metadata>")
})
