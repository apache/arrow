
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

  storage <- Array$create(1:10)
  array <- type$MakeArray(storage$data())
  expect_r6_class(array, "ExtensionArray")
  expect_r6_class(array$type, "ExtensionType")

  expect_true(array$type == type)
  expect_true(array$storage() == storage)
})

test_that("extension type subclasses work", {
  SomeExtensionTypeSubclass <- R6Class(
    "SomeExtensionTypeSubclass", inherit = ExtensionType,
    public = list(
      some_custom_method = function() {
        private$some_custom_field
      },

      .Deserialize = function(storage_type, extension_name, extension_metadata) {
        private$some_custom_field <- head(extension_metadata, 5)
      }
    ),
    private = list(
      some_custom_field = NULL
    )
  )

  SomeExtensionArraySubclass <- R6Class(
    "SomeExtensionArraySubclass", inherit = ExtensionArray
  )

  type <- MakeExtensionType(
    int32(),
    "some_extension_subclass",
    charToRaw("some custom metadata"),
    type_class = SomeExtensionTypeSubclass
  )

  expect_r6_class(type, "SomeExtensionTypeSubclass")
  expect_identical(type$some_custom_method(), charToRaw("some "))



})

test_that("extension types can be registered", {

})


