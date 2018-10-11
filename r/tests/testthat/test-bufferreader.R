context("test-bufferreader")

test_that("BufferReader can be created from R objects", {
  num <- buffer_reader(numeric(13))
  int <- buffer_reader(integer(13))
  raw <- buffer_reader(raw(16))

  expect_is(num, "arrow::io::BufferReader")
  expect_is(int, "arrow::io::BufferReader")
  expect_is(raw, "arrow::io::BufferReader")

  expect_equal(num$GetSize(), 13*8)
  expect_equal(int$GetSize(), 13*4)
  expect_equal(raw$GetSize(), 16)
})

test_that("BufferReader can be created from Buffer", {
  buf <- buffer(raw(76))
  reader <- buffer_reader(buf)

  expect_is(reader, "arrow::io::BufferReader")
  expect_equal(reader$GetSize(), 76)
})
