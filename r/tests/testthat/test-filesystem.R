# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

test_that("LocalFilesystem", {
  fs <- LocalFileSystem$create()
  expect_identical(fs$type_name, "local")
  DESCRIPTION <- system.file("DESCRIPTION", package = "arrow")
  info <- fs$GetFileInfo(DESCRIPTION)[[1]]
  expect_equal(info$base_name(), "DESCRIPTION")
  expect_equal(info$extension(), "")
  expect_equal(info$type, FileType$File)
  expect_equal(info$path, DESCRIPTION)
  info <- file.info(DESCRIPTION)

  expect_equal(info$size, info$size)
  expect_equal(info$mtime, info$mtime)

  tf <- tempfile(fileext = ".txt")
  fs$CopyFile(DESCRIPTION, tf)
  info <- fs$GetFileInfo(tf)[[1]]
  expect_equal(info$extension(), "txt")
  expect_equal(info$size, info$size)
  expect_equal(readLines(DESCRIPTION), readLines(tf))

  tf2 <- tempfile(fileext = ".txt")
  fs$Move(tf, tf2)
  infos <- fs$GetFileInfo(c(tf, tf2, dirname(tf)))
  expect_equal(infos[[1]]$type, FileType$NotFound)
  expect_equal(infos[[2]]$type, FileType$File)
  expect_equal(infos[[3]]$type, FileType$Directory)

  fs$DeleteFile(tf2)
  expect_equal(fs$GetFileInfo(tf2)[[1L]]$type, FileType$NotFound)
  expect_true(!file.exists(tf2))

  expect_equal(fs$GetFileInfo(tf)[[1L]]$type, FileType$NotFound)
  expect_true(!file.exists(tf))

  td <- tempfile()
  fs$CreateDir(td)
  expect_equal(fs$GetFileInfo(td)[[1L]]$type, FileType$Directory)
  fs$CopyFile(DESCRIPTION, file.path(td, "DESCRIPTION"))
  fs$DeleteDirContents(td)
  expect_equal(length(dir(td)), 0L)
  fs$DeleteDir(td)
  expect_equal(fs$GetFileInfo(td)[[1L]]$type, FileType$NotFound)

  tf3 <- tempfile()
  os <- fs$OpenOutputStream(path = tf3)
  bytes <- as.raw(1:40)
  os$write(bytes)
  os$close()

  is <- fs$OpenInputStream(tf3)
  buf <- is$Read(40)
  expect_equal(buf$data(), bytes)
  is$close()
})

test_that("SubTreeFilesystem", {
  dir.create(td <- tempfile())
  DESCRIPTION <- system.file("DESCRIPTION", package = "arrow")
  file.copy(DESCRIPTION, file.path(td, "DESCRIPTION"))

  st_fs <- SubTreeFileSystem$create(td)
  expect_r6_class(st_fs, "SubTreeFileSystem")
  expect_r6_class(st_fs, "FileSystem")
  expect_r6_class(st_fs$base_fs, "LocalFileSystem")
  expect_identical(
    capture.output(print(st_fs)),
    paste0("SubTreeFileSystem: ", "file://", st_fs$base_path)
  )

  # FIXME windows has a trailing slash for one but not the other
  # expect_identical(normalizePath(st_fs$base_path), normalizePath(td)) # nolint

  st_fs$CreateDir("test")
  st_fs$CopyFile("DESCRIPTION", "DESC.txt")
  infos <- st_fs$GetFileInfo(c("DESCRIPTION", "test", "nope", "DESC.txt"))
  expect_equal(infos[[1L]]$type, FileType$File)
  expect_equal(infos[[2L]]$type, FileType$Directory)
  expect_equal(infos[[3L]]$type, FileType$NotFound)
  expect_equal(infos[[4L]]$type, FileType$File)
  expect_equal(infos[[4L]]$extension(), "txt")

  local_fs <- LocalFileSystem$create()
  local_fs$DeleteDirContents(td)
  infos <- st_fs$GetFileInfo(c("DESCRIPTION", "test", "nope", "DESC.txt"))
  expect_equal(infos[[1L]]$type, FileType$NotFound)
  expect_equal(infos[[2L]]$type, FileType$NotFound)
  expect_equal(infos[[3L]]$type, FileType$NotFound)
  expect_equal(infos[[4L]]$type, FileType$NotFound)
})

test_that("LocalFileSystem + Selector", {
  fs <- LocalFileSystem$create()
  dir.create(td <- tempfile())
  writeLines("blah blah", file.path(td, "one.txt"))
  writeLines("yada yada", file.path(td, "two.txt"))
  dir.create(file.path(td, "dir"))
  writeLines("...", file.path(td, "dir", "three.txt"))

  selector <- FileSelector$create(td, recursive = TRUE)
  infos <- fs$GetFileInfo(selector)
  expect_equal(length(infos), 4L)
  types <- sapply(infos, function(.x) .x$type)
  expect_equal(sum(types == FileType$File), 3L)
  expect_equal(sum(types == FileType$Directory), 1L)

  selector <- FileSelector$create(td, recursive = FALSE)
  infos <- fs$GetFileInfo(selector)
  expect_equal(length(infos), 3L)
  types <- sapply(infos, function(.x) .x$type)
  expect_equal(sum(types == FileType$File), 2L)
  expect_equal(sum(types == FileType$Directory), 1L)
})

# This test_that block must be above the two that follow it because S3FileSystem$create
# uses a slightly different set of cpp code that is R-only, so if there are bugs
# in the initialization of S3 (e.g. ARROW-14667) they will not be caught because
# the blocks "FileSystem$from_uri" and "SubTreeFileSystem$create() with URI" actually
# initialize it
test_that("S3FileSystem", {
  skip_on_cran()
  skip_if_not_available("s3")
  skip_if_offline()
  s3fs <- S3FileSystem$create()
  expect_r6_class(s3fs, "S3FileSystem")
})

test_that("FileSystem$from_uri", {
  skip_on_cran()
  skip_if_not_available("s3")
  skip_if_offline()
  fs_and_path <- FileSystem$from_uri("s3://voltrondata-labs-datasets")
  expect_r6_class(fs_and_path$fs, "S3FileSystem")
  expect_identical(fs_and_path$fs$region, "us-east-2")
})

test_that("SubTreeFileSystem$create() with URI", {
  skip_on_cran()
  skip_if_not_available("s3")
  skip_if_offline()
  fs <- SubTreeFileSystem$create("s3://voltrondata-labs-datasets")
  expect_r6_class(fs, "SubTreeFileSystem")
  expect_identical(
    capture.output(print(fs)),
    "SubTreeFileSystem: s3://voltrondata-labs-datasets/"
  )
})

test_that("S3FileSystem$create() with proxy_options", {
  skip_on_cran()
  skip_if_not_available("s3")
  skip_if_offline()

  expect_error(
    S3FileSystem$create(proxy_options = "definitely not a valid proxy URI"),
    "Cannot parse URI"
  )
})

test_that("s3_bucket", {
  skip_on_cran()
  skip_if_not_available("s3")
  skip_if_offline()
  bucket <- s3_bucket("ursa-labs-r-test")
  expect_r6_class(bucket, "SubTreeFileSystem")
  expect_r6_class(bucket$base_fs, "S3FileSystem")
  expect_identical(bucket$region, "us-west-2")
  expect_identical(
    capture.output(print(bucket)),
    "SubTreeFileSystem: s3://ursa-labs-r-test/"
  )
  expect_identical(bucket$base_path, "ursa-labs-r-test/")
})

test_that("gs_bucket", {
  skip_on_cran()
  skip_if_not_available("gcs")
  skip_if_offline()
  bucket <- gs_bucket("voltrondata-labs-datasets")
  expect_r6_class(bucket, "SubTreeFileSystem")
  expect_r6_class(bucket$base_fs, "GcsFileSystem")
  expect_identical(
    capture.output(print(bucket)),
    "SubTreeFileSystem: gs://voltrondata-labs-datasets/"
  )
  expect_identical(bucket$base_path, "voltrondata-labs-datasets/")
})
