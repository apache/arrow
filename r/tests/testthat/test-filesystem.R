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

context("File system")

test_that("LocalFilesystem", {
  fs <- LocalFileSystem$create()
  DESCRIPTION <- system.file("DESCRIPTION", package = "arrow")
  stat <- fs$GetTargetStats(DESCRIPTION)[[1]]
  expect_equal(stat$base_name(), "DESCRIPTION")
  expect_equal(stat$extension(), "")
  expect_equal(stat$type, FileType$File)
  expect_equal(stat$path, DESCRIPTION)
  info <- file.info(DESCRIPTION)

  expect_equal(stat$size, info$size)
  # This fails due to a subsecond difference on Appveyor on Windows with R 3.3 only
  # So add a greater tolerance to allow for that
  expect_equal(stat$mtime, info$mtime, tolerance = 1)

  tf <- tempfile(fileext = ".txt")
  fs$CopyFile(DESCRIPTION, tf)
  stat <- fs$GetTargetStats(tf)[[1]]
  expect_equal(stat$extension(), "txt")
  expect_equal(stat$size, info$size)
  expect_equal(readLines(DESCRIPTION), readLines(tf))

  tf2 <- tempfile(fileext = ".txt")
  fs$Move(tf, tf2)
  stats <- fs$GetTargetStats(c(tf, tf2, dirname(tf)))
  expect_equal(stats[[1]]$type, FileType$NonExistent)
  expect_equal(stats[[2]]$type, FileType$File)
  expect_equal(stats[[3]]$type, FileType$Directory)

  fs$DeleteFile(tf2)
  expect_equal(fs$GetTargetStats(tf2)[[1L]]$type, FileType$NonExistent)
  expect_true(!file.exists(tf2))

  expect_equal(fs$GetTargetStats(tf)[[1L]]$type, FileType$NonExistent)
  expect_true(!file.exists(tf))

  td <- tempfile()
  fs$CreateDir(td)
  expect_equal(fs$GetTargetStats(td)[[1L]]$type, FileType$Directory)
  fs$CopyFile(DESCRIPTION, file.path(td, "DESCRIPTION"))
  fs$DeleteDirContents(td)
  expect_equal(length(dir(td)), 0L)
  fs$DeleteDir(td)
  expect_equal(fs$GetTargetStats(td)[[1L]]$type, FileType$NonExistent)

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

  local_fs <- LocalFileSystem$create()
  st_fs <- SubTreeFileSystem$create(td, local_fs)
  expect_is(st_fs, "SubTreeFileSystem")
  expect_is(st_fs, "FileSystem")
  st_fs$CreateDir("test")
  st_fs$CopyFile("DESCRIPTION", "DESC.txt")
  stats <- st_fs$GetTargetStats(c("DESCRIPTION", "test", "nope", "DESC.txt"))
  expect_equal(stats[[1L]]$type, FileType$File)
  expect_equal(stats[[2L]]$type, FileType$Directory)
  expect_equal(stats[[3L]]$type, FileType$NonExistent)
  expect_equal(stats[[4L]]$type, FileType$File)
  expect_equal(stats[[4L]]$extension(), "txt")

  local_fs$DeleteDirContents(td)
  stats <- st_fs$GetTargetStats(c("DESCRIPTION", "test", "nope", "DESC.txt"))
  expect_equal(stats[[1L]]$type, FileType$NonExistent)
  expect_equal(stats[[2L]]$type, FileType$NonExistent)
  expect_equal(stats[[3L]]$type, FileType$NonExistent)
  expect_equal(stats[[4L]]$type, FileType$NonExistent)
})

test_that("LocalFileSystem + Selector", {
  fs <- LocalFileSystem$create()
  dir.create(td <- tempfile())
  writeLines("blah blah", file.path(td, "one.txt"))
  writeLines("yada yada", file.path(td, "two.txt"))
  dir.create(file.path(td, "dir"))
  writeLines("...", file.path(td, "dir", "three.txt"))

  selector <- Selector$create(td, recursive = TRUE)
  stats <- fs$GetTargetStats(selector)
  expect_equal(length(stats), 4L)
  types <- sapply(stats, function(.x) .x$type)
  expect_equal(sum(types == FileType$File), 3L)
  expect_equal(sum(types == FileType$Directory), 1L)

  selector <- Selector$create(td, recursive = FALSE)
  stats <- fs$GetTargetStats(selector)
  expect_equal(length(stats), 3L)
  types <- sapply(stats, function(.x) .x$type)
  expect_equal(sum(types == FileType$File), 2L)
  expect_equal(sum(types == FileType$Directory), 1L)
})
