// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Smoke-test and run the testthat suite for the arrow R package under webR.
//
// This script is called by r_wasm_test.sh after it sets up the CRAN-like
// repo and installs the webr npm package.
//
// Environment variables:
//   ARROW_WASM_REPO_DIR  - path to the local CRAN-like repo containing
//                          the arrow wasm binary package
//   ARROW_R_TESTS_DIR   - path to the arrow R package tests/testthat directory

const { WebR } = require("webr");
const http = require("http");
const fs = require("fs");
const path = require("path");

const repoDir = process.env.ARROW_WASM_REPO_DIR;
if (!repoDir) {
  console.error("ERROR: ARROW_WASM_REPO_DIR not set");
  process.exit(1);
}

const testsDir = process.env.ARROW_R_TESTS_DIR;
if (!testsDir) {
  console.error("ERROR: ARROW_R_TESTS_DIR not set");
  process.exit(1);
}

// Recursively list all files under a directory
function listFilesRecursive(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...listFilesRecursive(full));
    } else {
      results.push(full);
    }
  }
  return results;
}

async function main() {
  // Serve the local repo over HTTP so webR (Emscripten) can access it.
  // webR's R runs in an Emscripten sandbox and cannot access the host
  // filesystem directly — it fetches packages over HTTP instead.
  const server = http.createServer((req, res) => {
    const filePath = path.join(repoDir, decodeURIComponent(req.url));
    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(404);
        res.end();
      } else {
        res.writeHead(200);
        res.end(data);
      }
    });
  });
  server.listen(8080);
  console.log("✓ Repo server on :8080");

  const webR = new WebR({
    RArgs: ["--quiet"],
    interactive: false,
  });

  await webR.init();
  console.log("✓ webR initialized");

  // Upload test files from the host into webR's virtual filesystem.
  // rwasm builds don't include tests (no --install-tests flag), so we
  // load them separately and use test_dir() instead of test_package().
  const vfsTestDir = "/tmp/arrow-tests";
  await webR.FS.mkdir(vfsTestDir);
  const testFiles = listFilesRecursive(testsDir);
  for (const filePath of testFiles) {
    const rel = path.relative(testsDir, filePath);
    const vfsPath = path.posix.join(vfsTestDir, rel.split(path.sep).join("/"));
    // Ensure parent directories exist
    const vfsDir = path.posix.dirname(vfsPath);
    if (vfsDir !== vfsTestDir) {
      await webR.evalRVoid(`dir.create("${vfsDir}", recursive = TRUE, showWarnings = FALSE)`);
    }
    const contents = fs.readFileSync(filePath);
    await webR.FS.writeFile(vfsPath, contents);
  }
  console.log(`✓ Uploaded ${testFiles.length} test files to webR VFS`);

  // Install the arrow Wasm package, put localhost:8080 before repo.r-wasm.org
  // (which is used for deps)
  await webR.installPackages(["arrow"], {
    repos: ["http://localhost:8080", "https://repo.r-wasm.org"],
    quiet: false,
    mount: false,
  });
  console.log("✓ arrow installed");

  // Install test deps from DESCRIPTION Suggests + packages used in helpers.
  // All of these are available on repo.r-wasm.org for wasm.
  await webR.installPackages(
    [
      "testthat",
      "tibble",
      "dplyr",
      "withr",
      "pillar",
      "lubridate",
      "purrr",
      "stringr",
      "stringi",
      "bit64",
      "hms",
      "rlang",
      "vctrs",
      "tzdb",
    ],
    {
      repos: ["https://repo.r-wasm.org"],
      quiet: false,
      mount: false,
    },
  );
  console.log("✓ test dependencies installed");

  // Test the package loads and functions basically
  const loadResult = await webR.evalRString(`
    library(arrow)
    cat("arrow loaded\\n")
    cat("R.version$os =", R.version$os, "\\n")
    use_threads <- getOption("arrow.use_threads")
    cat("arrow.use_threads =", use_threads, "\\n")
    stopifnot(identical(use_threads, FALSE))
    tab <- arrow::as_arrow_table(data.frame(x = 1:10, y = letters[1:10]))
    stopifnot(nrow(tab) == 10L)
    cat("Created Arrow table with", nrow(tab), "rows\\n")
    "PASS"
  `);

  if (loadResult !== "PASS") {
    console.error("Package load test FAILED");
    await webR.close();
    server.close();
    process.exit(1);
  }
  console.log("✓ Package loads and works correctly");

  // Run tests using test_dir() since the wasm binary doesn't include tests
  console.log("Running testthat suite under webR...");

  const testResult = await webR.evalRString(`
    library(testthat)
    library(arrow)
    results <- testthat::test_dir("${vfsTestDir}", reporter = "summary", stop_on_failure = FALSE, package = "arrow")
    df <- as.data.frame(results)
    n_pass <- sum(df$passed)
    n_skip <- sum(df$skipped)
    n_fail <- sum(df$failed)
    n_error <- sum(df$error)
    cat(sprintf("Results: %d passed, %d skipped, %d failed, %d errors\\n",
                n_pass, n_skip, n_fail, n_error))
    if (n_fail > 0 || n_error > 0) "FAIL" else "PASS"
  `);

  if (testResult !== "PASS") {
    console.error("testthat suite FAILED");
    await webR.close();
    server.close();
    process.exit(1);
  }
  console.log("✓ testthat suite passed");

  console.log("✓ All tests passed!");
  await webR.close();
  server.close();
}

main().catch((e) => {
  console.error("FAILED:", e);
  process.exit(1);
});
