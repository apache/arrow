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

// Smoke-test the arrow R package under webR, then run the testthat suite.
// Called by r_wasm_test.sh. Requires env vars:
//   ARROW_WASM_REPO_DIR - local CRAN-like repo with the arrow .tgz
//   ARROW_R_TESTS_DIR   - path to tests/testthat in the source tree

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
  // Serve the repo over HTTP (webR can't access the host filesystem directly)
  const server = http.createServer((req, res) => {
    const filePath = path.join(repoDir, decodeURIComponent(req.url));
    if (!filePath.startsWith(path.resolve(repoDir))) {
      res.writeHead(403);
      res.end();
      return;
    }
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

  const webR = new WebR({ RArgs: ["--quiet"], interactive: false });
  await webR.init();
  console.log("✓ webR initialized");

  // Upload test files to webR VFS (rwasm doesn't include tests in binaries)
  const vfsTestDir = "/tmp/arrow-tests";
  await webR.FS.mkdir(vfsTestDir);
  const testFiles = listFilesRecursive(testsDir);
  const createdDirs = new Set([vfsTestDir]);
  for (const file of testFiles) {
    const rel = path.relative(testsDir, file);
    const vfsPath = path.posix.join(vfsTestDir, rel.split(path.sep).join("/"));
    const vfsDir = path.posix.dirname(vfsPath);
    if (!createdDirs.has(vfsDir)) {
      await webR.evalRVoid(`dir.create("${vfsDir}", recursive=TRUE, showWarnings=FALSE)`);
      createdDirs.add(vfsDir);
    }
    await webR.FS.writeFile(vfsPath, fs.readFileSync(file));
  }
  console.log(`✓ Uploaded ${testFiles.length} test files to VFS`);

  // Install arrow from local repo, deps from r-wasm.org
  await webR.installPackages(["arrow"], {
    repos: ["http://localhost:8080", "https://repo.r-wasm.org"],
    quiet: false,
    mount: false,
  });
  console.log("✓ arrow installed");

  // Install test deps parsed from DESCRIPTION
  const depsList = await webR.evalRString(`
    desc <- read.dcf(system.file("DESCRIPTION", package = "arrow"),
                     fields = c("Imports", "Suggests"))
    pkgs <- unlist(strsplit(paste(na.omit(desc[1,]), collapse = ","), ",\\\\s*"))
    pkgs <- trimws(sub("\\\\s*\\\\(.*\\\\)", "", pkgs))
    pkgs <- pkgs[pkgs != "" & pkgs != "R"]
    pkgs <- pkgs[!pkgs %in% loadedNamespaces()]
    paste(pkgs, collapse = "\\n")
  `);
  const testDeps = depsList.split("\n").filter(Boolean);
  console.log(`Installing ${testDeps.length} dependencies from DESCRIPTION...`);
  await webR.installPackages(testDeps, {
    repos: ["https://repo.r-wasm.org"],
    quiet: false,
    mount: false,
  });
  console.log("✓ test dependencies installed");

  // Smoke test: package loads, threading disabled, basic operations work
  const loadResult = await webR.evalRString(`
    library(arrow)
    cat("R.version$os =", R.version$os, "\\n")
    stopifnot(identical(getOption("arrow.use_threads"), FALSE))
    tab <- arrow::as_arrow_table(data.frame(x = 1:10, y = letters[1:10]))
    stopifnot(nrow(tab) == 10L)
    cat("Created table with", nrow(tab), "rows\\n")
    "PASS"
  `);
  if (loadResult !== "PASS") {
    throw new Error("Smoke test failed");
  }
  console.log("✓ Smoke test passed");

  // Run testthat suite
  console.log("Running testthat suite...");
  const testResult = await webR.evalRString(`
    library(testthat)
    results <- testthat::test_dir(
      "${vfsTestDir}",
      reporter = "summary",
      stop_on_failure = FALSE,
      package = "arrow"
    )
    df <- as.data.frame(results)
    cat(sprintf("Results: %d passed, %d skipped, %d failed, %d errors\\n",
                sum(df$passed), sum(df$skipped), sum(df$failed), sum(df$error)))
    if (sum(df$failed) > 0 || sum(df$error) > 0) "FAIL" else "PASS"
  `);
  if (testResult !== "PASS") {
    throw new Error("testthat suite failed");
  }
  console.log("✓ testthat suite passed");

  await webR.close();
  server.close();
}

main().catch((e) => {
  console.error("FAILED:", e);
  process.exit(1);
});
