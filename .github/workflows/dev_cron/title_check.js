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

console.log("title-check");

const fs = require("fs");

const {owner: owner, repo: repo} = context.repo;

function haveJIRAID(title) {
  if (!title) {
    return false;
  }
  return /^(ARROW|PARQUET)-\d+/.test(title);
}

async function commentOpenJIRAIssue(pullRequestNumber) {
  const {data: comments} = await github.issues.listComments({
    owner: owner,
    repo: repo,
    issue_number: pullRequestNumber,
    per_page: 1
  });
  if (comments.length > 0) {
    return;
  }
  const commentPath = ".github/workflows/dev_cron/title_check.md";
  const comment = fs.readFileSync(commentPath).toString();
  await github.issues.createComment({
    owner: owner,
    repo: repo,
    issue_number: pullRequestNumber,
    body: comment
  });
}

(async () => {
  const {data: pulls} = await github.pulls.list({
    owner: owner,
    repo: repo,
  });
  pulls.forEach(async (pull) => {
    const pullRequestNumber = pull.number;
    const title = pull.title;
    if (!haveJIRAID(title)) {
      await commentOpenJIRAIssue(pullRequestNumber);
    }
  });
})();
