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

const fs = require("fs");
const helpers = require("./helpers.js");

async function commentOpenGitHubIssue(github, context, pullRequestNumber) {
  const {data: comments} = await github.issues.listComments({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: pullRequestNumber,
    per_page: 1
  });
  if (comments.length > 0) {
    return;
  }
  const commentPath = ".github/workflows/dev_pr/title_check.md";
  const comment = fs.readFileSync(commentPath).toString();
  await github.issues.createComment({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: pullRequestNumber,
    body: comment
  });
}

module.exports = async ({github, context}) => {
  const pullRequestNumber = context.payload.number;
  const title = context.payload.pull_request.title;
  const issue = helpers.detectIssue(title)
  if (!issue) {
    await commentOpenGitHubIssue(github, context, pullRequestNumber);
  }
};
