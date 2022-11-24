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

const helpers = require("./helpers.js");


async function haveComment(github, context, pullRequestNumber, body) {
  const options = {
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: pullRequestNumber,
    page: 1
  };
  while (true) {
    const response = await github.issues.listComments(options);
    if (response.data.some(comment => comment.body === body)) {
      return true;
    }
    if (!/;\s*rel="next"/.test(response.headers.link || "")) {
      break;
    }
    options.page++;
  }
  return false;
}

async function commentJIRAURL(github, context, pullRequestNumber, jiraID) {
  const jiraURL = `https://issues.apache.org/jira/browse/${jiraID}`;
  if (await haveComment(github, context, pullRequestNumber, jiraURL)) {
    return;
  }
  await github.issues.createComment({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: pullRequestNumber,
    body: jiraURL
  });
}

async function commentGitHubURL(github, context, pullRequestNumber, issueID) {
  // Make the call to ensure issue exists before adding comment
  const issueInfo = await helpers.getGitHubInfo(github, context, issueID, pullRequestNumber);
  // TODO: Check if comment is already there
  //if (await haveComment(github, context, pullRequestNumber, jiraURL)) {
  //  return;
  //}
  if (issueInfo){
    await github.issues.createComment({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: pullRequestNumber,
      body: "* Github Issue: #" + issueInfo.number
    });
  }
}

module.exports = async ({github, context}) => {
  const pullRequestNumber = context.payload.number;
  const title = context.payload.pull_request.title;
  const issueID = helpers.detectIssueID(title);
  if (helpers.haveJIRAID(title)) {
    await commentJIRAURL(github, context, pullRequestNumber, issueID);
  } else if (helpers.haveGitHubIssueID(title)) {
    await commentGitHubURL(github, context, pullRequestNumber, issueID);
  }
};
