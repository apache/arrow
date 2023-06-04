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


/**
 * Checks whether message is present on Pull Request list of comments.
 *
 * @param {Object} github
 * @param {Object} context
 * @param {String} pullRequestNumber
 * @param {String} message
 * @returns {Boolean} true if message was found.
 */
async function haveComment(github, context, pullRequestNumber, message) {
  const options = {
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: pullRequestNumber,
    page: 1
  };
  while (true) {
    const response = await github.issues.listComments(options);
    if (response.data.some(comment => comment.body === message)) {
      return true;
    }
    if (!/;\s*rel="next"/.test(response.headers.link || "")) {
      break;
    }
    options.page++;
  }
  return false;
}

/**
 * Adds a comment on the Pull Request linking the JIRA issue.
 *
 * @param {Object} github
 * @param {Object} context
 * @param {String} pullRequestNumber
 * @param {String} jiraID
 */
async function commentJIRAURL(github, context, pullRequestNumber, jiraID) {
  const issueInfo = await helpers.getJiraInfo(jiraID);
  const jiraURL = `https://issues.apache.org/jira/browse/${jiraID}`;
  if (await haveComment(github, context, pullRequestNumber, jiraURL)) {
    return;
  }
  if (issueInfo){
    await github.issues.createComment({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: pullRequestNumber,
      body: jiraURL
    });
  }
}

/**
 * Adds a comment on the Pull Request linking the GitHub issue.
 *
 * @param {Object} github
 * @param {Object} context
 * @param {String} pullRequestNumber - String containing numeric id of PR
 * @param {String} issueID - String containing numeric id of the github issue
 */
async function commentGitHubURL(github, context, pullRequestNumber, issueID) {
  // Make the call to ensure issue exists before adding comment
  const issueInfo = await helpers.getGitHubInfo(github, context, issueID, pullRequestNumber);
  const message = "* Closes: #" + issueInfo.number
  if (issueInfo) {
    if (context.payload.pull_request.body.includes(message)) {
      return;
    }
    await github.pulls.update({
      owner: context.repo.owner,
      repo: context.repo.repo,
      pull_number: pullRequestNumber,
      body: (context.payload.pull_request.body || "") + "\n" + message
    });
  }
}

module.exports = async ({github, context}) => {
  const pullRequestNumber = context.payload.number;
  const title = context.payload.pull_request.title;
  const issue = helpers.detectIssue(title);
  if (issue){
    if (issue.kind == "jira") {
      await commentJIRAURL(github, context, pullRequestNumber, issue.id);
    } else if (issue.kind == "github") {
      await commentGitHubURL(github, context, pullRequestNumber, issue.id);
    }
  }
};
