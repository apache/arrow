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
 * Assigns the GitHub Issue to the PR creator.
 *
 * @param {Object} github
 * @param {Object} context
 * @param {String} pullRequestNumber
 * @param {Object} issueInfo
 */
async function assignGitHubIssue(github, context, pullRequestNumber, issueInfo) {
    await github.rest.issues.addAssignees({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: issueInfo.number,
        assignees: context.payload.pull_request.user.login
    });
    await github.rest.issues.createComment({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: pullRequestNumber,
        body: ":warning: GitHub issue #" + issueInfo.number + " **has been automatically assigned in GitHub** to PR creator."
    });
}

/**
 * Performs checks on the GitHub Issue:
 * - The issue is assigned to someone. If not assign it gets automatically
 *   assigned to the PR creator.
 * - The issue contains any label.
 *
 * @param {Object} github
 * @param {Object} context
 * @param {String} pullRequestNumber
 * @param {String} issueID
 */
async function verifyGitHubIssue(github, context, pullRequestNumber, issueID) {
    const issueInfo = await helpers.getGitHubInfo(github, context, issueID, pullRequestNumber);
    if (!issueInfo) {
        await github.rest.issues.createComment({
            owner: context.repo.owner,
            repo: context.repo.repo,
            issue_number: pullRequestNumber,
            body: ":x: GitHub issue #" + issueID + " could not be retrieved."
        })
    }
    if (!issueInfo.assignees.length) {
        await assignGitHubIssue(github, context, pullRequestNumber, issueInfo);
    }
    if(!issueInfo.labels.filter((label) => label.name.startsWith("Component:")).length) {
        await github.rest.issues.createComment({
            owner: context.repo.owner,
            repo: context.repo.repo,
            issue_number: pullRequestNumber,
            body: ":warning: GitHub issue #" + issueID + " **has no components**, please add labels for components."
        })
    }
}

module.exports = async ({github, context}) => {
    const pullRequestNumber = context.payload.number;
    const title = context.payload.pull_request.title;
    const issue = helpers.detectIssue(title)
    if (issue && issue.kind === "github") {
        await verifyGitHubIssue(github, context, pullRequestNumber, issue.id);
    }
};
