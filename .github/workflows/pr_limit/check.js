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

const WRITE_PERMISSIONS = new Set(["write", "maintain", "admin"]);

/**
 * Returns whether the user has write access to the repository.
 *
 * Note that `author_association` is not a reliable signal for this:
 * ASF members show up as MEMBER regardless of their permission on this
 * repository, and triage collaborators show up as COLLABORATOR.
 *
 * @param {Object} github
 * @param {Object} context
 * @param {String} username
 */
async function hasWriteAccess(github, context, username) {
  try {
    const {data} = await github.rest.repos.getCollaboratorPermissionLevel({
      owner: context.repo.owner,
      repo: context.repo.repo,
      username: username
    });
    return WRITE_PERMISSIONS.has(data.permission);
  } catch (error) {
    if (error.status === 404) {
      return false;
    }
    throw error;
  }
}

/**
 * Returns the number of open pull requests authored by the user in this
 * repository, including the one that triggered the workflow.
 *
 * @param {Object} github
 * @param {Object} context
 * @param {String} username
 */
async function countOpenPullRequests(github, context, username) {
  const {data} = await github.rest.search.issuesAndPullRequests({
    q: `repo:${context.repo.owner}/${context.repo.repo} is:pr is:open author:${username}`,
    per_page: 1
  });
  return data.total_count;
}

/**
 * Comments on the pull request explaining the limit, then closes it.
 *
 * @param {Object} github
 * @param {Object} context
 * @param {Number} pullRequestNumber
 * @param {String} username
 * @param {Number} limit
 * @param {Number} count
 */
async function commentAndClose(github, context, pullRequestNumber, username, limit, count) {
  const commentPath = ".github/workflows/pr_limit/comment.md";
  const comment = fs.readFileSync(commentPath).toString()
    .replaceAll("${PR_LIMIT}", limit)
    .replaceAll("${OPEN_COUNT}", count)
    .replaceAll("${USERNAME}", username);
  await github.rest.issues.createComment({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: pullRequestNumber,
    body: comment
  });
  await github.rest.pulls.update({
    owner: context.repo.owner,
    repo: context.repo.repo,
    pull_number: pullRequestNumber,
    state: "closed"
  });
}

module.exports = async ({github, context, core}) => {
  const limit = parseInt(process.env.PR_LIMIT, 10);
  if (!Number.isInteger(limit) || limit < 1) {
    throw new Error(`PR_LIMIT must be a positive integer, got: ${process.env.PR_LIMIT}`);
  }

  const pullRequestNumber = context.payload.number;
  const user = context.payload.pull_request.user;

  if (user.type === "Bot") {
    core.info(`Skipping: ${user.login} is a bot.`);
    return;
  }

  if (await hasWriteAccess(github, context, user.login)) {
    core.info(`Skipping: ${user.login} has write access.`);
    return;
  }

  // A committer reopening a previously closed pull request is a deliberate
  // decision to accept it, so don't close it again.
  const sender = context.payload.sender;
  if (sender.login !== user.login && await hasWriteAccess(github, context, sender.login)) {
    core.info(`Skipping: ${context.payload.action} by ${sender.login}, who has write access.`);
    return;
  }

  const count = await countOpenPullRequests(github, context, user.login);
  core.info(`${user.login} has ${count} open pull request(s); limit is ${limit}.`);
  if (count <= limit) {
    return;
  }

  core.info(`Closing #${pullRequestNumber}: over the limit.`);
  await commentAndClose(github, context, pullRequestNumber, user.login, limit, count);
};
