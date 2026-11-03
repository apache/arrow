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

const AUTHORSHIP_HEADING = "PR code and description written by:";
const REVIEW_HEADING = "Reviewed before submission by:";

function parseSection(lines, start, end, options) {
  const found = new Set();
  const selected = [];

  for (const line of lines.slice(start + 1, end)) {
    const match = line.match(/^\s*-\s+\[([ xX])\]\s*(.*?)\s*$/);

    if (!match) {
      if (/^\s*-\s+\[/.test(line)) return null;
      continue;
    }

    const option = match[2];

    if (!options.includes(option) || found.has(option)) return null;

    found.add(option);

    if (match[1].toLowerCase() === "x") {
      selected.push(option);
    }
  }

  if (
    found.size !== options.length ||
    selected.length === 0 ||
    (selected.includes("Not reviewed") && selected.length > 1)
  ) {
    return null;
  }

  return selected;
}

function normalizeHeading(line) {
  return line.trim().replace(/^\*\*(.*?)\*\*$/, "$1");
}

function isHeading(line) {
  const trimmed = line.trim();
  return /^#{1,6}\s/.test(trimmed) || /^\*\*.+\*\*$/.test(trimmed);
}

function parseDisclosure(body) {
  const lines = body.split(/\r?\n/);
  const headings = lines.map(normalizeHeading);
  const authorshipStart = headings.indexOf(AUTHORSHIP_HEADING);
  const reviewStart = headings.indexOf(REVIEW_HEADING);

  if (
    authorshipStart === -1 ||
    reviewStart <= authorshipStart ||
    headings.lastIndexOf(AUTHORSHIP_HEADING) !== authorshipStart ||
    headings.lastIndexOf(REVIEW_HEADING) !== reviewStart
  ) {
    return {status: "incomplete"};
  }

  const reviewEnd = lines.findIndex(
    (line, index) => index > reviewStart && isHeading(line)
  );

  const authorship = parseSection(
    lines, authorshipStart, reviewStart, ["Human", "AI"]
  );

  const review = parseSection(
    lines,
    reviewStart,
    reviewEnd === -1 ? lines.length : reviewEnd,
    ["Human", "AI", "Not reviewed"]
  );

  if (!authorship || !review) {
    return {status: "incomplete"};
  }

  if (review.includes("Human")) {
    return {status: "complete"};
  }

  if (authorship.length === 1 && authorship[0] === "AI") {
    return {status: "ai-unreviewed"};
  }

  return {status: "needs-human-review"};
}

/**
 * Adds a comment to the pull request.
 */
async function createDisclosureComment(
  github, context, pullRequestNumber, body
) {
  await github.rest.issues.createComment({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: pullRequestNumber,
    body
  });
}

/**
 * Converts the pull request to Draft unless it is already a draft.
 *
 * @param {Object} github
 * @param {Object} context
 */
async function convertToDraft(github, context) {
  if (context.payload.pull_request.draft) return;

  await github.graphql(
    `
      mutation ($pullRequestId: ID!) {
        convertPullRequestToDraft(
          input: {pullRequestId: $pullRequestId}
        ) {
          pullRequest {
            id
          }
        }
      }
    `,
    {pullRequestId: context.payload.pull_request.node_id}
  );
}

module.exports = async ({github, context, core}) => {
  const pullRequest = context.payload.pull_request;
  const pullRequestNumber = pullRequest.number;

  if (
    pullRequest.state !== "open" ||
    (pullRequest.draft && context.payload.action !== "ready_for_review") ||
    pullRequest.user?.type === "Bot"
  ) {
    core.info(`Skipping #${pullRequestNumber}: not an eligible pull request.`);
    return;
  }

  const result = parseDisclosure(pullRequest.body || "");

  core.info(`Disclosure status for #${pullRequestNumber}: ${result.status}.`);

  if (result.status === "complete") {
    return;
  }

  if (result.status === "ai-unreviewed") {
    await createDisclosureComment(
      github,
      context,
      pullRequestNumber,
      `This pull request has been automatically closed because its authorship is AI-only and it has no human review.
        Contributions involving AI assistance are welcome. However, we require human involvement in the contribution and its review.`
    );

    await github.rest.pulls.update({
      owner: context.repo.owner,
      repo: context.repo.repo,
      pull_number: pullRequestNumber,
      state: "closed"
    });

    core.info(`Closed #${pullRequestNumber}: AI-only authorship without human review.`);
    return;
  }

  await createDisclosureComment(
    github,
    context,
    pullRequestNumber,
    result.status === "incomplete"
      ? fs.readFileSync(`${__dirname}/incomplete.md`, "utf8")
      : `This pull request has been converted to Draft because it has not been reviewed by a human. Please review your contribution 
          yourself before marking this pull request as ready for review. 
          \nOnce you have completed your review, update the review disclosure section and mark this pull request as Ready for review.`
  );

  await convertToDraft(github, context);
};
