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

const COMMENT_MARKER = "<!-- arrow-ai-disclosure-check -->";

const AUTHORSHIP_HEADING = "**PR code and description written by:**";
const REVIEW_HEADING = "**Reviewed before submission by:**";

const INCOMPLETE_COMMENT = `${COMMENT_MARKER}

The AI disclosure in this pull request is missing or incomplete.

Please complete both disclosure sections in the pull request description by selecting the applicable checkboxes. Once the disclosure is complete, you can mark the pull request as ready for review.

If the disclosure is not present, add the following to the pull request description:

**PR code and description written by:**

- [ ] Human
- [ ] AI

**Reviewed before submission by:**

- [ ] Human
- [ ] AI
- [ ] Not reviewed

Once you have completed the disclosure, please manually mark this pull request as **Ready for review**.
This pull request has been converted to Draft until the disclosure is completed.`;

const AI_ONLY_COMMENT = `${COMMENT_MARKER}

This pull request has been automatically closed because its authorship indicates AI involvement without human review.

Contributions involving AI assistance are welcome. However, we require human involvement in the contribution and its review.

If this classification is incorrect, please reopen the pull request and correct the disclosure.`;

const RESOLVED_COMMENT = `${COMMENT_MARKER}

The AI disclosure is now complete. No further action is required.`;

function parseSection(lines, start, end, options) {
  const found = new Set();
  const selected = [];

  for (const line of lines.slice(start + 1, end)) {
    const match = line.match(/^\s*-\s+\[([ xX])\]\s*(.*?)\s*$/);

    if (!match) {
      continue;
    }

    const option = match[2];
    const checked = match[1].toLowerCase() === "x";

    if (!options.includes(option) || found.has(option)) {
      return null;
    }

    found.add(option);

    if (checked) {
      selected.push(option);
    }
  }

  if (found.size !== options.length || selected.length === 0) {
    return null;
  }

  return selected;
}

function parseDisclosure(body) {
  const lines = body.split(/\r?\n/);
  const normalizedLines = lines.map((line) => line.trim());

  const authorshipStart = normalizedLines.indexOf(AUTHORSHIP_HEADING);
  const reviewStart = normalizedLines.indexOf(REVIEW_HEADING);

  if (
    authorshipStart === -1 ||
    reviewStart <= authorshipStart ||
    normalizedLines.lastIndexOf(AUTHORSHIP_HEADING) !== authorshipStart ||
    normalizedLines.lastIndexOf(REVIEW_HEADING) !== reviewStart
  ) {
    return {status: "incomplete"};
  }

  const reviewEnd = normalizedLines.findIndex(
    (line, index) =>
      index > reviewStart && /^\*\*[^*].+?\*\*$/.test(line)
  );

  const authorship = parseSection(
    lines,
    authorshipStart,
    reviewStart,
    ["Human", "AI"]
  );

  const review = parseSection(
    lines,
    reviewStart,
    reviewEnd === -1 ? lines.length : reviewEnd,
    ["Human", "AI", "Not reviewed"]
  );

  if (
    !authorship ||
    !review ||
    (review.includes("Not reviewed") && review.length > 1)
  ) {
    return {status: "incomplete"};
  }

  if (
    authorship.length === 1 &&
    authorship[0] === "AI" &&
    review.length === 1 &&
    ["AI", "Not reviewed"].includes(review[0])
  ) {
    return {status: "ai-only", authorship, review};
  }

  return {status: "complete", authorship, review};
}

async function findDisclosureComment(github, context, pullRequestNumber) {
  const comments = await github.paginate(github.rest.issues.listComments, {
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: pullRequestNumber,
    per_page: 100
  });
  return comments.find((comment) =>
    comment.user.type === "Bot" && comment.body.includes(COMMENT_MARKER)
  );
}

async function updateDisclosureComment(
  github,
  context,
  pullRequestNumber,
  body
) {
  const existing = await findDisclosureComment(
    github,
    context,
    pullRequestNumber
  );

  if (existing) {
    if (existing.body !== body) {
      await github.rest.issues.updateComment({
        owner: context.repo.owner,
        repo: context.repo.repo,
        comment_id: existing.id,
        body
      });
    }
  } else {
    await github.rest.issues.createComment({
      owner: context.repo.owner,
      repo: context.repo.repo,
      issue_number: pullRequestNumber,
      body
    });
  }
}

async function checkDisclosure({github, context, core}) {
  const pullRequest = context.payload.pull_request;
  const pullRequestNumber = pullRequest.number;
  const isReadyForReview = context.payload.action === "ready_for_review";

  if (pullRequest.state !== "open") {
    core.info(`Skipping #${pullRequestNumber}: pull request is not open.`);
    return;
  }

  if (pullRequest.draft && !isReadyForReview) {
    core.info(`Skipping #${pullRequestNumber}: pull request is a draft.`);
    return;
  }

  if (pullRequest.user.type === "Bot") {
    core.info(`Skipping #${pullRequestNumber}: author is a bot.`);
    return;
  }

  const result = parseDisclosure(pullRequest.body || "");

  core.info(`Disclosure status for #${pullRequestNumber}: ${result.status}.`);

  if (result.status === "incomplete") {
    await updateDisclosureComment(
      github,
      context,
      pullRequestNumber,
      INCOMPLETE_COMMENT
    );

    await github.graphql(
      `mutation($pullRequestId: ID!) {
        convertPullRequestToDraft(
          input: {pullRequestId: $pullRequestId}
        ) {
          pullRequest {
            isDraft
          }
        }
      }`,
      {pullRequestId: pullRequest.node_id}
    );

    return;
  }

  if (result.status === "ai-only") {
    await updateDisclosureComment(
      github,
      context,
      pullRequestNumber,
      AI_ONLY_COMMENT
    );

    await github.rest.pulls.update({
      owner: context.repo.owner,
      repo: context.repo.repo,
      pull_number: pullRequestNumber,
      state: "closed"
    });

    core.info(`Closed #${pullRequestNumber}: AI-only authorship and review.`);
    return;
  }

  const existing = await findDisclosureComment(
    github,
    context,
    pullRequestNumber
  );

  if (existing && existing.body !== RESOLVED_COMMENT) {
    await github.rest.issues.updateComment({
      owner: context.repo.owner,
      repo: context.repo.repo,
      comment_id: existing.id,
      body: RESOLVED_COMMENT
    });
  }
}

module.exports = checkDisclosure;
module.exports.parseDisclosure = parseDisclosure;
