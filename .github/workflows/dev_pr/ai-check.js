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

const AUTHORSHIP_HEADING = "PR code and description written by:";
const REVIEW_HEADING = "Reviewed before submission by:";

const INCOMPLETE_COMMENT =
  "This pull request does not follow the Apache Arrow PR template and is therefore being closed.\n\n" +
  "If you wish to contribute, please open a new pull request using the [Apache Arrow PR template](https://github.com/apache/arrow/blob/main/.github/pull_request_template.md) " +
  "and complete all required sections";

const AI_ONLY_COMMENT =
  "This pull request has been automatically closed because its authorship is AI-only and it has no human review.\n\n" +
  "Contributions involving AI assistance are welcome. However, we require human involvement in the contribution and its review.";

const NEEDS_HUMAN_REVIEW_COMMENT =
  "This pull request has been converted to Draft because it has not been reviewed by a human.\n\n" +
  "Please review your contribution, update the review disclosure section, " +
  "and mark the pull request as Ready for review.";

function parseSection(lines, start, end, options) {
  const found = new Set();
  const selected = [];

  for (const line of lines.slice(start + 1, end)) {
    const match = line.match(/^\s*-\s+\[([ xX])\]\s*(.*?)\s*$/);

    if (!match) {
      // Reject malformed checkbox entries.
      if (/^\s*-\s+\[/.test(line)) return null;
      continue;
    }

    const option = match[2];

    // Each expected option must appear exactly once.
    if (!options.includes(option) || found.has(option)) return null;

    found.add(option);
    if (match[1].toLowerCase() === "x") selected.push(option);
  }

  if (
    found.size !== options.length ||
    !selected.length ||
    (selected.includes("Not reviewed") && selected.length > 1)
  ) {
    return null;
  }

  return selected;
}

function parseDisclosure(body) {
  const lines = body.split(/\r?\n/);
  const headings = lines.map(line =>
    line.trim().replace(/^\*\*(.*?)\*\*$/, "$1")
  );

  const authorshipStart = headings.indexOf(AUTHORSHIP_HEADING);
  const reviewStart = headings.indexOf(REVIEW_HEADING);

  // Both headings must exist exactly once and in the correct order.
  if (
    authorshipStart === -1 ||
    reviewStart <= authorshipStart ||
    headings.lastIndexOf(AUTHORSHIP_HEADING) !== authorshipStart ||
    headings.lastIndexOf(REVIEW_HEADING) !== reviewStart
  ) {
    return "incomplete";
  }

  // Find where the review section ends.
  const reviewEnd = lines.findIndex(
    (line, index) =>
      index > reviewStart &&
      (/^#{1,6}\s/.test(line.trim()) || /^\*\*.+\*\*$/.test(line.trim()))
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

  if (!authorship || !review) return "incomplete";
  if (review.includes("Human")) return "complete";

  return authorship.length === 1 && authorship[0] === "AI"
    ? "ai-only"
    : "needs-human-review";
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
 */
async function convertToDraft(github, pullRequest) {
  if (pullRequest.draft) return;

  await github.graphql(
    `mutation($id: ID!) {
      convertPullRequestToDraft(input: {pullRequestId: $id}) {
        pullRequest {
          id
        }
      }
    }`,
    {id: pullRequest.node_id}
  );
}

module.exports = async ({github, context, core}) => {
  const run = context.payload.workflow_run;
  const head = run.head_repository;

  if (
    run.event !== "pull_request" ||
    !head?.owner?.login ||
    !head.full_name ||
    !run.head_branch ||
    !run.head_sha
  ) {
    core.info("Unable to identify an associated pull request.");
    return;
  }

  // Find the PR using the contributor's fork and branch.
  const candidates = await github.paginate(
    github.rest.pulls.list,
    {
      owner: context.repo.owner,
      repo: context.repo.repo,
      state: "open",
      head: `${head.owner.login}:${run.head_branch}`,
      per_page: 100
    }
  );

  for (const candidate of candidates) {
    const {data: pr} = await github.rest.pulls.get({
      owner: context.repo.owner,
      repo: context.repo.repo,
      pull_number: candidate.number
    });

    // Ignore outdated runs and bot-authored PRs.
    if (
      pr.state !== "open" ||
      pr.head.sha !== run.head_sha ||
      pr.head.repo?.full_name.toLowerCase() !== head.full_name.toLowerCase() ||
      pr.user?.type === "Bot"
    ) {
      continue;
    }

    const status = parseDisclosure(pr.body || "");

    core.info(`Disclosure status for #${pr.number}: ${status}.`);

    // A valid disclosure requires no action.
    if (status === "complete") continue;

    const comment = {
    incomplete: INCOMPLETE_COMMENT,
    "ai-only": AI_ONLY_COMMENT,
    "needs-human-review": NEEDS_HUMAN_REVIEW_COMMENT
    }[status];

    await createDisclosureComment(
    github, context, pr.number, comment
    );

    // Close missing/incomplete disclosures and AI-only contributions
    // without human review. Other contributions must receive human review.
    if (status === "incomplete" || status === "ai-only") {
      await github.rest.pulls.update({
        owner: context.repo.owner,
        repo: context.repo.repo,
        pull_number: pr.number,
        state: "closed"
      });
    } else {
      await convertToDraft(github, pr);
    }
  }
};

module.exports.parseDisclosure = parseDisclosure;
