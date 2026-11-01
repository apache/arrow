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

const SECTIONS = [
  {
    heading: "PR code and description written by:",
    options: ["Human", "AI"]
  },
  {
    heading: "Reviewed before submission by:",
    options: ["Human", "AI", "Not reviewed"]
  }
];

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

Once you have completed the disclosure, please manually mark this pull request as **Ready for review**
This pull request has been converted to Draft until the disclosure is completed.`;

const AI_ONLY_COMMENT = `${COMMENT_MARKER}

This pull request has been automatically closed because its authorship indicates AI involvement without human review.

Contributions involving AI assistance are welcome. However, we require human involvement in the contribution and its review.

If this classification is incorrect, please reopen the pull request and correct the disclosure.`;

const RESOLVED_COMMENT = `${COMMENT_MARKER}

The AI disclosure is now complete. No further action is required.`;

const READY_WITHOUT_DISCLOSURE_COMMENT = `${COMMENT_MARKER}

This pull request was marked Ready for review, but the AI disclosure is still missing or incomplete.

Please add and complete both AI disclosure sections in the pull request description before marking it Ready for review again.`;

function parseDisclosure(body) {
  const lines = body.split(/\r?\n/);
  // Keep the sections ordered so unrelated checkboxes cannot be misclassified.
  const sections = lines.flatMap((line, lineNumber) =>
    SECTIONS
      .filter(({heading}) => line.trim() === `**${heading}**`)
      .map((section) => ({...section, line: lineNumber}))
  );

  if (
    sections.length !== SECTIONS.length ||
    sections.some((section, index) => section.heading !== SECTIONS[index].heading)
  ) {
    return {status: "incomplete"};
  }

  const selected = sections.map((section, index) => {
    const nextSection = sections[index + 1];
    let endLine = nextSection?.line ?? lines.length;

    // The final section ends at the next Markdown heading, if there is one.
    if (!nextSection) {
      const headingLine = lines.findIndex(
        (line, lineNumber) =>
          lineNumber > section.line &&
          /^\s*\*\*[^*].+?\*\*\s*$/.test(line)
      );
      if (headingLine !== -1) {
        endLine = headingLine;
      }
    }

    const selections = lines
      .slice(section.line + 1, endLine)
      .filter((line) => /^\s*-\s+\[[ xX]\]\s*.+$/.test(line))
      .map((line) => {
        const [, checked, option] = line.match(
          /^\s*-\s+\[([ xX])\]\s*(.*?)\s*$/
        );
        return {checked: checked.toLowerCase() === "x", option};
      });
    const foundOptions = selections.map(({option}) => option);

    // Every declared option must appear once, with at least one selected.
    if (
      foundOptions.length !== section.options.length ||
      section.options.some((option) => !foundOptions.includes(option)) ||
      new Set(foundOptions).size !== foundOptions.length ||
      !selections.some(({checked}) => checked)
    ) {
      return null;
    }

    return selections
      .filter(({checked}) => checked)
      .map(({option}) => option);
  });

  if (selected.some((section) => section === null)) {
    return {status: "incomplete"};
  }

  const [authorship, review] = selected;

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
  body,
  createNew = false
) {
  const existing = await findDisclosureComment(
    github,
    context,
    pullRequestNumber
  );
  const params = {
    owner: context.repo.owner,
    repo: context.repo.repo,
    body
  };
  await (existing && !createNew
    ? github.rest.issues.updateComment({...params, comment_id: existing.id})
    : github.rest.issues.createComment({
        ...params,
        issue_number: pullRequestNumber
      }));
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
    if (isReadyForReview) {
      await github.rest.issues.createComment({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: pullRequestNumber,
        body: READY_WITHOUT_DISCLOSURE_COMMENT
      });
    } else {
      await updateDisclosureComment(
        github,
        context,
        pullRequestNumber,
        INCOMPLETE_COMMENT
      );
    }

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

  // Update an existing bot comment, but don't create one for valid disclosures.
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
