
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

// Releases abandoned assignments on newcomer-friendly issues:
//   - a linked PR is closed without merging -> unassign immediately
//   - no linked PR and no assignee comment for WARN_AFTER_DAYS -> warn,
//     then unassign GRACE_DAYS later if there is still no activity.

const helpers = require("./dev_pr/helpers.js");

const WARN_AFTER_DAYS = 5 / (24 * 60); // 5 minutes
const GRACE_DAYS = 1 / (24 * 60);      // 1 minute
const LABELS = ["good-first-issue", "good-second-issue"];
const DAY = 24 * 60 * 60 * 1000;

const same = (a, b) => a?.toLowerCase() === b?.toLowerCase();
const hasNewcomerLabel = issue =>
  issue.labels.some(label => LABELS.includes(label.name));
const warningMarker = login =>
  `<!-- unassign-bot-warning:${login.toLowerCase()} -->`;

// Issue number from Arrow's "GH-1234: ..." PR title convention.
function linkedIssue(pullRequest) {
  const ref = helpers.detectIssue(pullRequest.title);
  return ref?.kind === "github" ? Number(ref.id) : null;
}

module.exports = async ({ github, context }) => {
  const { owner, repo } = context.repo;
  const paginate = (method, params) =>
    github.paginate(method, { owner, repo, per_page: 100, ...params });

  async function comment(number, body) {
    await github.rest.issues.createComment({
      owner,
      repo,
      issue_number: number,
      body,
    });
  }

  async function unassign(number, login, why) {
    console.log(`Unassigning ${login} from #${number}: ${why}`);

    await github.rest.issues.removeAssignees({
      owner,
      repo,
      issue_number: number,
      assignees: [login],
    });

    await comment(
      number,
      `This issue was automatically unassigned from @${login} because ${why}.\n\n` +
        `If you are still working on this issue, please leave a comment here to let us know. ` +
        `A maintainer or collaborator can reassign it to you.\n\n` +
        `Other contributors who are interested in working on this issue are welcome to pick it up.`
    );
  }

  // A linked PR was closed without merging.
  if (context.eventName === "pull_request_target") {
    const pr = context.payload.pull_request;
    const number = linkedIssue(pr);

    if (context.payload.action !== "closed" || pr.merged || !number) {
      return;
    }

    const issue = await github.rest.issues
      .get({ owner, repo, issue_number: number })
      .then(r => r.data, () => null);  // the title may name a missing issue

    if (!issue) {
      console.log(`Skipping PR #${pr.number}: referenced issue #${number} was not found`);
      return;
    }

    const author = pr.user.login;

    if (
      !hasNewcomerLabel(issue) ||
      !issue.assignees.some(a => same(a.login, author))
    ) {
      return;
    }

    // Another PR protects its own author, not this contributor.
    const open = await paginate(github.rest.pulls.list, { state: "open" });

    if (
      open.some(
        p =>
          p.number !== pr.number &&
          linkedIssue(p) === number &&
          same(p.user.login, author)
      )
    ) {
      return;
    }

    await unassign(
      number,
      author,
      `PR #${pr.number} was closed without being merged`
    );
    return;
  }

  // Scheduled / manual: warn, then release assignments with no author PR.
  const byNumber = new Map();

  for (const label of LABELS) {
    const issues = await paginate(github.rest.issues.listForRepo, {
      state: "open",
      labels: label,
    });

    for (const issue of issues) {
      if (!issue.pull_request && issue.assignees.length) {
        byNumber.set(issue.number, issue);
      }
    }
  }

  const openPrs = await paginate(github.rest.pulls.list, { state: "open" });

  for (const issue of byNumber.values()) {
    for (const { login } of issue.assignees) {
      try {
        // An open linked PR protects only its own author.
        const hasOpenPr = openPrs.some(
          pr =>
            linkedIssue(pr) === issue.number &&
            same(pr.user.login, login)
        );

        if (hasOpenPr) {
          continue;
        }

        const events = await paginate(github.rest.issues.listEvents, {
          issue_number: issue.number,
        });

        const assignedAt = Math.max(
          0,
          ...events
            .filter(
              event =>
                event.event === "assigned" &&
                same(event.assignee?.login, login)
            )
            .map(event => Date.parse(event.created_at))
        );

        if (!assignedAt) {
          continue;  // unknown assignment time: keep the assignment
        }

        const comments = await paginate(github.rest.issues.listComments, {
          issue_number: issue.number,
          since: new Date(assignedAt).toISOString(),
        });

        const lastActivity = Math.max(
          assignedAt,
          ...comments
            .filter(comment => same(comment.user?.login, login))
            .map(comment => Date.parse(comment.created_at))
        );

        if (Date.now() - lastActivity < WARN_AFTER_DAYS * DAY) {
          continue;
        }

        const warning = comments.find(
          comment =>
            comment.body.includes(warningMarker(login)) &&
            Date.parse(comment.created_at) >= lastActivity
        );

        if (!warning) {
          console.log(`Warning ${login} on #${issue.number}`);

          await comment(
            issue.number,
            `${warningMarker(login)}\n@${login}, there has been no linked pull request ` +
              `or comment from you on this issue for ${WARN_AFTER_DAYS} days. It will be ` +
              `unassigned in ${GRACE_DAYS} days unless you comment here or open a PR.`
          );
        } else if (
          Date.now() - Date.parse(warning.created_at) >= GRACE_DAYS * DAY
        ) {
          await unassign(
            issue.number,
            login,
            `there was no linked PR or activity for ${WARN_AFTER_DAYS + GRACE_DAYS} days`
          );
        }
      } catch (error) {
        console.log(`Failed on #${issue.number} (${login}): ${error.message}`);
      }
    }
  }
};
