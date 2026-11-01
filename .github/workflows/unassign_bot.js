// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Releases abandoned assignments on newcomer-friendly issues:
//   - no qualifying activity for WARN_AFTER_DAYS -> warn,
//     then unassign GRACE_DAYS later if there is still no activity.

const helpers = require("./dev_pr/helpers.js");

const WARN_AFTER_DAYS = 5;
const GRACE_DAYS = 3;
const LABELS = ["good-first-issue", "good-second-issue"];
const DAY = 24 * 60 * 60 * 1000;
const TIMER_PAUSE_LABELS = [
  "awaiting review",
  "awaiting committer review",
  "awaiting change review",
  "awaiting merge",
];

// GitHub author associations used to identify maintainers and collaborators.
const isMaintainer = association =>
  ["MEMBER", "OWNER", "COLLABORATOR"].includes(association);

// A PR awaiting review or merge is not considered inactive.
const hasTimerPauseLabel = pr =>
  pr.labels.some(label =>
    TIMER_PAUSE_LABELS.includes(label.name.toLowerCase())
  );

const same = (a, b) => a?.toLowerCase() === b?.toLowerCase();

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

  // Scheduled: warn, then release inactive assignments.
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

  // Fetch open PRs once and reuse them for all assigned issues.
  const openPrs = await paginate(github.rest.pulls.list, { state: "open" });

  let failures = 0;

  for (const issue of byNumber.values()) {
    for (const { login } of issue.assignees) {
      try {
        // Only consider linked PRs opened by this assignee.
        const linkedPrs = openPrs.filter(
          pr =>
            linkedIssue(pr) === issue.number &&
            same(pr.user.login, login)
        );

        // Pause the timer while the PR is awaiting review.
        if (linkedPrs.some(hasTimerPauseLabel)) {
          continue;
        }

        const events = await paginate(github.rest.issues.listEvents, {
          issue_number: issue.number,
        });

        // Use the most recent assignment event to avoid counting activity from
        // before the contributor was assigned.
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
          continue; // Unknown assignment time: keep the assignment.
        }

        const since = new Date(assignedAt).toISOString();
        const comments = await paginate(github.rest.issues.listComments, {
          issue_number: issue.number,
          since,
        });

        // Activity on a linked PR counts when it comes from the assignee or
        // someone with maintainer/collaborator permissions.
        const prActivity = [];
        const activityDates = (items, getDate, qualifies = () => true) =>
          items
            .filter(item => {
              const timestamp = Date.parse(getDate(item));
              return timestamp >= assignedAt && qualifies(item);
            })
            .map(item => Date.parse(getDate(item)));

        for (const pr of linkedPrs) {
          const [prComments, reviewComments, reviews, labelEvents, commits] =
            await Promise.all([
              paginate(github.rest.issues.listComments, {
                issue_number: pr.number,
                since,
              }),
              paginate(github.rest.pulls.listReviewComments, {
                pull_number: pr.number,
                since,
              }),
              paginate(github.rest.pulls.listReviews, {
                pull_number: pr.number,
              }),
              paginate(github.rest.issues.listEvents, {
                issue_number: pr.number,
              }),
              paginate(github.rest.pulls.listCommits, {
                pull_number: pr.number,
              }),
            ]);

          prActivity.push(
            ...activityDates(
              [...prComments, ...reviewComments],
              c => c.created_at,
              c =>
                same(c.user?.login, login) ||
                isMaintainer(c.author_association)
            ),

            ...activityDates(
              reviews.filter(review => review.submitted_at),
              review => review.submitted_at,
              review =>
                same(review.user?.login, login) ||
                isMaintainer(review.author_association)
            ),

            // Removing a timer-pausing label starts a fresh countdown.
            ...activityDates(
              labelEvents,
              event => event.created_at,
              event =>
                event.event === "unlabeled" &&
                TIMER_PAUSE_LABELS.includes(event.label?.name?.toLowerCase())
            ),

            ...activityDates(
              commits,
              commit => commit.commit.committer.date,
              commit => same(commit.committer?.login, login)
            )
          );
        }

        // The latest qualifying activity determines when the inactivity period starts.
        const lastActivity = Math.max(
          assignedAt,
          ...comments
            .filter(
              c =>
                Date.parse(c.created_at) >= assignedAt &&
                same(c.user?.login, login)
            )
            .map(c => Date.parse(c.created_at)),
          ...prActivity
        );

        if (Date.now() - lastActivity < WARN_AFTER_DAYS * DAY) {
          continue;
        }

        const warning = comments.find(
          c =>
            c.body.includes(warningMarker(login)) &&
            Date.parse(c.created_at) >= lastActivity
        );

        if (!warning) {
          await comment(
            issue.number,
            `${warningMarker(login)}\n@${login}, there has been no qualifying activity ` +
              `on this issue or its linked PR for ${WARN_AFTER_DAYS} days. It will be ` +
              `unassigned in ${GRACE_DAYS} days unless there is further activity.`
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
        failures++;
        console.error(`Failed on #${issue.number} (${login})`, error);
      }
    }
  }
  if (failures > 0) {
    throw new Error(`Failed to process ${failures} assignment(s).`);
  }
};
