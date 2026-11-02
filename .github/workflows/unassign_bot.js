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

const helpers = require("./dev_pr/helpers.js");

const LABELS = ["good-first-issue", "good-second-issue"];
const PAUSE_LABELS = [
  "awaiting review",
  "awaiting committer review",
  "awaiting change review",
  "awaiting merge",
];
const DAY = 24 * 60 * 60 * 1000;
const WARN_AFTER = 5 * DAY;
const GRACE_PERIOD = 3 * DAY;

const sameUser = (a, b) =>
  Boolean(a && b && a.toLowerCase() === b.toLowerCase());
const isMaintainer = association =>
  ["MEMBER", "OWNER", "COLLABORATOR"].includes(association);
const warningMarker = login =>
  `<!-- unassign-bot-warning:${login.toLowerCase()} -->`;

module.exports = async ({ github, context }) => {
  const { owner, repo } = context.repo;
  const paginate = (method, params) =>
    github.paginate(method, { owner, repo, per_page: 100, ...params });
  const comment = (issue_number, body) =>
    github.rest.issues.createComment({ owner, repo, issue_number, body });

  const unassign = async (issue_number, login) => {
    console.log(`Unassigning ${login} from #${issue_number}`);
    await github.rest.issues.removeAssignees({
      owner,
      repo,
      issue_number,
      assignees: [login],
    });
    await comment(
      issue_number,
      `This issue was automatically unassigned from @${login} due to inactivity. ` +
        `If you are still working on it, please comment here so a maintainer can reassign it.`
    );
  };

  const openPrs = await paginate(github.rest.pulls.list, { state: "open" });
  const processed = new Set();
  let failures = 0;

  // Process each issue only once, even when it has both newcomer labels.
  for (const label of LABELS) {
    const issues = await paginate(github.rest.issues.listForRepo, {
      state: "open",
      labels: label,
    });

    for (const issue of issues) {
      if (issue.pull_request || !issue.assignees.length || processed.has(issue.number)) {
        continue;
      }
      processed.add(issue.number);

      for (const { login } of issue.assignees) {
        try {
          const linkedPrs = openPrs.filter(pr => {
            const ref = helpers.detectIssue(pr.title);
            return (
              ref?.kind === "github" &&
              Number(ref.id) === issue.number &&
              sameUser(pr.user.login, login)
            );
          });

          // A linked PR awaiting review or merge pauses the inactivity timer.
          if (
            linkedPrs.some(pr =>
              pr.labels.some(label => PAUSE_LABELS.includes(label.name.toLowerCase()))
            )
          ) {
            continue;
          }

          // Timeline events include assignment, comments, reviews and label changes.
          const events = await paginate(github.rest.issues.listEventsForTimeline, {
            issue_number: issue.number,
          });
          const assignedAt = Math.max(
            0,
            ...events
              .filter(
                event =>
                  event.event === "assigned" &&
                  sameUser(event.assignee?.login, login)
              )
              .map(event => Date.parse(event.created_at))
          );
          if (!assignedAt) continue;

          // Start at the assignment time so activity before assignment is ignored.
          let lastActivity = assignedAt;
          for (const event of events) {
            if (event.event === "commented" && sameUser(event.user?.login, login)) {
              lastActivity = Math.max(lastActivity, Date.parse(event.created_at));
            }
          }

          for (const pr of linkedPrs) {
            const [prEvents, reviewComments, commits] = await Promise.all([
              paginate(github.rest.issues.listEventsForTimeline, {
                issue_number: pr.number,
              }),
              paginate(github.rest.pulls.listReviewComments, {
                pull_number: pr.number,
              }),
              paginate(github.rest.pulls.listCommits, {
                pull_number: pr.number,
              }),
            ]);

            for (const event of prEvents) {
              const isCommentOrReview =
                ["commented", "reviewed"].includes(event.event) &&
                (sameUser(event.user?.login, login) ||
                  isMaintainer(event.author_association));

              const isPauseLabelRemoved =
                event.event === "unlabeled" &&
                PAUSE_LABELS.includes(event.label?.name?.toLowerCase());

              if (isCommentOrReview || isPauseLabelRemoved) {
                lastActivity = Math.max(
                  lastActivity,
                  Date.parse(event.submitted_at || event.created_at)
                );
              }
            }

            // Inline review comments are not included in the PR timeline.
            for (const comment of reviewComments) {
              if (
                sameUser(comment.user?.login, login) ||
                isMaintainer(comment.author_association)
              ) {
                lastActivity = Math.max(
                  lastActivity,
                  Date.parse(comment.created_at)
                );
              }
            }

            // Match commits using the associated GitHub author account.
            for (const commit of commits) {
              const committedAt = Date.parse(commit.commit.author.date);

              if (
                sameUser(commit.author?.login, login) &&
                Number.isFinite(committedAt)
              ) {
                lastActivity = Math.max(lastActivity, committedAt);
              }
            }
          }

          if (Date.now() - lastActivity < WARN_AFTER) continue;

          const marker = warningMarker(login);
          const warning = events.find(
            event =>
              event.event === "commented" &&
              event.body?.includes(marker) &&
              Date.parse(event.created_at) >= lastActivity
          );
          if (!warning) {
            await comment(
              issue.number,
              `${marker}\n@${login}, there has been no qualifying activity for 5 days. ` +
                `This issue will be unassigned in 3 days unless there is further activity.`
            );
          } else if (Date.now() - Date.parse(warning.created_at) >= GRACE_PERIOD) {
            await unassign(issue.number, login);
          }
        } catch (error) {
          failures++;
          console.error(`Failed to process #${issue.number} for ${login}`, error);
        }
      }
    }
  }

  if (failures) {
    throw new Error(`Failed to process ${failures} assignment(s).`);
  }
};
