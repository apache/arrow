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

async function verifyJIRAIssue(github, context, pullRequestNumber, jiraID) {
    const ticketInfo = await helpers.getJiraInfo(jiraID);
    if(!ticketInfo["fields"]["components"].length) {
        await commentMissingComponents(github, context, pullRequestNumber);
    }

    if(ticketInfo["fields"]["status"]["id"] == 1) {
        // "status": {"name":"Open","id":"1"
        //            "description":"The issue is open and ready for the assignee to start work on it.", 
        await commentNotStartedTicket(github, context, pullRequestNumber);
    }
}

async function commentMissingComponents(github, context, pullRequestNumber) {
    const {data: comments} = await github.issues.listComments({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: pullRequestNumber,
        per_page: 100
    });

    var found = false;
    for(var i=0; i<comments.length; i++) { 
        if (comments[i].body.includes("has no components in JIRA")) { 
            found = true; 
        } 
    }
    if (!found) {
        await github.issues.createComment({
            owner: context.repo.owner,
            repo: context.repo.repo,
            issue_number: pullRequestNumber,
            body: ":warning: Ticket **has no components in JIRA**, make sure you assign one."
        });
    }
}

async function commentNotStartedTicket(github, context, pullRequestNumber) {
    const {data: comments} = await github.issues.listComments({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: pullRequestNumber,
        per_page: 100
    });

    var found = false;
    for(var i=0; i<comments.length; i++) { 
        if (comments[i].body.includes("has not been started in JIRA")) { 
            found = true; 
        } 
    }
    if (!found) {
        await github.issues.createComment({
            owner: context.repo.owner,
            repo: context.repo.repo,
            issue_number: pullRequestNumber,
            body: ":warning: Ticket **has not been started in JIRA**, please click 'Start Progress'."
        });
    }
}

async function assignGitHubIssue(github, context, pullRequestNumber, issueInfo) {
    await github.issues.addAssignees({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: issueInfo.number,
        assignees: context.payload.pull_request.user.login
    });
    await github.issues.createComment({
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: pullRequestNumber,
        body: ":warning: GitHub issue #" + issueInfo.number + " **has been automatically assigned in GitHub** to PR creator."
    });
}

async function verifyGitHubIssue(github, context, pullRequestNumber, issueID) {
    const issueInfo = await helpers.getGitHubInfo(github, context, issueID, pullRequestNumber);
    if (issueInfo) {
        if (!issueInfo.assignees.length) {
            await assignGitHubIssue(github, context, pullRequestNumber, issueInfo);
        }
        if(!issueInfo.labels.length) {
            await github.issues.createComment({
                owner: context.repo.owner,
                repo: context.repo.repo,
                issue_number: pullRequestNumber,
                body: ":warning: GitHub issue #" + issueID + " **has no labels in GitHub**, please add labels for components."
            })
        }
    } else {
        await github.issues.createComment({
            owner: context.repo.owner,
            repo: context.repo.repo,
            issue_number: pullRequestNumber,
            body: ":x: GitHub issue #" + issueID + " could not be retrieved."
        })
    }
}

module.exports = async ({github, context}) => {
    const pullRequestNumber = context.payload.number;
    const title = context.payload.pull_request.title;
    const issue = helpers.detectIssue(title)
    if (issue){
        if (issue.kind == "jira") {
            await verifyJIRAIssue(github, context, pullRequestNumber, issue.id);
      } else if(issue.kind == "github") {
          await verifyGitHubIssue(github, context, pullRequestNumber, issue.id);
      }
    }
};
