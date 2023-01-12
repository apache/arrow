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

const https = require('https');

/**
 * Given the title of a PullRequest return the Issue
 *
 * @param {String} title 
 * @returns {Issue} or null if no issue detected.
 *
 * @typedef {Object} Issue
 * @property {string} kind - The kind of issue: minor, jira or github
 * @property {string} id   - The id of the issue:
 *                            PARQUET-XXXX for jira
 *                            The numeric issue id for github
 */
function detectIssue(title) {
    if (!title) {
        return null;
    }
    if (title.startsWith("MINOR: ")) {
        return {"kind": "minor"};
    }
    const matched_jira = /^(WIP:?\s*)?((PARQUET)-\d+)/.exec(title);
    if (matched_jira) {
        return {"kind": "jira", "id": matched_jira[2]};
    }
    const matched_gh = /^(WIP:?\s*)?GH-(\d+)/.exec(title);
    if (matched_gh) {
        return {"kind": "github", "id": matched_gh[2]};
    }
    return null;
}

/**
 * Retrieves information about a JIRA issue.
 * @param {String} jiraID 
 * @returns {Object} the information about a JIRA issue.
 */
async function getJiraInfo(jiraID) {
    const jiraURL = `https://issues.apache.org/jira/rest/api/2/issue/${jiraID}`;

    return new Promise((resolve) => {
        https.get(jiraURL, res => {
            let data = '';

            res.on('data', chunk => { data += chunk }) 

            res.on('end', () => {
               resolve(JSON.parse(data));
            })
        })
    });
}

/**
 * Retrieves information about a GitHub issue.
 * @param {String} issueID
 * @returns {Object} the information about a GitHub issue.
 */
 async function getGitHubInfo(github, context, issueID, pullRequestNumber) {
    try {
        const response = await github.issues.get({
            issue_number: issueID,
            owner: context.repo.owner,
            repo: context.repo.repo,
        })
        return response.data
    } catch (error) {
        console.log(`${error.name}: ${error.code}`);
        return false
    }
}

module.exports = {
    detectIssue,
    getJiraInfo,
    getGitHubInfo
};