# This script requires repositories `arrow` and `arrow-rs` to be present at the appropriate paths


import os
import git
from collections import Counter
from github import Github
gh_session = Github(os.getenv('GITHUB_PAT'))



# Configure the repos
arrow = {
    "dir": "../arrow", 
    "repo": "apache/arrow", 
    "from_tag": "apache-arrow-5.0.0", 
    "to_tag": "apache-arrow-6.0.0"
}

arrow_rs = {
    "dir": "../arrow-rs", 
    "repo": "apache/arrow-rs", 
    "from_tag": "5.0.0", 
    "to_tag": "6.0.0"
}

repos = [arrow, arrow_rs]


def get_commits(git_dir, repo_config):
    # get the relevant commit IDs
    cts = git_dir.log('--format=%h|%an|%ae|%cn|%ce', repo_config["from_tag"] + ".." + repo_config["to_tag"])
    cts = cts.split("\n")
    names = ['hash', 'author_name', 'author_email', 'committer_name', 'committer_email']
    return [dict(zip(names, commit.split("|"))) for commit in cts]

def fix_multiple_names_to_email(commits, email_field, name_field):
    """
    Map email addresses to a single name - defaults to the longest name
    """
    email_to_name = {}
    for commit in commits:
        if commit[email_field] in email_to_name:
            if len(commit[name_field]) > len(email_to_name[commit[email_field]]):
                email_to_name[commit[email_field]] = commit[name_field]
        else:
            email_to_name[commit[email_field]] = commit[name_field]
            
    for commit in commits:
        commit[name_field] = email_to_name[commit[email_field]]

def fix_committer_name(commit_list, repo_obj):
    """ If the committer name is marked as GitHub, use other sources of info to find the correct name """
    for commit in commit_list:
        if commit["committer_name"] == "GitHub":
            issue = repo_obj.get_commit(sha=commit["hash"])

            # If there is no committer flagged in the committer field
            if issue.committer is None and issue.commit.committer is not None:
                if issue.commit.committer.name is not None:
                    commit["committer_name"] = issue.commit.committer.name
                if issue.commit.committer.email is not None:
                    commit["committer_email"] = issue.commit.committer.email

            if(issue.committer.name == 'GitHub Web Flow'):

                # it's a push to master
                if issue.get_pulls().totalCount == 0:
                    committer = issue.author
                # it's a merge from another branch
                else:
                    merge_pr = issue.get_pulls()
                    # if this issue has been cherry-picked just get the first one
                    committer = merge_pr[merge_pr.totalCount-1].merged_by
            else:
                committer = issue.committer

            # not every user has the name filled in 
            if committer.name is not None:
                commit["committer_name"] = committer.name
            else:
                commit["committer_name"] = committer.login
            if committer.email is not None:
                commit["committer_email"] = committer.email


def print_contributors(ctbs):
    """ Given a list of commits, print who contributed how many """
    authors = [commit["author_name"] for commit in ctbs]
    distinct_contributors = len(set((authors)))
    
    print("This release includes", len(ctbs), "commits from", distinct_contributors, "distinct contributors.\n")
    
    author_numbers = Counter(authors).most_common()
    for (name, number) in author_numbers:
        print(number, "\t", name)


def print_patch_committers(mergers):
    """ Given a list of commits, print who merged how many """
    
    committers = [commit["committer_name"] for commit in mergers]
    
    print("The following Apache committers merged contributed patches to the repository.\n")
    
    committer_numbers = Counter(committers).most_common()
    for (name, number) in committer_numbers:
        print(number, "\t", name)

def get_commit_list(repo):
    """ Get tidied list of commits for a repo """
    git_dir = git.Git(repo["dir"])
    commit_list = get_commits(git_dir, repo)
    fix_multiple_names_to_email(commit_list, "author_email", "author_name")
    repo_session = gh_session.get_repo(repo["repo"])
    fix_committer_name(commit_list, repo_session)
    return commit_list



final_list = []

for repo in repos:
    final_list.extend(get_commit_list(repo))

print_contributors(final_list)
print_patch_committers(final_list)







