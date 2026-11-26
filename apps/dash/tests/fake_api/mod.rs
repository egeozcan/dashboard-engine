//! Real GitHub and Jira REST payload shapes for the loopback stand-ins of the acceptance suites.
//!
//! The suites keep their upstream state as the reference fixture's normalized records
//! (`packages/github/mock/fixtures/prs.json`, `packages/jira/mock/fixtures/issues.json`) and
//! answer with what the real APIs return, so the connectors' normalization is exercised end to
//! end through the host HTTP broker.
#![allow(dead_code)]

use serde_json::{json, Value};
use std::collections::BTreeSet;

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// A GitHub REST pull request for a normalized record. `detail` adds the fields only
/// `GET /repos/{o}/{r}/pulls/{n}` returns (`merged`, `mergeable`, ...).
pub fn github_pr(rec: &Value, detail: bool) -> Value {
    let repo = rec["repository"].as_str().unwrap_or_default();
    let number = rec["number"].as_i64().unwrap_or_default();
    let state = rec["state"].as_str().unwrap_or("open");
    let merged = state == "merged" || rec["merged"] == true;
    let mut pr = json!({
        "id": 9_000_000 + number,
        "node_id": format!("PR_{number}"),
        "number": number,
        "state": if state == "open" { "open" } else { "closed" },
        "title": rec["title"],
        "user": { "login": rec["author"] },
        "body": null,
        "draft": rec["draft"].as_bool().unwrap_or(false),
        "html_url": format!("https://github.com/{repo}/pull/{number}"),
        "created_at": rec["created_at"].as_str().unwrap_or("2026-11-20T09:00:00Z"),
        "updated_at": rec["updated_at"],
        "closed_at": if state == "open" { Value::Null } else { json!("2026-11-23T12:00:00Z") },
        "merged_at": if merged { json!("2026-11-23T12:00:00Z") } else { Value::Null },
        "base": { "ref": rec["base"].as_str().unwrap_or("main"), "repo": { "full_name": repo } },
        "head": {
            "ref": rec["head"].as_str().unwrap_or("feature"),
            "sha": rec["head_sha"].as_str().unwrap_or("0000000"),
            "repo": { "full_name": repo }
        },
        "labels": strings(&rec["labels"]).iter().map(|l| json!({ "name": l, "color": "ededed" })).collect::<Vec<_>>(),
        "assignees": strings(&rec["assignees"]).iter().map(|l| json!({ "login": l })).collect::<Vec<_>>(),
        "requested_reviewers": strings(&rec["requested_reviewers"]).iter().map(|l| json!({ "login": l })).collect::<Vec<_>>(),
    });
    if detail {
        pr["merged"] = json!(merged);
        pr["mergeable"] = if state == "open" {
            json!(true)
        } else {
            Value::Null
        };
        pr["mergeable_state"] = json!(if state == "open" { "clean" } else { "unknown" });
        pr["merged_by"] = if merged {
            json!({ "login": "octocat" })
        } else {
            Value::Null
        };
    }
    pr
}

/// Jira status category key of a status name in the fixture workflow.
pub fn jira_category(status: &str) -> &'static str {
    match status {
        "To Do" => "new",
        "Done" => "done",
        _ => "indeterminate",
    }
}

/// A Jira REST issue (`/rest/api/3/search/jql`, `/rest/api/3/issue/{key}`) for a normalized record.
pub fn jira_issue(rec: &Value) -> Value {
    let key = rec["key"].as_str().unwrap_or_default();
    let status = rec["status"].as_str().unwrap_or("To Do");
    let project = key.split('-').next().unwrap_or_default();
    json!({
        "id": rec["id"].as_str().map(str::to_string).unwrap_or_else(|| format!("1{}", key.len())),
        "key": key,
        "self": format!("https://jira.example.com/rest/api/3/issue/{key}"),
        "fields": {
            "summary": rec["summary"],
            "status": {
                "name": status,
                "statusCategory": { "key": jira_category(status), "name": status }
            },
            "priority": rec["priority"].as_str().map(|p| json!({ "name": p })).unwrap_or(Value::Null),
            "assignee": rec["assignee"].as_str().map(|a| json!({ "displayName": a, "accountId": format!("acc-{a}") })).unwrap_or(Value::Null),
            "reporter": null,
            "issuetype": { "name": rec["issue_type"].as_str().unwrap_or("Task") },
            "project": { "key": project },
            "labels": strings(&rec["labels"]),
            "created": rec["created"],
            "updated": rec["updated"],
            "resolution": null,
            "duedate": null
        }
    })
}

/// `{ transitions: [...] }` of `GET /rest/api/3/issue/{key}/transitions` for an issue in `status`
/// (the reference fixture's workflow).
pub fn jira_transitions(status: &str) -> Value {
    let t = |id: &str, name: &str, to: &str| {
        json!({
            "id": id,
            "name": name,
            "to": { "name": to, "statusCategory": { "key": jira_category(to) } }
        })
    };
    let list = match status {
        "To Do" => vec![
            t("11", "Start Progress", "In Progress"),
            t("31", "Done", "Done"),
        ],
        "In Progress" => vec![
            t("21", "Request Review", "In Review"),
            t("31", "Done", "Done"),
            t("41", "Stop Progress", "To Do"),
        ],
        "In Review" => vec![
            t("31", "Done", "Done"),
            t("51", "Back to Progress", "In Progress"),
        ],
        "Done" => vec![t("61", "Reopen", "To Do")],
        _ => vec![],
    };
    json!({ "transitions": list })
}

/// Target status of a transition id in the fixture workflow.
pub fn jira_transition_target(id: &str) -> Option<&'static str> {
    match id {
        "11" | "51" => Some("In Progress"),
        "21" => Some("In Review"),
        "31" => Some("Done"),
        "41" | "61" => Some("To Do"),
        _ => None,
    }
}

/// Issue keys of a `key in (A-1,B-2)` JQL query.
pub fn jql_keys(jql: &str) -> BTreeSet<String> {
    jql.split_once('(')
        .and_then(|(_, rest)| rest.split_once(')'))
        .map(|(inner, _)| inner.split(',').map(|k| k.trim().to_string()).collect())
        .unwrap_or_default()
}

/// `GET /rest/api/3/search/jql` response body for the matching issues (single page).
pub fn jira_search_page(issues: &[Value]) -> Value {
    json!({
        "issues": issues.iter().map(jira_issue).collect::<Vec<_>>(),
        "isLast": true
    })
}
