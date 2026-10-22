//! Bounded per-MCP log buffers.
//!
//! Holds the most recent stderr lines from stdio MCP processes Core spawns,
//! plus Core's own connection events, so the clients can show why an MCP is
//! in an error state.
//!
//! Lines are redacted against the MCP's own configured secrets (env and
//! header values, credential-looking args, endpoint query values) before they
//! are stored, and entries are only kept for registered MCPs.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use serde::Serialize;
use uuid::Uuid;

use super::types::McpTransport;

/// Lines kept per MCP; older lines are dropped first.
pub const MCP_LOG_CAPACITY: usize = 300;
/// Longer lines are truncated to this many bytes (on a char boundary).
pub const MCP_LOG_LINE_MAX: usize = 2048;
/// Shorter values are too likely to occur in ordinary text to redact.
const MIN_SECRET_LEN: usize = 6;
const REDACTED: &str = "***";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum McpLogKind {
    /// A line the MCP process wrote to stderr.
    Stderr,
    /// A connection event recorded by Core.
    Event,
}

#[derive(Debug, Clone, Serialize)]
pub struct McpLogEntry {
    pub at: chrono::DateTime<chrono::Utc>,
    pub kind: McpLogKind,
    pub message: String,
}

#[derive(Default)]
struct McpLog {
    entries: VecDeque<McpLogEntry>,
    /// Longest first, so a secret containing another is replaced whole.
    secrets: Vec<String>,
}

#[derive(Default)]
pub struct McpLogStore {
    logs: Mutex<HashMap<Uuid, McpLog>>,
}

fn is_credential_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    [
        "token",
        "key",
        "secret",
        "password",
        "passwd",
        "auth",
        "credential",
        "bearer",
    ]
    .iter()
    .any(|word| name.contains(word))
}

/// Values in an MCP's configuration that must not appear in its logs.
pub fn transport_secrets(transport: &McpTransport) -> Vec<String> {
    let mut secrets: Vec<String> = Vec::new();
    match transport {
        McpTransport::Stdio { args, env, .. } => {
            secrets.extend(env.values().cloned());
            let mut previous_is_flag = false;
            for arg in args {
                if let Some((name, value)) = arg.split_once('=') {
                    if is_credential_name(name) {
                        secrets.push(value.to_string());
                    }
                } else if previous_is_flag {
                    secrets.push(arg.clone());
                }
                previous_is_flag =
                    arg.starts_with('-') && !arg.contains('=') && is_credential_name(arg);
            }
        }
        McpTransport::Http { endpoint, headers } => {
            for value in headers.values() {
                secrets.push(value.clone());
                // "Bearer <token>": the token alone may be echoed too.
                secrets.extend(value.split_whitespace().map(str::to_string));
            }
            if let Ok(url) = reqwest::Url::parse(endpoint) {
                secrets.extend(url.query_pairs().map(|(_, v)| v.into_owned()));
                if let Some(password) = url.password() {
                    secrets.push(password.to_string());
                }
            }
        }
    }
    secrets.retain(|s| s.len() >= MIN_SECRET_LEN);
    secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
    secrets.dedup();
    secrets
}

fn redact_with(secrets: &[String], text: &str) -> String {
    let mut text = text.to_string();
    for secret in secrets {
        if text.contains(secret.as_str()) {
            text = text.replace(secret.as_str(), REDACTED);
        }
    }
    text
}

fn truncate_line(line: &str) -> String {
    let line = line.trim_end_matches(['\r', '\n']);
    if line.len() <= MCP_LOG_LINE_MAX {
        return line.to_string();
    }
    let mut end = MCP_LOG_LINE_MAX;
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &line[..end])
}

impl McpLogStore {
    /// Start (or keep) a log for an MCP and set the secrets to redact.
    /// Existing entries survive a transport change.
    pub fn register(&self, id: Uuid, transport: &McpTransport) {
        let mut logs = self.logs.lock().unwrap_or_else(|e| e.into_inner());
        logs.entry(id).or_default().secrets = transport_secrets(transport);
    }

    /// Redact an MCP's configured secrets from text (e.g. an error message).
    pub fn redact(&self, id: Uuid, text: &str) -> String {
        let logs = self.logs.lock().unwrap_or_else(|e| e.into_inner());
        match logs.get(&id) {
            Some(log) => redact_with(&log.secrets, text),
            None => text.to_string(),
        }
    }

    /// Append an entry. A no-op for MCPs that aren't registered (removed),
    /// so a late stderr line can't recreate a deleted MCP's log.
    pub fn push(&self, id: Uuid, kind: McpLogKind, message: &str) {
        let mut logs = self.logs.lock().unwrap_or_else(|e| e.into_inner());
        let Some(log) = logs.get_mut(&id) else {
            return;
        };
        let entry = McpLogEntry {
            at: chrono::Utc::now(),
            kind,
            message: truncate_line(&redact_with(&log.secrets, message)),
        };
        if log.entries.len() == MCP_LOG_CAPACITY {
            log.entries.pop_front();
        }
        log.entries.push_back(entry);
    }

    pub fn event(&self, id: Uuid, message: &str) {
        self.push(id, McpLogKind::Event, message);
    }

    /// The last `limit` entries, oldest first.
    pub fn tail(&self, id: Uuid, limit: usize) -> Vec<McpLogEntry> {
        let logs = self.logs.lock().unwrap_or_else(|e| e.into_inner());
        let Some(log) = logs.get(&id) else {
            return Vec::new();
        };
        let skip = log.entries.len().saturating_sub(limit);
        log.entries.iter().skip(skip).cloned().collect()
    }

    /// Forget an MCP's log for good (it was removed).
    pub fn remove(&self, id: Uuid) {
        let mut logs = self.logs.lock().unwrap_or_else(|e| e.into_inner());
        logs.remove(&id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stdio(args: &[&str], env: &[(&str, &str)]) -> McpTransport {
        McpTransport::Stdio {
            command: "server".to_string(),
            args: args.iter().map(|a| a.to_string()).collect(),
            env: env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn keeps_only_the_most_recent_lines() {
        let store = McpLogStore::default();
        let id = Uuid::new_v4();
        store.register(id, &stdio(&[], &[]));
        for i in 0..(MCP_LOG_CAPACITY + 5) {
            store.push(id, McpLogKind::Stderr, &format!("line {i}"));
        }
        let all = store.tail(id, usize::MAX);
        assert_eq!(all.len(), MCP_LOG_CAPACITY);
        assert_eq!(all[0].message, "line 5");
        assert_eq!(
            all.last().unwrap().message,
            format!("line {}", MCP_LOG_CAPACITY + 4)
        );
    }

    #[test]
    fn tail_returns_newest_entries_oldest_first() {
        let store = McpLogStore::default();
        let id = Uuid::new_v4();
        store.register(id, &stdio(&[], &[]));
        store.event(id, "a");
        store.event(id, "b");
        store.push(id, McpLogKind::Stderr, "c\n");
        let tail = store.tail(id, 2);
        let messages: Vec<_> = tail.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(messages, ["b", "c"]);
        assert_eq!(tail[1].kind, McpLogKind::Stderr);
    }

    #[test]
    fn truncates_long_lines_on_a_char_boundary() {
        let store = McpLogStore::default();
        let id = Uuid::new_v4();
        store.register(id, &stdio(&[], &[]));
        let long = "é".repeat(MCP_LOG_LINE_MAX);
        store.event(id, &long);
        let message = &store.tail(id, 1)[0].message;
        assert!(message.ends_with('…'));
        assert!(message.len() <= MCP_LOG_LINE_MAX + '…'.len_utf8());
    }

    #[test]
    fn removed_and_unknown_ids_stay_empty() {
        let store = McpLogStore::default();
        let id = Uuid::new_v4();
        // Unregistered: pushes are dropped.
        store.event(id, "x");
        assert!(store.tail(id, 10).is_empty());
        store.register(id, &stdio(&[], &[]));
        store.event(id, "x");
        assert_eq!(store.tail(id, 10).len(), 1);
        store.remove(id);
        // A late line after removal doesn't recreate the log.
        store.push(id, McpLogKind::Stderr, "late");
        assert!(store.tail(id, 10).is_empty());
    }

    #[test]
    fn redacts_configured_secrets() {
        let store = McpLogStore::default();
        let id = Uuid::new_v4();
        store.register(
            id,
            &stdio(
                &[
                    "--api-key",
                    "flag-secret-1",
                    "--token=arg-secret-2",
                    "--port=8080",
                    "/srv/data",
                ],
                &[("API_KEY", "env-secret-3"), ("SHORT", "abc")],
            ),
        );
        store.push(
            id,
            McpLogKind::Stderr,
            "using flag-secret-1 arg-secret-2 env-secret-3 abc port 8080 /srv/data",
        );
        assert_eq!(
            store.tail(id, 1)[0].message,
            "using *** *** *** abc port 8080 /srv/data"
        );
        assert_eq!(store.redact(id, "Error: env-secret-3"), "Error: ***");

        let http = Uuid::new_v4();
        store.register(
            http,
            &McpTransport::Http {
                endpoint: "https://mcp.example.com/mcp?key=query-secret".to_string(),
                headers: HashMap::from([(
                    "Authorization".to_string(),
                    "Bearer header-secret".to_string(),
                )]),
            },
        );
        assert_eq!(
            store.redact(http, "header-secret / Bearer header-secret / query-secret"),
            "*** / *** / ***"
        );
    }
}
