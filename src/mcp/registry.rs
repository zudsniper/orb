//! MCP runtime registry - manages connections and tool execution.
//!
//! Supports both HTTP and stdio transports:
//! - HTTP: JSON-RPC over HTTP POST requests
//! - Stdio: JSON-RPC over stdin/stdout with spawned child processes

use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use super::config::McpConfigStore;
use super::logs::{McpLogEntry, McpLogKind, McpLogStore};
use super::types::*;
use crate::util::current_service_companion_binary_path;

/// MCP protocol version we support
const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

/// Streamable HTTP servers answer either with JSON or an SSE stream and
/// reject requests (406) that don't accept both.
const HTTP_ACCEPT: &str = "application/json, text/event-stream";
/// Upper bound on a buffered SSE event, so a stream without event
/// boundaries can't grow without limit.
const SSE_EVENT_MAX: usize = 8 * 1024 * 1024;
/// Session id a Streamable HTTP server may assign at initialize.
const MCP_SESSION_HEADER: &str = "mcp-session-id";

/// Pop the next complete event from an SSE byte buffer and return its joined
/// `data:` payload. Carriage returns must already be stripped.
fn take_sse_event(buffer: &mut Vec<u8>) -> Option<String> {
    let end = buffer.windows(2).position(|w| w == b"\n\n")?;
    let event: Vec<u8> = buffer.drain(..end + 2).collect();
    let event = String::from_utf8_lossy(&event);
    let data: Vec<&str> = event
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(|data| data.strip_prefix(' ').unwrap_or(data))
        .collect();
    Some(data.join("\n"))
}

/// Parse one JSON-RPC message (or batch) and return the response to
/// `request_id`. Notifications, server-to-client requests, responses to other
/// ids and non-JSON lines yield `None`. An error without an id is taken as
/// the response, since the server couldn't tell which request it rejects.
fn parse_matching_response(payload: &str, request_id: u64) -> Option<JsonRpcResponse> {
    let value: serde_json::Value = serde_json::from_str(payload.trim()).ok()?;
    let messages = match value {
        serde_json::Value::Array(items) => items,
        other => vec![other],
    };
    messages.into_iter().find_map(|message| {
        if message.get("method").is_some() {
            return None;
        }
        match message.get("id") {
            Some(id) if id.as_u64() == Some(request_id) => {}
            // A parse/invalid-request error can't name the request it rejects;
            // with one request in flight per call it is ours.
            Some(serde_json::Value::Null) | None if message.get("error").is_some() => {}
            _ => return None,
        }
        serde_json::from_value(message).ok()
    })
}

fn into_result(response: JsonRpcResponse) -> anyhow::Result<serde_json::Value> {
    if let Some(error) = response.error {
        anyhow::bail!("JSON-RPC error {}: {}", error.code, error.message);
    }
    response
        .result
        .ok_or_else(|| anyhow::anyhow!("No result in response"))
}

/// Read an SSE response until the event carrying the response to `request_id`.
async fn read_sse_response(
    response: reqwest::Response,
    request_id: u64,
) -> anyhow::Result<JsonRpcResponse> {
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| e.without_url())?;
        buffer.extend(chunk.iter().copied().filter(|b| *b != b'\r'));
        while let Some(data) = take_sse_event(&mut buffer) {
            if let Some(found) = parse_matching_response(&data, request_id) {
                return Ok(found);
            }
        }
        if buffer.len() > SSE_EVENT_MAX {
            anyhow::bail!(
                "Event stream sent more than {} bytes without an event boundary",
                SSE_EVENT_MAX
            );
        }
    }
    // A final event may lack the terminating blank line.
    buffer.extend_from_slice(b"\n\n");
    take_sse_event(&mut buffer)
        .and_then(|data| parse_matching_response(&data, request_id))
        .ok_or_else(|| anyhow::anyhow!("Event stream ended without a response"))
}

/// Short, secret-free description of a transport for log events: the origin
/// of an HTTP endpoint (no path or query) or the stdio command's file name.
fn transport_label(transport: &McpTransport) -> String {
    match transport {
        McpTransport::Http { endpoint, .. } => match reqwest::Url::parse(endpoint) {
            Ok(url) => format!("http {}", url.origin().ascii_serialization()),
            Err(_) => "http".to_string(),
        },
        McpTransport::Stdio { command, .. } => {
            let name = Path::new(command)
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| command.clone());
            format!("stdio {}", name)
        }
    }
}

/// Sanitize MCP server name to create a valid function name prefix.
///
/// Converts names like "filesystem" or "My Server" to lowercase alphanumeric.
fn sanitize_mcp_prefix(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-')
        .collect::<String>()
        .to_lowercase()
        .replace('-', "_")
}

fn command_exists(command: &str) -> bool {
    if command.trim().is_empty() {
        return false;
    }

    let path = Path::new(command);
    if path.is_absolute() || command.contains('/') {
        return path.exists();
    }

    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };

    for dir in std::env::split_paths(&paths) {
        let candidate = dir.join(command);
        if candidate.is_file() {
            return true;
        }
        #[cfg(windows)]
        {
            for ext in ["exe", "cmd", "bat"] {
                if candidate.with_extension(ext).is_file() {
                    return true;
                }
            }
        }
    }

    false
}

fn is_builtin_workspace_mcp(config: &McpServerConfig, working_dir: &Path) -> bool {
    if config.name != "workspace" {
        return false;
    }

    let McpTransport::Stdio { command, args, .. } = &config.transport else {
        return false;
    };
    if !args.is_empty() {
        return false;
    }

    let command_path = Path::new(command);
    let scoped_workspace_mcp = current_service_companion_binary_path("workspace-mcp");
    command == "workspace-mcp"
        || command_path == Path::new("/usr/local/bin/workspace-mcp")
        || scoped_workspace_mcp
            .as_deref()
            .is_some_and(|path| command_path == path)
        || command_path == working_dir.join("target/release/workspace-mcp")
        || command_path == working_dir.join("target/debug/workspace-mcp")
}

/// Reconcile the implicit full-environment grant for the trusted built-in
/// workspace proxy. A config merely named `workspace` is not trusted: an
/// operator may have repointed it to an unrelated executable.
fn reconcile_workspace_mcp_env_trust(config: &mut McpServerConfig, working_dir: &Path) -> bool {
    if config.name != "workspace" {
        return false;
    }

    if is_builtin_workspace_mcp(config, working_dir) {
        if config.workspace_env_allowlist == ["*".to_string()] {
            return false;
        }
        config.workspace_env_allowlist = vec!["*".to_string()];
        return true;
    }

    let original_len = config.workspace_env_allowlist.len();
    config
        .workspace_env_allowlist
        .retain(|entry| entry.trim() != "*");
    config.workspace_env_allowlist.len() != original_len
}

/// Handle for a stdio MCP process
struct StdioProcess {
    child: Child,
    stdin: tokio::process::ChildStdin,
    stdout_lines: Arc<Mutex<BufReader<tokio::process::ChildStdout>>>,
}

/// Runtime registry for MCP servers.
pub struct McpRegistry {
    /// Persistent configuration store
    config_store: Arc<McpConfigStore>,
    /// Runtime state for each MCP (keyed by ID)
    states: RwLock<HashMap<Uuid, McpServerState>>,
    /// HTTP client for HTTP MCP requests
    http_client: reqwest::Client,
    /// Stdio processes for stdio MCPs (keyed by ID)
    stdio_processes: RwLock<HashMap<Uuid, Arc<Mutex<StdioProcess>>>>,
    /// Disabled tools (by name)
    disabled_tools: RwLock<std::collections::HashSet<String>>,
    /// Request ID counter for JSON-RPC
    request_id: AtomicU64,
    /// Recent stderr lines and connection events (keyed by ID)
    logs: Arc<McpLogStore>,
    /// Streamable HTTP session ids assigned by servers (keyed by ID)
    http_sessions: RwLock<HashMap<Uuid, String>>,
}

const MCP_REQUEST_TIMEOUT: Duration = Duration::from_secs(600);
const MCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

impl McpRegistry {
    /// Create a new MCP registry.
    pub async fn new(working_dir: &Path) -> Self {
        let config_store = Arc::new(McpConfigStore::new(working_dir).await);

        // Initialize states from configs
        let mut configs = config_store.list().await;
        configs = Self::ensure_defaults(&config_store, configs, working_dir).await;
        let mut states = HashMap::new();
        let logs = Arc::new(McpLogStore::default());
        for config in configs {
            logs.register(config.id, &config.transport);
            states.insert(config.id, McpServerState::from_config(config));
        }

        // Use generous timeouts for long-running MCP tools (e.g., Minecraft launches)
        let http_client = reqwest::Client::builder()
            .timeout(MCP_REQUEST_TIMEOUT)
            .connect_timeout(MCP_CONNECT_TIMEOUT)
            .build()
            .unwrap_or_default();

        Self {
            config_store,
            states: RwLock::new(states),
            http_client,
            stdio_processes: RwLock::new(HashMap::new()),
            disabled_tools: RwLock::new(std::collections::HashSet::new()),
            request_id: AtomicU64::new(1),
            logs,
            http_sessions: RwLock::new(HashMap::new()),
        }
    }

    /// The most recent log entries for an MCP, oldest first.
    pub fn logs(&self, id: Uuid, limit: usize) -> Vec<McpLogEntry> {
        self.logs.tail(id, limit)
    }

    /// Return the raw MCP configs (for workspace opencode.json generation).
    pub async fn list_configs(&self) -> Vec<McpServerConfig> {
        self.config_store.list().await
    }

    fn default_configs(working_dir: &Path) -> Vec<McpServerConfig> {
        let mut desktop_env = HashMap::new();
        if let Ok(res) = std::env::var("DESKTOP_RESOLUTION") {
            if !res.trim().is_empty() {
                desktop_env.insert("DESKTOP_RESOLUTION".to_string(), res);
            }
        } else {
            desktop_env.insert("DESKTOP_RESOLUTION".to_string(), "1920x1080".to_string());
        }

        let desktop_command = {
            let release = working_dir
                .join("target")
                .join("release")
                .join("desktop-mcp");
            let debug = working_dir.join("target").join("debug").join("desktop-mcp");
            if release.exists() {
                release.to_string_lossy().to_string()
            } else if debug.exists() {
                debug.to_string_lossy().to_string()
            } else {
                "desktop-mcp".to_string()
            }
        };
        let mut desktop = McpServerConfig::new_stdio(
            "desktop".to_string(),
            desktop_command,
            Vec::new(),
            desktop_env,
        );
        desktop.scope = McpScope::Workspace;
        desktop.default_enabled = true;

        let workspace_command = {
            let release = working_dir
                .join("target")
                .join("release")
                .join("workspace-mcp");
            let debug = working_dir
                .join("target")
                .join("debug")
                .join("workspace-mcp");
            if release.exists() {
                release.to_string_lossy().to_string()
            } else if debug.exists() {
                debug.to_string_lossy().to_string()
            } else {
                "workspace-mcp".to_string()
            }
        };
        let mut workspace = McpServerConfig::new_stdio(
            "workspace".to_string(),
            workspace_command,
            Vec::new(),
            HashMap::new(),
        );
        workspace.scope = McpScope::Workspace;
        workspace.default_enabled = true;
        // workspace-mcp is a trusted compatibility shell/file proxy. It needs
        // the same environment as the workspace commands it executes. Native
        // harness bash remains the default for ordinary missions.
        workspace.workspace_env_allowlist = vec!["*".to_string()];
        // Prefer bunx (Bun) when present, but fall back to npx for compatibility.
        let js_runner = if command_exists("bunx") {
            "bunx"
        } else {
            "npx"
        };
        let mut playwright = McpServerConfig::new_stdio(
            "playwright".to_string(),
            js_runner.to_string(),
            vec![
                "@playwright/mcp@latest".to_string(),
                "--headless".to_string(),
                "--isolated".to_string(),
                "--no-sandbox".to_string(),
            ],
            HashMap::new(),
        );
        playwright.scope = McpScope::Workspace;
        playwright.default_enabled = true;

        let orchestrator_command = {
            let release = working_dir
                .join("target")
                .join("release")
                .join("sandboxed-mcp");
            let debug = working_dir
                .join("target")
                .join("debug")
                .join("sandboxed-mcp");
            if release.exists() {
                release.to_string_lossy().to_string()
            } else if debug.exists() {
                debug.to_string_lossy().to_string()
            } else if let Some(scoped) =
                current_service_companion_binary_path("sandboxed-mcp").filter(|path| path.exists())
            {
                scoped.to_string_lossy().to_string()
            } else {
                "sandboxed-mcp".to_string()
            }
        };
        let mut orchestrator = McpServerConfig::new_stdio(
            "sandboxed".to_string(),
            orchestrator_command,
            Vec::new(),
            HashMap::new(),
        );
        orchestrator.scope = McpScope::Workspace;
        orchestrator.default_enabled = true;

        let automation_manager_command = std::env::var("AUTOMATION_MANAGER_MCP_BIN")
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| {
                let release = working_dir
                    .join("target")
                    .join("release")
                    .join("automation-manager-mcp");
                let debug = working_dir
                    .join("target")
                    .join("debug")
                    .join("automation-manager-mcp");
                if release.exists() {
                    release.to_string_lossy().to_string()
                } else if debug.exists() {
                    debug.to_string_lossy().to_string()
                } else {
                    "automation-manager-mcp".to_string()
                }
            });
        let mut automation_manager_env = HashMap::new();
        if let Ok(api_url) = std::env::var("API_URL") {
            if !api_url.trim().is_empty() {
                automation_manager_env.insert("API_URL".to_string(), api_url);
            }
        }
        let mut automation_manager = McpServerConfig::new_stdio(
            "automation-manager".to_string(),
            automation_manager_command,
            Vec::new(),
            automation_manager_env,
        );
        automation_manager.scope = McpScope::Workspace;
        automation_manager.default_enabled = false;

        let mut engram = McpServerConfig::new_stdio(
            "engram".to_string(),
            "engram".to_string(),
            vec!["mcp".to_string()],
            HashMap::new(),
        );
        engram.scope = McpScope::Workspace;
        engram.default_enabled = false;

        vec![
            workspace,
            desktop,
            playwright,
            orchestrator,
            automation_manager,
            engram,
        ]
    }

    async fn ensure_defaults(
        config_store: &McpConfigStore,
        mut configs: Vec<McpServerConfig>,
        working_dir: &Path,
    ) -> Vec<McpServerConfig> {
        fn resolve_local_binary(working_dir: &Path, name: &str) -> Option<String> {
            let release = working_dir.join("target").join("release").join(name);
            if release.exists() {
                return Some(release.to_string_lossy().to_string());
            }
            let debug = working_dir.join("target").join("debug").join(name);
            if debug.exists() {
                return Some(debug.to_string_lossy().to_string());
            }
            if let Some(scoped) =
                current_service_companion_binary_path(name).filter(|path| path.exists())
            {
                return Some(scoped.to_string_lossy().to_string());
            }
            None
        }

        // Retire the built-in orchestrator descriptor in the same rollout.
        // An unrelated user-configured executable with that name is preserved.
        for config in &mut configs {
            // The legacy automation control MCP requires a signing secret and
            // bypasses the executor role boundary. Keep custom executables,
            // but never revive the old built-in through persisted enablement.
            let legacy_automation = config.name == "automation-manager"
                && matches!(&config.transport, McpTransport::Stdio { command, .. }
                    if Path::new(command).file_name().and_then(|s| s.to_str())
                        .is_some_and(|s| matches!(s, "automation-manager-mcp" | "automation-manager-mcp-dev")));
            if legacy_automation {
                config.default_enabled = false;
                if let McpTransport::Stdio { env, .. } = &mut config.transport {
                    env.remove("JWT_SECRET");
                }
                let updated = config.clone();
                let _ = config_store
                    .update(config.id, move |entry| *entry = updated)
                    .await;
            }
            let builtin = config.name == "orchestrator"
                && matches!(&config.transport,
                McpTransport::Stdio { command, .. } if Path::new(command).file_name().and_then(|s|s.to_str()).is_some_and(|s|matches!(s,"orchestrator-mcp"|"orchestrator-mcp-dev")));
            if builtin {
                config.name = "sandboxed".into();
                if let McpTransport::Stdio { command, args, env } = &mut config.transport {
                    *command = resolve_local_binary(working_dir, "sandboxed-mcp")
                        .unwrap_or_else(|| "sandboxed-mcp".into());
                    *args = vec!["--profile".into(), "executor".into()];
                    for key in ["JWT_SECRET", "API_TOKEN", "SANDBOXED_API_TOKEN"] {
                        env.remove(key);
                    }
                }
                let updated = config.clone();
                let _ = config_store
                    .update(config.id, move |entry| *entry = updated)
                    .await;
            }
        }

        // Remove duplicate MCPs by name (keep the first one).
        // This handles corrupted configs where the same MCP name appears twice.
        let mut seen_names = std::collections::HashSet::new();
        let mut duplicates = Vec::new();
        for config in &configs {
            if !seen_names.insert(config.name.clone()) {
                duplicates.push(config.id);
                tracing::warn!(
                    "Removing duplicate MCP '{}' (id: {})",
                    config.name,
                    config.id
                );
            }
        }
        for dup_id in duplicates {
            let _ = config_store.remove(dup_id).await;
            configs.retain(|c| c.id != dup_id);
        }

        let defaults = Self::default_configs(working_dir);
        for config in defaults {
            if configs.iter().any(|c| c.name == config.name) {
                continue;
            }
            match config_store.add(config.clone()).await {
                Ok(saved) => configs.push(saved),
                Err(e) => tracing::warn!("Failed to add default MCP {}: {}", config.name, e),
            }
        }
        // Ensure Playwright MCP runs in isolated mode and without sandboxing
        // so it can launch browsers under root.
        for config in configs.iter_mut() {
            if config.name != "playwright" {
                continue;
            }

            if config.scope != McpScope::Workspace {
                config.scope = McpScope::Workspace;
                let id = config.id;
                let _ = config_store
                    .update(id, |c| {
                        c.scope = McpScope::Workspace;
                    })
                    .await;
            }

            let missing_flags: Vec<&str> = match &config.transport {
                McpTransport::Stdio { args, .. } => ["--headless", "--isolated", "--no-sandbox"]
                    .iter()
                    .copied()
                    .filter(|flag| !args.iter().any(|arg| arg == *flag))
                    .collect(),
                McpTransport::Http { .. } => Vec::new(),
            };

            if missing_flags.is_empty() {
                continue;
            }

            if let McpTransport::Stdio { args, .. } = &mut config.transport {
                for flag in &missing_flags {
                    args.push((*flag).to_string());
                }
            }

            let id = config.id;
            let _ = config_store
                .update(id, |c| {
                    if let McpTransport::Stdio { args, .. } = &mut c.transport {
                        for flag in ["--headless", "--isolated", "--no-sandbox"] {
                            if !args.iter().any(|arg| arg == flag) {
                                args.push(flag.to_string());
                            }
                        }
                    }
                })
                .await;
        }

        // Ensure workspace/desktop/orchestrator MCPs have correct scope (migrate old configs).
        // This must run even if the binary doesn't exist locally.
        for config in configs.iter_mut() {
            if !matches!(config.name.as_str(), "workspace" | "desktop" | "sandboxed") {
                continue;
            }

            if config.scope != McpScope::Workspace {
                config.scope = McpScope::Workspace;
                let id = config.id;
                let _ = config_store
                    .update(id, |c| {
                        c.scope = McpScope::Workspace;
                    })
                    .await;
            }
        }

        // Ensure built-in MCPs have default_enabled = true (migrate old configs).
        for config in configs.iter_mut() {
            if !matches!(
                config.name.as_str(),
                "workspace" | "desktop" | "playwright" | "sandboxed"
            ) {
                continue;
            }

            if !config.default_enabled {
                config.default_enabled = true;
                let id = config.id;
                let _ = config_store
                    .update(id, |c| {
                        c.default_enabled = true;
                    })
                    .await;
            }
        }

        // workspace-mcp is the only built-in compatibility proxy that needs
        // the full workspace environment for commands it executes. Persist
        // that trust decision explicitly so all other MCPs stay deny-by-default.
        for config in configs
            .iter_mut()
            .filter(|config| config.name == "workspace")
        {
            if reconcile_workspace_mcp_env_trust(config, working_dir) {
                let id = config.id;
                let allowlist = config.workspace_env_allowlist.clone();
                let _ = config_store
                    .update(id, move |c| {
                        c.workspace_env_allowlist = allowlist;
                    })
                    .await;
            }
        }

        // Prefer repo-local MCP binaries for workspace/desktop (debug or release),
        // so default configs work without installing to PATH.
        for config in configs.iter_mut() {
            let binary_name = match config.name.as_str() {
                "workspace" => Some("workspace-mcp"),
                "desktop" => Some("desktop-mcp"),
                "sandboxed" => Some("sandboxed-mcp"),
                _ => None,
            };

            let Some(binary_name) = binary_name else {
                continue;
            };
            let Some(resolved) = resolve_local_binary(working_dir, binary_name) else {
                continue;
            };

            if let McpTransport::Stdio { command, .. } = &mut config.transport {
                if command != &resolved {
                    *command = resolved.clone();
                    let id = config.id;
                    let _ = config_store
                        .update(id, |c| {
                            if let McpTransport::Stdio { command, .. } = &mut c.transport {
                                *command = resolved.clone();
                            }
                        })
                        .await;
                }
            }
        }

        configs
    }

    /// Get the next request ID for JSON-RPC
    fn next_request_id(&self) -> u64 {
        self.request_id.fetch_add(1, Ordering::SeqCst)
    }

    /// POST a JSON-RPC message to an HTTP MCP. Carries the session id the
    /// server assigned at initialize and records a newly assigned one.
    async fn post_http(
        &self,
        mcp_id: Uuid,
        endpoint: &str,
        headers: &HashMap<String, String>,
        body: &serde_json::Value,
    ) -> anyhow::Result<reqwest::Response> {
        let mut req_builder = self
            .http_client
            .post(endpoint)
            .header("Content-Type", "application/json")
            .header("Accept", HTTP_ACCEPT);

        if let Some(session) = self.http_sessions.read().await.get(&mcp_id) {
            req_builder = req_builder.header(MCP_SESSION_HEADER, session.as_str());
        }

        // Add custom headers
        for (key, value) in headers {
            req_builder = req_builder.header(key.as_str(), value.as_str());
        }

        let response = req_builder
            .json(body)
            .send()
            .await
            .map_err(|e| e.without_url())?;

        if let Some(session) = response
            .headers()
            .get(MCP_SESSION_HEADER)
            .and_then(|v| v.to_str().ok())
        {
            self.http_sessions
                .write()
                .await
                .insert(mcp_id, session.to_string());
        }

        Ok(response)
    }

    /// Send a JSON-RPC request via HTTP (plain JSON or Streamable HTTP SSE).
    async fn send_jsonrpc_http(
        &self,
        mcp_id: Uuid,
        endpoint: &str,
        method: &str,
        params: Option<serde_json::Value>,
        headers: &HashMap<String, String>,
    ) -> anyhow::Result<serde_json::Value> {
        let request = JsonRpcRequest::new(self.next_request_id(), method, params);
        let request_id = request.id;

        let response = self
            .post_http(mcp_id, endpoint, headers, &serde_json::to_value(&request)?)
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("HTTP {}", response.status());
        }

        let is_event_stream = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"));

        let json_response = if is_event_stream {
            read_sse_response(response, request_id).await?
        } else {
            response
                .json::<JsonRpcResponse>()
                .await
                .map_err(|e| e.without_url())?
        };

        into_result(json_response)
    }

    /// Send a JSON-RPC request via stdio
    async fn send_jsonrpc_stdio(
        &self,
        process: &Arc<Mutex<StdioProcess>>,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> anyhow::Result<serde_json::Value> {
        let request = JsonRpcRequest::new(self.next_request_id(), method, params);
        let request_id = request.id;
        let request_json = serde_json::to_string(&request)?;

        let mut proc = process.lock().await;

        // Write request to stdin
        proc.stdin.write_all(request_json.as_bytes()).await?;
        proc.stdin.write_all(b"\n").await?;
        proc.stdin.flush().await?;

        // Read lines until the response to this request. Servers may emit
        // notifications (e.g. logging) or their own requests first.
        let mut stdout = proc.stdout_lines.lock().await;
        let deadline = tokio::time::Instant::now() + MCP_REQUEST_TIMEOUT;
        let mut line = String::new();

        loop {
            line.clear();
            match tokio::time::timeout_at(deadline, stdout.read_line(&mut line)).await {
                Ok(Ok(0)) => anyhow::bail!("MCP process closed stdout"),
                Ok(Ok(_)) => {
                    if let Some(response) = parse_matching_response(&line, request_id) {
                        return into_result(response);
                    }
                }
                Ok(Err(e)) => anyhow::bail!("Read error: {}", e),
                Err(_) => anyhow::bail!("Timeout waiting for MCP response"),
            }
        }
    }

    /// Spawn a stdio MCP process
    async fn spawn_stdio_process(
        &self,
        id: Uuid,
        command: &str,
        args: &[String],
        env: &HashMap<String, String>,
    ) -> anyhow::Result<StdioProcess> {
        let mut cmd = Command::new(command);
        cmd.args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Add environment variables
        for (key, value) in env {
            cmd.env(key, value);
        }

        let mut child = cmd.spawn()?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stdout"))?;

        let stdout_lines = Arc::new(Mutex::new(BufReader::new(stdout)));

        // Drain stderr into the log buffer. Left unread, a chatty server
        // fills the pipe and blocks.
        if let Some(stderr) = child.stderr.take() {
            let logs = Arc::clone(&self.logs);
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr);
                let mut line = Vec::new();
                loop {
                    line.clear();
                    match reader.read_until(b'\n', &mut line).await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => logs.push(id, McpLogKind::Stderr, &String::from_utf8_lossy(&line)),
                    }
                }
            });
        }

        Ok(StdioProcess {
            child,
            stdin,
            stdout_lines,
        })
    }

    /// Initialize connection with an MCP server (HTTP)
    async fn initialize_mcp_http(
        &self,
        mcp_id: Uuid,
        endpoint: &str,
        headers: &HashMap<String, String>,
    ) -> anyhow::Result<InitializeResult> {
        let params = InitializeParams {
            protocol_version: MCP_PROTOCOL_VERSION.to_string(),
            capabilities: ClientCapabilities::default(),
            client_info: ClientInfo {
                name: "open-agent".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
        };

        // A new initialize starts a new session.
        self.http_sessions.write().await.remove(&mcp_id);

        let result = self
            .send_jsonrpc_http(
                mcp_id,
                endpoint,
                "initialize",
                Some(serde_json::to_value(params)?),
                headers,
            )
            .await?;

        let init_result: InitializeResult = serde_json::from_value(result)?;

        // Send initialized notification (no response expected, but some servers require it)
        let _ = self
            .post_http(
                mcp_id,
                endpoint,
                headers,
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "notifications/initialized"
                }),
            )
            .await;

        Ok(init_result)
    }

    /// Initialize connection with an MCP server (stdio)
    async fn initialize_mcp_stdio(
        &self,
        process: &Arc<Mutex<StdioProcess>>,
    ) -> anyhow::Result<InitializeResult> {
        let params = InitializeParams {
            protocol_version: MCP_PROTOCOL_VERSION.to_string(),
            capabilities: ClientCapabilities::default(),
            client_info: ClientInfo {
                name: "open-agent".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
        };

        let result = self
            .send_jsonrpc_stdio(process, "initialize", Some(serde_json::to_value(params)?))
            .await?;

        let init_result: InitializeResult = serde_json::from_value(result)?;

        // Send initialized notification
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        });
        let notification_json = serde_json::to_string(&notification)?;

        let mut proc = process.lock().await;
        let _ = proc.stdin.write_all(notification_json.as_bytes()).await;
        let _ = proc.stdin.write_all(b"\n").await;
        let _ = proc.stdin.flush().await;

        Ok(init_result)
    }

    /// List all MCP servers with their current state.
    pub async fn list(&self) -> Vec<McpServerState> {
        self.states.read().await.values().cloned().collect()
    }

    /// Get a specific MCP server state.
    pub async fn get(&self, id: Uuid) -> Option<McpServerState> {
        self.states.read().await.get(&id).cloned()
    }

    /// Add a new MCP server.
    /// Note: This does NOT automatically attempt to connect. Use refresh() after adding.
    pub async fn add(&self, req: AddMcpRequest) -> anyhow::Result<McpServerState> {
        let mut config = match &req.transport {
            McpTransport::Http { endpoint, .. } => {
                McpServerConfig::new(req.name.clone(), endpoint.clone())
            }
            McpTransport::Stdio { command, args, env } => McpServerConfig::new_stdio(
                req.name.clone(),
                command.clone(),
                args.clone(),
                env.clone(),
            ),
        };
        config.description = req.description;
        if let Some(scope) = req.scope {
            config.scope = scope;
        }
        if let Some(default_enabled) = req.default_enabled {
            config.default_enabled = default_enabled;
        }
        config.workspace_env_allowlist = req.workspace_env_allowlist;

        // Save to persistent store
        let config = self.config_store.add(config).await?;
        let id = config.id;

        // Create runtime state
        self.logs.register(id, &config.transport);
        let state = McpServerState::from_config(config.clone());

        // Add to states
        {
            let mut states = self.states.write().await;
            states.insert(id, state.clone());
        }

        // Return immediately - user should call refresh() to connect
        Ok(state)
    }

    /// Remove an MCP server.
    pub async fn remove(&self, id: Uuid) -> anyhow::Result<()> {
        // Kill stdio process if running
        {
            let mut processes = self.stdio_processes.write().await;
            if let Some(process) = processes.remove(&id) {
                let mut proc = process.lock().await;
                let _ = proc.child.kill().await;
            }
        }

        // Remove from persistent store
        self.config_store.remove(id).await?;

        // Remove from states
        self.states.write().await.remove(&id);
        self.http_sessions.write().await.remove(&id);
        self.logs.remove(id);

        Ok(())
    }

    /// Enable an MCP server.
    /// Note: This does NOT automatically attempt to connect. Use refresh() after enabling.
    pub async fn enable(&self, id: Uuid) -> anyhow::Result<McpServerState> {
        // Update persistent config
        let config = self.config_store.enable(id).await?;

        // Update runtime state
        {
            let mut states = self.states.write().await;
            if let Some(state) = states.get_mut(&id) {
                state.config = config;
                state.status = McpStatus::Disconnected;
                state.error = None;
            }
        }

        self.get(id)
            .await
            .ok_or_else(|| anyhow::anyhow!("MCP not found"))
    }

    /// Disable an MCP server.
    pub async fn disable(&self, id: Uuid) -> anyhow::Result<McpServerState> {
        // Kill stdio process if running
        {
            let mut processes = self.stdio_processes.write().await;
            if let Some(process) = processes.remove(&id) {
                let mut proc = process.lock().await;
                let _ = proc.child.kill().await;
            }
        }

        // Update persistent config
        let config = self.config_store.disable(id).await?;

        // Update runtime state
        {
            let mut states = self.states.write().await;
            if let Some(state) = states.get_mut(&id) {
                state.config = config;
                state.status = McpStatus::Disabled;
                state.error = None;
            }
        }
        self.http_sessions.write().await.remove(&id);
        self.logs.event(id, "Disabled");

        self.get(id)
            .await
            .ok_or_else(|| anyhow::anyhow!("MCP not found"))
    }

    /// Update an MCP server configuration (name, description, transport/env).
    /// Note: If transport changes, a refresh is recommended.
    pub async fn update(
        &self,
        id: Uuid,
        req: super::types::UpdateMcpRequest,
    ) -> anyhow::Result<McpServerState> {
        // Kill existing stdio process if transport might change
        if req.transport.is_some() {
            let mut processes = self.stdio_processes.write().await;
            if let Some(process) = processes.remove(&id) {
                let mut proc = process.lock().await;
                let _ = proc.child.kill().await;
            }
        }

        // Update persistent config
        let config = self
            .config_store
            .update(id, |c| {
                if let Some(name) = &req.name {
                    c.name = name.clone();
                }
                if let Some(description) = &req.description {
                    c.description = Some(description.clone());
                }
                if let Some(enabled) = req.enabled {
                    c.enabled = enabled;
                }
                if let Some(scope) = req.scope {
                    c.scope = scope;
                }
                if let Some(transport) = &req.transport {
                    c.transport = transport.clone();
                }
                if let Some(default_enabled) = req.default_enabled {
                    c.default_enabled = default_enabled;
                }
                if let Some(workspace_env_allowlist) = &req.workspace_env_allowlist {
                    c.workspace_env_allowlist = workspace_env_allowlist.clone();
                }
            })
            .await?;

        // Update runtime state
        self.logs.register(id, &config.transport);
        {
            let mut states = self.states.write().await;
            if let Some(state) = states.get_mut(&id) {
                state.config = config;
                // Reset status if transport changed
                if req.transport.is_some() {
                    state.status = if state.config.enabled {
                        McpStatus::Disconnected
                    } else {
                        McpStatus::Disabled
                    };
                    state.error = None;
                }
            }
        }

        self.get(id)
            .await
            .ok_or_else(|| anyhow::anyhow!("MCP not found"))
    }

    /// Helper to update state with error - retries a few times to handle lock contention
    async fn update_state_error(&self, id: Uuid, error_msg: String) {
        // Server-supplied error text can echo configured credentials.
        let error_msg = self.logs.redact(id, &error_msg);
        self.logs.event(id, &format!("Error: {}", error_msg));
        // Try up to 5 times with small delays to handle temporary lock contention
        for attempt in 0..5 {
            if let Ok(mut states) = self.states.try_write() {
                if let Some(state) = states.get_mut(&id) {
                    state.status = McpStatus::Error;
                    state.error = Some(error_msg);
                }
                return;
            }
            // Small delay before retry (10ms, 20ms, 40ms, 80ms, 160ms)
            if attempt < 4 {
                tokio::time::sleep(Duration::from_millis(10 << attempt)).await;
            }
        }
        // If still can't get lock after retries, log warning
        tracing::warn!("Failed to update MCP {} error state after retries", id);
    }

    /// Helper to update state with success - retries a few times to handle lock contention
    async fn update_state_success(
        &self,
        id: Uuid,
        tool_descriptors: Vec<McpToolDescriptor>,
        server_version: Option<String>,
    ) {
        let tool_names: Vec<String> = tool_descriptors.iter().map(|t| t.name.clone()).collect();
        self.logs.event(
            id,
            &format!(
                "Connected: {} tool(s){}",
                tool_names.len(),
                server_version
                    .as_deref()
                    .map(|v| format!(", server version {}", v))
                    .unwrap_or_default()
            ),
        );

        // Try up to 5 times with small delays to handle temporary lock contention
        for attempt in 0..5 {
            if let Ok(mut states) = self.states.try_write() {
                if let Some(state) = states.get_mut(&id) {
                    state.config.tools = tool_names.clone();
                    state.config.tool_descriptors = tool_descriptors.clone();
                    state.config.version = server_version.clone();
                    state.config.last_connected_at = Some(chrono::Utc::now());
                    state.status = McpStatus::Connected;
                    state.error = None;
                }
                return;
            }
            // Small delay before retry
            if attempt < 4 {
                tokio::time::sleep(Duration::from_millis(10 << attempt)).await;
            }
        }
        // If still can't get lock after retries, log warning
        tracing::warn!("Failed to update MCP {} success state after retries", id);
    }

    /// Refresh an MCP server - reconnect and discover tools.
    pub async fn refresh(&self, id: Uuid) -> anyhow::Result<McpServerState> {
        let state = self
            .get(id)
            .await
            .ok_or_else(|| anyhow::anyhow!("MCP not found"))?;

        if !state.config.enabled {
            return Ok(state);
        }

        self.logs.event(
            id,
            &format!("Refreshing ({})", transport_label(&state.config.transport)),
        );

        match &state.config.transport {
            McpTransport::Http { endpoint, headers } => {
                self.refresh_http(id, endpoint.clone(), headers.clone())
                    .await
            }
            McpTransport::Stdio { command, args, env } => {
                self.refresh_stdio(id, command.clone(), args.clone(), env.clone())
                    .await
            }
        }
    }

    /// Refresh an HTTP MCP server
    async fn refresh_http(
        &self,
        id: Uuid,
        endpoint: String,
        headers: HashMap<String, String>,
    ) -> anyhow::Result<McpServerState> {
        let endpoint = endpoint.trim_end_matches('/').to_string();

        // Step 1: Initialize the MCP connection with JSON-RPC
        let init_result = match self.initialize_mcp_http(id, &endpoint, &headers).await {
            Ok(result) => result,
            Err(e) => {
                self.update_state_error(id, format!("Initialize failed: {}", e))
                    .await;
                return self
                    .get(id)
                    .await
                    .ok_or_else(|| anyhow::anyhow!("MCP not found"));
            }
        };

        // Extract server version if available
        let server_version = init_result
            .server_info
            .as_ref()
            .and_then(|s| s.version.clone());

        // Step 2: List tools using JSON-RPC
        match self
            .send_jsonrpc_http(id, &endpoint, "tools/list", None, &headers)
            .await
        {
            Ok(result) => {
                match serde_json::from_value::<McpToolsResponse>(result) {
                    Ok(tools_response) => {
                        let tool_descriptors = tools_response.tools;
                        let tool_names: Vec<String> =
                            tool_descriptors.iter().map(|t| t.name.clone()).collect();

                        // Update config with discovered tools
                        let _ = self
                            .config_store
                            .update(id, |c| {
                                c.tools = tool_names.clone();
                                c.tool_descriptors = tool_descriptors.clone();
                                c.version = server_version.clone();
                                c.last_connected_at = Some(chrono::Utc::now());
                            })
                            .await;

                        // Update runtime state
                        self.update_state_success(id, tool_descriptors, server_version)
                            .await;
                    }
                    Err(e) => {
                        self.update_state_error(id, format!("Failed to parse tools: {}", e))
                            .await;
                    }
                }
            }
            Err(e) => {
                self.update_state_error(id, format!("tools/list failed: {}", e))
                    .await;
            }
        }

        self.get(id)
            .await
            .ok_or_else(|| anyhow::anyhow!("MCP not found"))
    }

    /// Refresh a stdio MCP server
    async fn refresh_stdio(
        &self,
        id: Uuid,
        command: String,
        args: Vec<String>,
        env: HashMap<String, String>,
    ) -> anyhow::Result<McpServerState> {
        // Kill existing process if any
        {
            let mut processes = self.stdio_processes.write().await;
            if let Some(process) = processes.remove(&id) {
                let mut proc = process.lock().await;
                let _ = proc.child.kill().await;
            }
        }

        // Spawn new process
        let process = match self.spawn_stdio_process(id, &command, &args, &env).await {
            Ok(p) => Arc::new(Mutex::new(p)),
            Err(e) => {
                self.update_state_error(id, format!("Failed to spawn process: {}", e))
                    .await;
                return self
                    .get(id)
                    .await
                    .ok_or_else(|| anyhow::anyhow!("MCP not found"));
            }
        };

        // Store process handle
        {
            let mut processes = self.stdio_processes.write().await;
            processes.insert(id, Arc::clone(&process));
        }

        // Step 1: Initialize the MCP connection
        let init_result = match self.initialize_mcp_stdio(&process).await {
            Ok(result) => result,
            Err(e) => {
                self.update_state_error(id, format!("Initialize failed: {}", e))
                    .await;
                // Clean up process
                let mut processes = self.stdio_processes.write().await;
                if let Some(process) = processes.remove(&id) {
                    let mut proc = process.lock().await;
                    let _ = proc.child.kill().await;
                }
                return self
                    .get(id)
                    .await
                    .ok_or_else(|| anyhow::anyhow!("MCP not found"));
            }
        };

        // Extract server version if available
        let server_version = init_result
            .server_info
            .as_ref()
            .and_then(|s| s.version.clone());

        // Step 2: List tools
        match self.send_jsonrpc_stdio(&process, "tools/list", None).await {
            Ok(result) => {
                match serde_json::from_value::<McpToolsResponse>(result) {
                    Ok(tools_response) => {
                        let tool_descriptors = tools_response.tools;
                        let tool_names: Vec<String> =
                            tool_descriptors.iter().map(|t| t.name.clone()).collect();

                        // Update config with discovered tools
                        let _ = self
                            .config_store
                            .update(id, |c| {
                                c.tools = tool_names.clone();
                                c.tool_descriptors = tool_descriptors.clone();
                                c.version = server_version.clone();
                                c.last_connected_at = Some(chrono::Utc::now());
                            })
                            .await;

                        // Update runtime state
                        self.update_state_success(id, tool_descriptors, server_version)
                            .await;
                    }
                    Err(e) => {
                        self.update_state_error(id, format!("Failed to parse tools: {}", e))
                            .await;
                    }
                }
            }
            Err(e) => {
                self.update_state_error(id, format!("tools/list failed: {}", e))
                    .await;
            }
        }

        self.get(id)
            .await
            .ok_or_else(|| anyhow::anyhow!("MCP not found"))
    }

    /// Refresh all MCP servers concurrently.
    /// When `skip_workspace` is true, workspace-scoped MCPs (e.g. orchestrator)
    /// are skipped because they require per-mission context unavailable at startup.
    pub async fn refresh_all(&self, skip_workspace: bool) {
        let states = self.states.read().await;
        let ids: Vec<Uuid> = states
            .iter()
            .filter(|(_, state)| !skip_workspace || state.config.scope != McpScope::Workspace)
            .map(|(id, _)| *id)
            .collect();
        drop(states);

        // Refresh all MCPs concurrently using join_all
        let futures: Vec<_> = ids.iter().map(|id| self.refresh(*id)).collect();
        futures::future::join_all(futures).await;
    }

    /// Call a tool on an MCP server.
    pub async fn call_tool(
        &self,
        mcp_id: Uuid,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> anyhow::Result<String> {
        // Check if tool is disabled
        if self.disabled_tools.read().await.contains(tool_name) {
            anyhow::bail!("Tool {} is disabled", tool_name);
        }

        let state = self
            .get(mcp_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("MCP not found"))?;

        if !state.config.enabled {
            anyhow::bail!("MCP {} is disabled", state.config.name);
        }

        if state.status != McpStatus::Connected {
            anyhow::bail!("MCP {} is not connected", state.config.name);
        }

        let params = serde_json::json!({
            "name": tool_name,
            "arguments": arguments
        });

        let result = match &state.config.transport {
            McpTransport::Http { endpoint, headers } => {
                let endpoint = endpoint.trim_end_matches('/');
                self.send_jsonrpc_http(mcp_id, endpoint, "tools/call", Some(params), headers)
                    .await
            }
            McpTransport::Stdio { .. } => {
                let processes = self.stdio_processes.read().await;
                let process = processes
                    .get(&mcp_id)
                    .ok_or_else(|| anyhow::anyhow!("No stdio process for MCP {}", mcp_id))?;
                self.send_jsonrpc_stdio(process, "tools/call", Some(params))
                    .await
            }
        };

        match result {
            Ok(result) => {
                let response: McpCallToolResponse = serde_json::from_value(result)?;

                // Increment counters
                {
                    let mut states = self.states.write().await;
                    if let Some(state) = states.get_mut(&mcp_id) {
                        if response.is_error {
                            state.tool_errors += 1;
                        } else {
                            state.tool_calls += 1;
                        }
                    }
                }

                if response.is_error {
                    let error_text = response
                        .content
                        .iter()
                        .filter_map(|c| c.text.as_deref())
                        .collect::<Vec<_>>()
                        .join("\n");
                    anyhow::bail!("Tool error: {}", error_text);
                }

                // Combine text content
                let output = response
                    .content
                    .iter()
                    .filter_map(|c| c.text.as_deref())
                    .collect::<Vec<_>>()
                    .join("\n");

                Ok(output)
            }
            Err(e) => {
                // Increment error counter
                {
                    let mut states = self.states.write().await;
                    if let Some(state) = states.get_mut(&mcp_id) {
                        state.tool_errors += 1;
                    }
                }
                self.logs
                    .event(mcp_id, &format!("tools/call {} failed: {}", tool_name, e));
                anyhow::bail!("Tool call failed: {}", e);
            }
        }
    }

    /// List all tools from all connected MCPs.
    ///
    /// Tool names are prefixed with the MCP server name to avoid conflicts
    /// with built-in tools (e.g., `filesystem_read_file` instead of `read_file`).
    pub async fn list_tools(&self) -> Vec<McpTool> {
        let states = self.states.read().await;
        let disabled = self.disabled_tools.read().await;

        let mut tools = Vec::new();
        for state in states.values() {
            if state.config.enabled && state.status == McpStatus::Connected {
                // Derive prefix from MCP server name (sanitized for function names)
                let prefix = sanitize_mcp_prefix(&state.config.name);

                for descriptor in &state.config.tool_descriptors {
                    // Prefix tool name with MCP server name to avoid conflicts
                    let prefixed_name = format!("{}_{}", prefix, descriptor.name);
                    let prefixed_description =
                        format!("[{}] {}", state.config.name, descriptor.description);

                    tools.push(McpTool {
                        name: prefixed_name.clone(),
                        description: prefixed_description,
                        parameters_schema: descriptor.input_schema.clone(),
                        mcp_id: state.config.id,
                        enabled: !disabled.contains(&descriptor.name)
                            && !disabled.contains(&prefixed_name),
                    });
                }
            }
        }
        tools
    }

    /// Enable a tool.
    pub async fn enable_tool(&self, name: &str) {
        self.disabled_tools.write().await.remove(name);
    }

    /// Disable a tool.
    pub async fn disable_tool(&self, name: &str) {
        self.disabled_tools.write().await.insert(name.to_string());
    }

    /// Check if a tool is enabled.
    pub async fn is_tool_enabled(&self, name: &str) -> bool {
        !self.disabled_tools.read().await.contains(name)
    }

    /// Find a tool by name (prefixed) and return its MCP ID if found.
    ///
    /// Tool names should be in prefixed format (e.g., `filesystem_read_file`).
    pub async fn find_tool(&self, name: &str) -> Option<McpTool> {
        let states = self.states.read().await;
        let disabled = self.disabled_tools.read().await;

        for state in states.values() {
            if state.config.enabled && state.status == McpStatus::Connected {
                let prefix = sanitize_mcp_prefix(&state.config.name);

                for descriptor in &state.config.tool_descriptors {
                    let prefixed_name = format!("{}_{}", prefix, descriptor.name);

                    if prefixed_name == name
                        && !disabled.contains(&descriptor.name)
                        && !disabled.contains(&prefixed_name)
                    {
                        return Some(McpTool {
                            name: prefixed_name,
                            description: format!(
                                "[{}] {}",
                                state.config.name, descriptor.description
                            ),
                            parameters_schema: descriptor.input_schema.clone(),
                            mcp_id: state.config.id,
                            enabled: true,
                        });
                    }
                }
            }
        }
        None
    }

    /// Get the original (unprefixed) tool name for an MCP call.
    ///
    /// When calling the MCP server, we need to use the original tool name.
    pub fn strip_prefix(prefixed_name: &str) -> String {
        // Find the first underscore and return everything after it
        if let Some(idx) = prefixed_name.find('_') {
            prefixed_name[idx + 1..].to_string()
        } else {
            prefixed_name.to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_events_are_split_and_data_lines_joined() {
        let mut buffer = b"event: message\ndata: {\"a\":\ndata: 1}\n\ndata: x".to_vec();
        assert_eq!(take_sse_event(&mut buffer).as_deref(), Some("{\"a\":\n1}"));
        // An incomplete event stays buffered.
        assert_eq!(take_sse_event(&mut buffer), None);
        assert_eq!(buffer, b"data: x");
    }

    #[test]
    fn matching_response_skips_notifications_requests_and_other_ids() {
        let notification = r#"{"jsonrpc":"2.0","method":"notifications/message","params":{}}"#;
        let server_request = r#"{"jsonrpc":"2.0","id":7,"method":"roots/list"}"#;
        let other = r#"{"jsonrpc":"2.0","id":8,"result":{}}"#;
        let ours = r#"{"jsonrpc":"2.0","id":7,"result":{"ok":true}}"#;
        assert!(parse_matching_response(notification, 7).is_none());
        assert!(parse_matching_response(server_request, 7).is_none());
        assert!(parse_matching_response(other, 7).is_none());
        assert!(parse_matching_response("not json", 7).is_none());
        let found = parse_matching_response(ours, 7).expect("response");
        assert_eq!(found.result, Some(serde_json::json!({"ok": true})));
        let null_id_error =
            r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"#;
        let found = parse_matching_response(null_id_error, 7).expect("null-id error");
        assert_eq!(found.error.map(|e| e.code), Some(-32700));
        let null_id_result = r#"{"jsonrpc":"2.0","id":null,"result":{}}"#;
        assert!(parse_matching_response(null_id_result, 7).is_none());
        let batch = format!("[{notification},{ours}]");
        assert!(parse_matching_response(&batch, 7).is_some());
    }

    #[test]
    fn transport_label_omits_paths_queries_args_and_env() {
        let http = McpTransport::Http {
            endpoint: "https://mcp.example.com:8443/v1/mcp?token=secret".to_string(),
            headers: HashMap::from([("Authorization".to_string(), "Bearer x".to_string())]),
        };
        assert_eq!(transport_label(&http), "http https://mcp.example.com:8443");
        let stdio = McpTransport::Stdio {
            command: "/usr/bin/npx".to_string(),
            args: vec!["--token=secret".to_string()],
            env: HashMap::from([("API_KEY".to_string(), "secret".to_string())]),
        };
        assert_eq!(transport_label(&stdio), "stdio npx");
    }

    async fn add_and_refresh(registry: &McpRegistry, transport: McpTransport) -> McpServerState {
        let added = registry
            .add(AddMcpRequest {
                name: "probe-test".to_string(),
                transport,
                description: None,
                scope: None,
                default_enabled: None,
                workspace_env_allowlist: Vec::new(),
            })
            .await
            .expect("add");
        registry.refresh(added.config.id).await.expect("refresh")
    }

    /// A Streamable HTTP server: rejects requests that don't accept SSE,
    /// assigns a session at initialize, requires it afterwards, and answers
    /// over SSE with a notification ahead of the response.
    #[tokio::test]
    async fn refresh_connects_to_streamable_http_server() {
        use axum::http::{HeaderMap, StatusCode};
        use axum::response::{IntoResponse, Response};

        async fn handler(headers: HeaderMap, body: String) -> Response {
            let accept = headers
                .get("accept")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if !(accept.contains("application/json") && accept.contains("text/event-stream")) {
                return StatusCode::NOT_ACCEPTABLE.into_response();
            }
            let message: serde_json::Value = serde_json::from_str(&body).unwrap();
            let method = message["method"].as_str().unwrap_or("");
            let session = headers.get("mcp-session-id").and_then(|v| v.to_str().ok());
            if method != "initialize" && session != Some("session-1") {
                return StatusCode::BAD_REQUEST.into_response();
            }
            let result = match method {
                "initialize" => serde_json::json!({
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "serverInfo": {"name": "test", "version": "1.2.3"}
                }),
                "tools/list" => serde_json::json!({"tools": [{"name": "echo", "inputSchema": {}}]}),
                _ => return StatusCode::ACCEPTED.into_response(),
            };
            let response =
                serde_json::json!({"jsonrpc": "2.0", "id": message["id"], "result": result});
            let body = format!(
                "event: message\r\ndata: {{\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{{}}}}\r\n\r\nevent: message\r\ndata: {response}\r\n\r\n"
            );
            (
                [
                    ("content-type", "text/event-stream"),
                    ("mcp-session-id", "session-1"),
                ],
                body,
            )
                .into_response()
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = axum::Router::new().route("/mcp", axum::routing::post(handler));
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let dir = tempfile::tempdir().unwrap();
        let registry = McpRegistry::new(dir.path()).await;
        let state = add_and_refresh(
            &registry,
            McpTransport::Http {
                endpoint: format!("http://{addr}/mcp"),
                headers: HashMap::new(),
            },
        )
        .await;

        assert_eq!(
            state.status,
            McpStatus::Connected,
            "error: {:?}",
            state.error
        );
        assert_eq!(state.config.tools, ["echo"]);
        assert_eq!(state.config.version.as_deref(), Some("1.2.3"));
        let logs = registry.logs(state.config.id, 10);
        let messages: Vec<_> = logs.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(
            messages,
            [
                format!("Refreshing (http http://{addr})").as_str(),
                "Connected: 1 tool(s), server version 1.2.3"
            ]
        );
    }

    /// A stdio server that writes to stderr and sends a notification before
    /// each initialize response.
    #[cfg(unix)]
    #[tokio::test]
    async fn refresh_skips_stdio_notifications_and_captures_stderr() {
        let script = r#"echo "booting" >&2
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  [ -z "$id" ] && continue
  case "$line" in
    *'"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info","data":"hi"}}'
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"t","version":"9"}}}\n' "$id" ;;
    *'"tools/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"echo","inputSchema":{}}]}}\n' "$id" ;;
  esac
done
"#;
        let dir = tempfile::tempdir().unwrap();
        let registry = McpRegistry::new(dir.path()).await;
        let state = add_and_refresh(
            &registry,
            McpTransport::Stdio {
                command: "sh".to_string(),
                args: vec!["-c".to_string(), script.to_string()],
                env: HashMap::new(),
            },
        )
        .await;

        assert_eq!(
            state.status,
            McpStatus::Connected,
            "error: {:?}",
            state.error
        );
        assert_eq!(state.config.tools, ["echo"]);

        // The stderr reader runs concurrently; give it a moment.
        let id = state.config.id;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !registry
            .logs(id, 50)
            .iter()
            .any(|e| e.kind == McpLogKind::Stderr && e.message == "booting")
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "stderr not captured"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(registry
            .logs(id, 50)
            .iter()
            .any(|e| e.message == "Refreshing (stdio sh)"));

        registry.remove(id).await.unwrap();
        assert!(registry.logs(id, 50).is_empty());
    }

    #[test]
    fn builtin_workspace_mcp_receives_full_workspace_environment() {
        let mut config = McpServerConfig::new_stdio(
            "workspace".to_string(),
            "/usr/local/bin/workspace-mcp".to_string(),
            Vec::new(),
            HashMap::new(),
        );

        assert!(reconcile_workspace_mcp_env_trust(
            &mut config,
            Path::new("/srv/sandboxed")
        ));
        assert_eq!(config.workspace_env_allowlist, ["*"]);
        assert!(!reconcile_workspace_mcp_env_trust(
            &mut config,
            Path::new("/srv/sandboxed")
        ));
    }

    #[test]
    fn repointed_workspace_mcp_loses_implicit_wildcard_grant() {
        let mut config = McpServerConfig::new_stdio(
            "workspace".to_string(),
            "/opt/third-party/server".to_string(),
            Vec::new(),
            HashMap::new(),
        );
        config.workspace_env_allowlist = vec!["PATH".to_string(), "*".to_string()];

        assert!(reconcile_workspace_mcp_env_trust(
            &mut config,
            Path::new("/srv/sandboxed")
        ));
        assert_eq!(config.workspace_env_allowlist, ["PATH"]);
        assert!(!reconcile_workspace_mcp_env_trust(
            &mut config,
            Path::new("/srv/sandboxed")
        ));
    }

    #[test]
    fn same_basename_at_an_unknown_path_is_not_trusted() {
        let mut config = McpServerConfig::new_stdio(
            "workspace".to_string(),
            "/opt/third-party/workspace-mcp".to_string(),
            Vec::new(),
            HashMap::new(),
        );
        config.workspace_env_allowlist = vec!["*".to_string()];

        assert!(reconcile_workspace_mcp_env_trust(
            &mut config,
            Path::new("/srv/sandboxed")
        ));
        assert!(config.workspace_env_allowlist.is_empty());
    }
}
