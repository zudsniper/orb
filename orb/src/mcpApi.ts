// MCP servers a workspace's missions get, and Core's per-server logs.
// Newer endpoints are feature-detected: a 404 means "backend too old", not an error.
import { ApiError, api } from "./api";

const enc = encodeURIComponent;

/** The server's own bookkeeping workspace: missions that run on Core itself. */
export const HOST_WORKSPACE_ID = "00000000-0000-0000-0000-000000000000";

export type McpStatus = "connected" | "disconnected" | "error" | "disabled";

/** Core's secret-free view of an MCP (`GET /api/workspaces/:id/mcps`). */
export interface McpSummary {
  id: string;
  name: string;
  transport: "http" | "stdio";
  scope: "global" | "workspace";
  description?: string | null;
  default_enabled: boolean;
  version?: string | null;
  tools: string[];
  last_connected_at?: string | null;
  status: McpStatus;
  error?: string | null;
  tool_calls: number;
  tool_errors: number;
}

export interface McpLogEntry { at: string; kind: "stderr" | "event"; message: string }

export interface WorkspaceRef { id: string; name: string }

export class UnsupportedEndpoint extends Error {}

const isNotFound = (e: unknown) => e instanceof ApiError && e.status === 404;
const tooOld = "Core doesn’t support this yet. Update Core to see MCP servers here.";

export async function listWorkspaces(): Promise<WorkspaceRef[]> {
  const all = await api<WorkspaceRef[]>("/api/workspaces");
  return all.map(w => ({ id: w.id, name: w.name }));
}

export async function workspaceMcps(workspaceId: string): Promise<McpSummary[]> {
  try {
    return await api<McpSummary[]>(`/api/workspaces/${enc(workspaceId)}/mcps`);
  } catch (e) {
    // The workspace itself exists (it came from the list), so a 404 is a missing route.
    if (isNotFound(e)) throw new UnsupportedEndpoint(tooOld);
    throw e;
  }
}

export async function mcpLogs(id: string, tail = 300): Promise<McpLogEntry[]> {
  try {
    return (await api<{ entries: McpLogEntry[] }>(`/api/mcp/${enc(id)}/logs?tail=${tail}`)).entries;
  } catch (e) {
    if (isNotFound(e)) throw new UnsupportedEndpoint("Core doesn’t keep MCP logs yet.");
    throw e;
  }
}

const isCheckStart = (e: McpLogEntry) => e.kind === "event" && e.message.startsWith("Refreshing (");
const isCheckEnd = (e: McpLogEntry) => e.kind === "event" && (e.message.startsWith("Connected:") || e.message.startsWith("Error:"));
const lastCheckStart = (entries: McpLogEntry[]) => {
  for (let i = entries.length - 1; i >= 0; i--) if (isCheckStart(entries[i])) return i;
  return -1;
};

export interface RefreshOptions { timeoutMs?: number; intervalMs?: number }

/** Reconnect and rediscover tools. Core runs the check in the background, so
 * this waits until the log records its outcome. Resolves `false` if the check
 * is still running at the timeout, or Core keeps no logs to tell. */
export async function refreshMcp(id: string, { timeoutMs = 60_000, intervalMs = 500 }: RefreshOptions = {}): Promise<boolean> {
  let previous: string | undefined;
  try {
    const before = await mcpLogs(id);
    previous = before[lastCheckStart(before)]?.at;
  } catch (e) {
    if (!(e instanceof UnsupportedEndpoint)) throw e;
    await api<unknown>(`/api/mcp/${enc(id)}/refresh`, { method: "POST" });
    return false;
  }
  await api<unknown>(`/api/mcp/${enc(id)}/refresh`, { method: "POST" });
  for (const deadline = Date.now() + timeoutMs; Date.now() < deadline;) {
    await new Promise(resolve => setTimeout(resolve, intervalMs));
    const entries = await mcpLogs(id);
    const start = lastCheckStart(entries);
    if (start >= 0 && entries[start].at !== previous && entries.slice(start + 1).some(isCheckEnd)) return true;
  }
  return false;
}

export type McpStatusView = { label: string; dot: "on" | "off" | "idle" };

/** Status as shown to the user. Workspace-scoped servers (e.g. the built-in
 * sandboxed MCP) are only started inside missions, so Core never connects them. */
export function mcpStatusView(m: Pick<McpSummary, "status" | "scope">): McpStatusView {
  switch (m.status) {
    case "connected": return { label: "Connected", dot: "on" };
    case "error": return { label: "Error", dot: "off" };
    case "disabled": return { label: "Disabled", dot: "idle" };
    default: return m.scope === "workspace"
      ? { label: "Starts with each mission", dot: "idle" }
      : { label: "Not checked yet", dot: "idle" };
  }
}

/** One log line: UTC time to the second, source, message. */
export const mcpLogLine = (e: McpLogEntry) => `${e.at.replace(/\.\d+Z$/, "Z")} ${e.kind === "stderr" ? "stderr" : "core  "} ${e.message}`;
