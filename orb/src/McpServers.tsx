import { For, Index, Show, createResource, createSignal } from "solid-js";
import { copyText } from "./clipboard";
import { Select } from "./Select";
import * as Ic from "./icons";
import {
  HOST_WORKSPACE_ID, listWorkspaces, mcpLogLine, mcpLogs, mcpStatusView, refreshMcp, workspaceMcps,
  type McpSummary, type WorkspaceRef,
} from "./mcpApi";
import "./mcpServers.css";

const message = (e: unknown) => (e instanceof Error ? e.message : String(e));
const plural = (n: number, one: string) => `${n} ${one}${n === 1 ? "" : "s"}`;
const when = (at?: string | null) => (at ? new Date(at).toLocaleString() : "Never");

async function workspaceChoices(): Promise<WorkspaceRef[]> {
  try {
    const all = await listWorkspaces();
    return all.some(w => w.id === HOST_WORKSPACE_ID) ? all : [{ id: HOST_WORKSPACE_ID, name: "host" }, ...all];
  } catch {
    return [{ id: HOST_WORKSPACE_ID, name: "host" }];
  }
}

function McpLogView(p: { id: string; version: () => number }) {
  const [entries, { refetch }] = createResource(() => [p.id, p.version()] as const, ([id]) => mcpLogs(id));
  const [note, setNote] = createSignal<string | null>(null);
  const text = () => (entries() ?? []).map(mcpLogLine).join("\n");
  const copy = async () => {
    try { await copyText(text()); setNote("Copied"); } catch (e) { setNote(`Couldn’t copy: ${message(e)}`); }
  };
  return (
    <>
      <div class="debug-info-actions">
        <button class="s-btn sm" onClick={() => { setNote(null); void refetch(); }}>Reload log</button>
        <button class="s-btn sm" disabled={!entries()?.length} onClick={() => void copy()}>Copy log</button>
        <Show when={note()}><span class="s-row-desc" role="status">{note()}</span></Show>
      </div>
      <Show when={!entries.error} fallback={<div class="s-row-desc" role="alert">{message(entries.error)}</div>}>
        <pre class="debug-info-log" aria-label="MCP server log">
          <Show when={entries()} fallback="Loading…">
            {list => <Show when={list().length} fallback="No log entries yet. Refresh to run Core’s check.">{text()}</Show>}
          </Show>
        </pre>
      </Show>
    </>
  );
}

/** Rendered with `Index`, so a refreshed list updates the row in place and it stays open. */
function McpRow(p: { mcp: () => McpSummary; onRefreshed: () => void }) {
  const [open, setOpen] = createSignal(false);
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  const [logVersion, setLogVersion] = createSignal(0);
  const status = () => mcpStatusView(p.mcp());
  const refresh = async () => {
    if (busy()) return;
    setBusy(true); setError(null);
    try { await refreshMcp(p.mcp().id); p.onRefreshed(); setLogVersion(v => v + 1); }
    catch (e) { setError(message(e)); }
    finally { setBusy(false); }
  };
  return (
    <details class="mcp-row" onToggle={e => { if (e.currentTarget.open) setOpen(true); }}>
      <summary class="s-row">
        <span class={`m-dot ${status().dot === "idle" ? "" : status().dot}`} aria-hidden="true" />
        <span class="s-row-text">
          <span class="s-row-title">{p.mcp().name}</span>
          <span class="s-row-desc">{p.mcp().transport} · {plural(p.mcp().tools.length, "tool")} · {status().label}</span>
          <Show when={p.mcp().status === "error" && p.mcp().error}>
            <span class="s-row-desc mcp-error">{p.mcp().error}</span>
          </Show>
        </span>
        <Ic.ChevronDown size={14} />
      </summary>
      <Show when={open()}>
        <div class="debug-info mcp-detail" data-testid={`mcp-${p.mcp().name}`}>
          <Show when={p.mcp().description}><div class="s-row-desc">{p.mcp().description}</div></Show>
          <dl class="debug-info-facts">
            <dt>Tools</dt><dd>{p.mcp().tools.length ? p.mcp().tools.join(", ") : "None discovered"}</dd>
            <dt>Calls</dt><dd>{p.mcp().tool_calls} ok · {p.mcp().tool_errors} failed (through Core)</dd>
            <dt>Last connected</dt><dd>{when(p.mcp().last_connected_at)}</dd>
            <Show when={p.mcp().version}><dt>Version</dt><dd>{p.mcp().version}</dd></Show>
            <dt>Included</dt><dd>{p.mcp().default_enabled ? "By default" : "Added to this workspace"}</dd>
          </dl>
          <div class="debug-info-actions">
            <button class="s-btn sm" disabled={busy()} onClick={() => void refresh()}>{busy() ? "Checking…" : "Refresh"}</button>
            <Show when={error()}><span class="s-row-desc" role="alert">{error()}</span></Show>
          </div>
          <McpLogView id={p.mcp().id} version={logVersion} />
        </div>
      </Show>
    </details>
  );
}

/** Settings → MCP servers: what a workspace's missions get, with Core's status and logs. */
export function McpServersPanel() {
  const [workspaces] = createResource(workspaceChoices);
  const [workspaceId, setWorkspaceId] = createSignal(HOST_WORKSPACE_ID);
  const [mcps, { refetch }] = createResource(workspaceId, async id => ({ id, list: await workspaceMcps(id) }));
  // While another workspace loads, don't keep showing the previous one's servers.
  const current = () => { const loaded = mcps(); return loaded?.id === workspaceId() ? loaded : undefined; };
  return (
    <div class="mcp-servers" data-testid="mcp-servers">
      <div class="mcp-servers-head">
        <span class="s-row-desc">Status and logs come from Core’s own check of each server. Missions start their own copy.</span>
        <Show when={(workspaces()?.length ?? 0) > 1}>
          <Select value={workspaceId()} aria-label="Workspace" onChange={e => setWorkspaceId(e.currentTarget.value)}>
            <For each={workspaces()}>{w => <option value={w.id}>{w.name}</option>}</For>
          </Select>
        </Show>
      </div>
      <Show when={!mcps.error} fallback={<div class="s-row-desc mcp-servers-msg" role="alert">{message(mcps.error)}</div>}>
        <Show when={current()} fallback={<div class="s-row-desc mcp-servers-msg">Loading…</div>}>
          {loaded => (
            <Show when={loaded().list.length} fallback={<div class="s-row-desc mcp-servers-msg">No MCP servers apply to this workspace.</div>}>
              {/* Rows remount per workspace; within one, a refresh updates them in place. */}
              <Show when={loaded().id} keyed>
                {(_workspace: string) => <Index each={loaded().list}>{m => <McpRow mcp={m} onRefreshed={() => void refetch()} />}</Index>}
              </Show>
            </Show>
          )}
        </Show>
      </Show>
    </div>
  );
}
