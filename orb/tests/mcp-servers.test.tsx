import { fireEvent, render, screen, waitFor } from "@solidjs/testing-library";
import { afterEach, expect, it, vi } from "vitest";
import { McpServersPanel } from "../src/McpServers";
import { HOST_WORKSPACE_ID, mcpStatusView, type McpSummary } from "../src/mcpApi";
import { clearConnection, setConnection } from "../src/api";

afterEach(() => { clearConnection(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });

const summary = (over: Partial<McpSummary>): McpSummary => ({
  id: "11111111-1111-1111-1111-111111111111", name: "smoke", transport: "stdio", scope: "global",
  default_enabled: true, tools: [], status: "connected", tool_calls: 0, tool_errors: 0, ...over,
});

const json = (data: unknown, status = 200) => new Response(JSON.stringify(data), { status });

function stubCore(routes: Record<string, () => Response>) {
  const fetch = vi.fn(async (url: string, init?: RequestInit) => {
    const path = new URL(url).pathname + new URL(url).search;
    const key = `${init?.method ?? "GET"} ${path}`;
    const route = routes[key];
    return route ? route() : json({ detail: "missing" }, 404);
  });
  vi.stubGlobal("fetch", fetch);
  return fetch;
}

function openRow(name: string) {
  const details = [...document.querySelectorAll("details.mcp-row")].find(d => d.textContent?.includes(name)) as HTMLDetailsElement;
  details.open = true;
  fireEvent(details, new Event("toggle"));
}

it("maps statuses, including servers Core only starts inside missions", () => {
  expect(mcpStatusView({ status: "connected", scope: "global" })).toEqual({ label: "Connected", dot: "on" });
  expect(mcpStatusView({ status: "error", scope: "global" })).toEqual({ label: "Error", dot: "off" });
  expect(mcpStatusView({ status: "disconnected", scope: "workspace" }).label).toBe("Starts with each mission");
  expect(mcpStatusView({ status: "disconnected", scope: "global" }).label).toBe("Not checked yet");
});

it("lists the host workspace's servers with status, tools and errors", async () => {
  setConnection("http://core.test", "test-token");
  stubCore({
    "GET /api/workspaces": () => json([{ id: HOST_WORKSPACE_ID, name: "host" }]),
    [`GET /api/workspaces/${HOST_WORKSPACE_ID}/mcps`]: () => json([
      summary({ name: "docs", transport: "http", tools: ["ask", "read"] }),
      summary({ id: "2", name: "broken", status: "error", error: "Initialize failed: HTTP 406" }),
    ]),
  });
  render(() => <McpServersPanel />);
  expect(await screen.findByText("http · 2 tools · Connected")).toBeTruthy();
  expect(screen.getByText("Initialize failed: HTTP 406")).toBeTruthy();
  // A single workspace needs no picker.
  expect(screen.queryByLabelText("Workspace")).toBeNull();
});

it("shows a server's log and refreshes it", async () => {
  setConnection("http://core.test", "test-token");
  const id = "11111111-1111-1111-1111-111111111111";
  let refreshed = false;
  const fetch = stubCore({
    "GET /api/workspaces": () => json([{ id: HOST_WORKSPACE_ID, name: "host" }]),
    [`GET /api/workspaces/${HOST_WORKSPACE_ID}/mcps`]: () => json([summary({ tools: ["echo"] })]),
    [`GET /api/mcp/${id}/logs?tail=300`]: () => json({ id, entries: refreshed
      ? [{ at: "2026-10-22T04:30:00Z", kind: "event", message: "Connected: 1 tool(s)" }]
      : [{ at: "2026-10-22T04:29:00Z", kind: "stderr", message: "server booting" }] }),
    [`POST /api/mcp/${id}/refresh`]: () => { refreshed = true; return json(summary({})); },
  });
  render(() => <McpServersPanel />);
  await screen.findByText("smoke");
  openRow("smoke");
  await waitFor(() => expect(screen.getByLabelText("MCP server log").textContent).toContain("stderr server booting"));
  fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
  await waitFor(() => expect(screen.getByLabelText("MCP server log").textContent).toContain("Connected: 1 tool(s)"));
  expect(fetch.mock.calls.some(([url, init]) => url.endsWith("/refresh") && init?.method === "POST")).toBe(true);
});

it("explains when Core is too old for the endpoint", async () => {
  setConnection("http://core.test", "test-token");
  stubCore({ "GET /api/workspaces": () => json([{ id: HOST_WORKSPACE_ID, name: "host" }]) });
  render(() => <McpServersPanel />);
  expect((await screen.findByRole("alert")).textContent).toContain("Core doesn’t support this yet");
});

it("switches workspaces when there are several", async () => {
  setConnection("http://core.test", "test-token");
  const other = "33333333-3333-3333-3333-333333333333";
  stubCore({
    "GET /api/workspaces": () => json([{ id: HOST_WORKSPACE_ID, name: "host" }, { id: other, name: "builder" }]),
    [`GET /api/workspaces/${HOST_WORKSPACE_ID}/mcps`]: () => json([summary({ name: "host-only" })]),
    [`GET /api/workspaces/${other}/mcps`]: () => json([]),
  });
  render(() => <McpServersPanel />);
  await screen.findByText("host-only");
  openRow("host-only");
  expect(document.querySelector("details.mcp-row[open]")).toBeTruthy();
  const select = await screen.findByLabelText("Workspace") as HTMLSelectElement;
  select.value = other;
  fireEvent.change(select);
  expect(await screen.findByText("No MCP servers apply to this workspace.")).toBeTruthy();
  expect(screen.queryByText("host-only")).toBeNull();
});
