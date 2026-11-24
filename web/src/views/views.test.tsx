import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import React from "react";
import { DashboardView } from "./DashboardView";
import { GraphEditorView } from "./GraphEditorView";
import { PackageStudioView } from "./PackageStudioView";
import { SignalClient, setSignalClient } from "../lib/signals";
import { MockWebSocket } from "../test/mockWebSocket";
import { useClientState, clientStateStorageKey, getOrCreateClientId } from "../lib/clientState";
import type { DashboardDoc, RenderedDashboard } from "../lib/api";

type Handler = (url: string, init: RequestInit | undefined) => unknown | Promise<unknown>;

function mockFetch(routes: Array<[string | RegExp, string, Handler]>) {
  const calls: Array<{ url: string; method: string; body: unknown }> = [];
  const spy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = String(input);
    const method = (init?.method ?? "GET").toUpperCase();
    const body = init?.body ? JSON.parse(String(init.body)) : undefined;
    calls.push({ url, method, body });
    for (const [pattern, m, handler] of routes) {
      const match = typeof pattern === "string" ? url === pattern : pattern.test(url);
      if (match && m === method) {
        try {
          const res = await handler(url, init);
          return new Response(JSON.stringify(res ?? {}), { status: 200 });
        } catch (e) {
          return new Response(JSON.stringify({ error: String((e as Error).message) }), { status: 400 });
        }
      }
    }
    return new Response(JSON.stringify({}), { status: 200 });
  });
  return { spy, calls };
}

const doc = (): DashboardDoc => ({
  id: "ops",
  title: "Ops",
  layout: "grid",
  initial_state: { selected_repository: "acme/platform" },
  adapters: [{ adapter_instance: "table_adapter", area: "main", query_params: {} }],
  overrides: {},
});

const renderedWith = (rows: Array<Record<string, unknown>>, generation = 7): RenderedDashboard => ({
  dashboard_id: "ops",
  title: "Ops",
  layout: "grid",
  generation,
  dashboard_state: { selected_repository: "acme/platform" },
  overrides: {},
  adapters: [
    {
      adapter_instance: "table_adapter",
      area: "main",
      tree: {
        widget_id: "table_adapter_root",
        widget_type: "dash.table",
        props: {
          title: "Items",
          columns: [{ key: "title", label: "Title" }],
          rows,
          row_action: { label: "Ship it", request: "ship_requested" },
        },
      },
    },
  ],
});

describe("DashboardView", () => {
  beforeEach(() => {
    localStorage.clear();
    vi.restoreAllMocks();
    MockWebSocket.reset();
    setSignalClient(
      new SignalClient({
        url: "ws://test/ws",
        WebSocketImpl: MockWebSocket as unknown as typeof WebSocket,
        reconnectDelayMs: () => 1_000_000,
      })
    );
  });
  afterEach(() => setSignalClient(null));

  it("applies pushed Signal renders and confirms action Requests with the Adapter-resolved Dry run", async () => {
    const { calls } = mockFetch([
      ["/api/dashboards", "GET", () => [doc()]],
      ["/api/dashboards/ops", "GET", () => ({ document: doc(), rendered: renderedWith([{ id: 1, title: "first" }]) })],
      [
        "/api/widget-request/dry-run",
        "POST",
        () => ({
          protocol: "dash.widget/1",
          dashboard_id: "ops",
          adapter_instance: "table_adapter",
          widget_id: "table_adapter_root",
          request: "ship_requested",
          resolution: "action",
          action_instance: "deploy_service",
          args: { id: 2 },
          dry_run: { action_instance: "deploy_service", feasible: true, advisory: true, warnings: ["slow"], effects: { deploys: 1 } },
          requires_confirmation: true,
        }),
      ],
      ["/api/widget-request", "POST", () => ({ execution_id: "act_1", status: "succeeded" })],
    ]);

    render(<DashboardView />);
    await screen.findByText("first");

    // The server pushes a new render (e.g. another Client or a poll) over /ws.
    const ws = MockWebSocket.latest();
    act(() => ws.open());
    expect(ws.sent).toContainEqual({ type: "subscribe_dashboard", dashboard_id: "ops" });
    act(() =>
      ws.receive({
        type: "dashboard_render",
        protocol: "dash.render/1",
        dashboard_id: "ops",
        generation: 8,
        reason: "query",
        signals: { items: { status: "fresh", updated_at: "2026-11-24T10:00:00Z", version: 2, generation: 8 } },
        data: { document: doc(), rendered: renderedWith([{ id: 2, title: "second" }], 8) },
      })
    );
    await screen.findByText("second");
    expect(screen.getByText("Generation #8")).toBeInTheDocument();
    expect(screen.getByText("Live")).toBeInTheDocument();
    expect(screen.getByText(/items: fresh/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Ship it" }));
    await screen.findByText("Confirm action (Dry run)");
    expect(screen.getByText("deploy_service", { selector: "code" })).toBeInTheDocument();
    expect(screen.getByText("slow")).toBeInTheDocument();
    const dryCall = calls.find((c) => c.url === "/api/widget-request/dry-run");
    expect(dryCall?.body).toMatchObject({ request: "ship_requested", generation: 8, widget_id: "table_adapter_root" });
    // No hard-coded action endpoint is used.
    expect(calls.some((c) => c.url.startsWith("/api/actions/"))).toBe(false);

    fireEvent.click(screen.getByRole("button", { name: "Confirm & Execute" }));
    await screen.findByText(/execution act_1/);
    expect(calls.filter((c) => c.url === "/api/widget-request" && c.method === "POST")).toHaveLength(1);
  });

  it("runs Requests that resolve to a refresh without confirmation", async () => {
    const { calls } = mockFetch([
      ["/api/dashboards", "GET", () => [doc()]],
      ["/api/dashboards/ops", "GET", () => ({ document: doc(), rendered: renderedWith([{ id: 1, title: "first" }]) })],
      ["/api/widget-request/dry-run", "POST", () => ({ resolution: "refresh", requires_confirmation: false })],
      ["/api/widget-request", "POST", () => ({ execution_id: "req_1", status: "succeeded" })],
    ]);
    render(<DashboardView />);
    await screen.findByText("first");
    fireEvent.click(screen.getByRole("button", { name: "Ship it" }));
    await screen.findByText(/execution req_1/);
    expect(screen.queryByText("Confirm action (Dry run)")).not.toBeVisible();
    expect(calls.map((c) => c.url)).toContain("/api/widget-request");
  });

  it("saves Designer edits onto the Draft's base Dashboard and aborts the Draft on failure", async () => {
    const newerBase: DashboardDoc = { ...doc(), title: "Ops (renamed by Agent)", overrides: { other: { w: 3, props: {} } } };
    let activateFails = true;
    const { calls } = mockFetch([
      ["/api/dashboards", "GET", () => [doc()]],
      ["/api/dashboards/ops", "GET", () => ({ document: doc(), rendered: renderedWith([{ id: 1, title: "first" }]) })],
      ["/api/graph", "GET", () => ({ instances: { table_adapter: { id: "table_adapter", category: "adapter" } } })],
      ["/api/drafts", "POST", () => ({ draft_id: "dft_d1" })],
      ["/api/drafts/dft_d1/graph", "GET", () => ({ dashboards: { ops: newerBase } })],
      ["/api/drafts/dft_d1/dashboards", "POST", () => ({ draft_id: "dft_d1" })],
      [
        "/api/drafts/dft_d1/activate",
        "POST",
        () => {
          if (activateFails) throw new Error("stale draft base revision");
          return { generation_id: 9 };
        },
      ],
      ["/api/drafts/dft_d1", "DELETE", () => ({ aborted: true })],
    ]);

    render(<DashboardView />);
    await screen.findByText("first");
    fireEvent.click(screen.getByRole("button", { name: "Dashboard Designer" }));
    fireEvent.click(document.querySelector('[data-widget-id="table_adapter_root"]') as HTMLElement);
    const editor = screen.getByText("Widget properties").closest(".card") as HTMLElement;
    fireEvent.change(within(editor).getByLabelText("Width (w, columns)"), { target: { value: "6" } });
    fireEvent.change(within(editor).getByLabelText("Height (h, rows)"), { target: { value: "4" } });
    fireEvent.change(within(editor).getByLabelText("Column start (x)"), { target: { value: "3" } });
    fireEvent.change(within(editor).getByPlaceholderText("Items"), { target: { value: "Shipping queue" } });

    // Live preview of the pending override (grid placement and configurable prop).
    const node = document.querySelector('[data-widget-id="table_adapter_root"]') as HTMLElement;
    expect(node.style.gridColumn).toBe("4 / span 6");
    expect(node.style.gridRow).toBe("span 4");
    expect(screen.getAllByText("Shipping queue").length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: "Save & Activate" }));
    await screen.findByText(/Save failed: Error: stale draft base revision/);
    expect(calls.some((c) => c.url === "/api/drafts/dft_d1" && c.method === "DELETE")).toBe(true);

    const posted = calls.find((c) => c.url === "/api/drafts/dft_d1/dashboards")?.body as DashboardDoc;
    // Built from the Draft's base (concurrent title/override changes kept) plus our edits.
    expect(posted.title).toBe("Ops (renamed by Agent)");
    expect(posted.overrides.other).toEqual({ w: 3, props: {} });
    expect(posted.overrides.table_adapter_root).toMatchObject({ w: 6, h: 4, x: 3, props: { title: "Shipping queue" } });

    // Edits are kept for a retry, which now succeeds.
    activateFails = false;
    fireEvent.click(screen.getByRole("button", { name: "Save & Activate" }));
    await screen.findByText(/activated Generation #9/);
  });

  it("edits Dashboard State generically from declared keys", async () => {
    const { calls } = mockFetch([
      ["/api/dashboards", "GET", () => [doc()]],
      ["/api/dashboards/ops", "GET", () => ({ document: doc(), rendered: renderedWith([]) })],
      ["/api/dashboards/ops/state", "POST", () => ({})],
    ]);
    render(<DashboardView />);
    const input = await screen.findByLabelText("Dashboard State selected_repository");
    fireEvent.change(input, { target: { value: "acme/payments" } });
    fireEvent.keyDown(input, { key: "Enter" });
    await waitFor(() =>
      expect(calls.find((c) => c.url === "/api/dashboards/ops/state")?.body).toEqual({
        key: "selected_repository",
        value: "acme/payments",
      })
    );
  });
});

describe("GraphEditorView", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("builds successive drag-connect edits on the Draft graph and shows issues and capabilities inline", async () => {
    const base = {
      gh: { id: "gh", category: "connector", definition: "github.connector", inputs: {}, config: {}, secrets: {}, capabilities: ["pulls:read"] },
      prs: { id: "prs", category: "data_provider", definition: "github.pull_requests", inputs: {}, config: {}, secrets: {}, capabilities: [] },
      issues: { id: "issues", category: "data_provider", definition: "jira.issues", inputs: {}, config: {}, secrets: {}, capabilities: [] },
      table: { id: "table", category: "adapter", definition: "eng.table", inputs: {}, config: {}, secrets: {}, capabilities: [] },
    };
    const defs = {
      "eng.table": {
        package: "eng", category: "adapter", description: "", capabilities: [], required_capabilities: [], optional_capabilities: [],
        inputs: { prs: { ref_kind: "data" }, issues: { ref_kind: "data" } }, accepts_requests: [],
      },
      "github.pull_requests": {
        package: "github", category: "data_provider", description: "", capabilities: [], required_capabilities: [], optional_capabilities: [],
        inputs: { github: { ref_kind: "connector", required_capabilities: ["pulls:read", "pulls:write"] } }, accepts_requests: [],
      },
    };
    let draftInstances: Record<string, typeof base.table> = JSON.parse(JSON.stringify(base));
    const { calls } = mockFetch([
      ["/api/graph", "GET", () => ({ generation_id: 3, instances: base, validation: { valid: true, issues: [] } })],
      ["/api/definitions", "GET", () => defs],
      ["/api/drafts", "POST", () => ({ draft_id: "dft_g" })],
      [
        "/api/drafts/dft_g/graph",
        "GET",
        () => ({
          draft_id: "dft_g",
          instances: draftInstances,
          definitions: defs,
          validation: {
            valid: false,
            issues: [
              { severity: "error", code: "INSUFFICIENT_CAPABILITY", target: "prs", message: "input 'github' requires capability 'pulls:write' which connector 'gh' does not grant" },
            ],
          },
        }),
      ],
      [
        "/api/drafts/dft_g/instances",
        "POST",
        (_u, init) => {
          const inst = JSON.parse(String(init?.body));
          draftInstances = { ...draftInstances, [inst.id]: inst };
          return { draft_id: "dft_g" };
        },
      ],
    ]);

    render(<GraphEditorView />);
    await screen.findByText("table");
    const card = (id: string) => document.querySelector(`[data-node-id="${id}"]`) as HTMLElement;

    fireEvent.dragStart(card("prs"));
    fireEvent.drop(card("table"));
    await screen.findByText("Active Draft: dft_g");
    await waitFor(() => expect(within(card("table")).getByText("prs", { selector: "code" })).toBeInTheDocument());

    fireEvent.dragStart(card("issues"));
    fireEvent.drop(card("table"));
    await waitFor(() => expect(within(card("table")).getByText("issues", { selector: "code" })).toBeInTheDocument());

    const upserts = calls.filter((c) => c.url === "/api/drafts/dft_g/instances").map((c) => c.body as { inputs: Record<string, string> });
    // The second edit keeps the first binding (built from the Draft graph, not the active one).
    expect(upserts[1].inputs).toEqual({ prs: "prs", issues: "issues" });

    // Validation issue on the offending input, and required capabilities with grant status.
    expect(within(card("prs")).getByText(/INSUFFICIENT_CAPABILITY/)).toBeInTheDocument();
    expect(within(card("prs")).getByText("requires pulls:read via github: missing")).toBeInTheDocument();
    expect(within(card("prs")).getByText("requires pulls:write via github: missing")).toBeInTheDocument();

    // Remove a binding from the Draft.
    fireEvent.click(within(card("table")).getByRole("button", { name: "Remove binding table.prs" }));
    await waitFor(() => expect(within(card("table")).queryByText("prs", { selector: "code" })).not.toBeInTheDocument());
  });
});

describe("PackageStudioView", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("opens existing Definition sources, maps diagnostics to lines and shows the Draft diff", async () => {
    const { calls } = mockFetch([
      ["/api/catalog", "GET", () => [{ id: "engineering.pr_table_adapter", package: "engineering", category: "adapter", description: "PR table", identity_version: "x" }]],
      ["/api/git/status", "GET", () => ({ branch: "main", head_commit: "abcdef1234567", dirty: false, changed_files: [], remote_url: null, sync_status: "no_remote" })],
      ["/api/git/history", "GET", () => [{ hash: "abcdef1234567", author: "dash", subject: "init" }]],
      ["/api/contracts/engineering.pr_table_adapter", "GET", () => ({ inputs: { linked: {} }, generated_luau_types: "" })],
      [
        /\/api\/definitions\/engineering\.pr_table_adapter\/sources/,
        "GET",
        () => ({
          definition_id: "engineering.pr_table_adapter",
          package: "engineering",
          category: "adapter",
          package_dir: "engineering",
          definition_dir: "pr_table_adapter",
          editable: true,
          definition_luau: "return dash.adapter { id = \"engineering.pr_table_adapter\" }\n",
          runtime_file: "runtime.luau",
          runtime_luau: "local Runtime = {}\nfunction Runtime.render(ctx)\n  return nil\nend\nreturn Runtime\n",
          tests_luau: "-- tests\n",
          generated_luau_types: "export type QueryParams = { repository: string }\n",
        }),
      ],
      ["/api/drafts", "POST", () => ({ draft_id: "dft_s" })],
      ["/api/drafts/dft_s/definitions", "POST", () => ({ draft_id: "dft_s" })],
      [
        "/api/drafts/dft_s/validate",
        "POST",
        () => {
          throw new Error('luau error: [string "engineering.pr_table_adapter/runtime.luau"]:3: attempt to call a nil value');
        },
      ],
      [
        "/api/drafts/dft_s/diff",
        "GET",
        () => ({
          draft_id: "dft_s",
          base_revision: "abcdef1234567",
          truncated: false,
          files: [
            {
              path: "packages/engineering/pr_table_adapter/runtime.luau",
              status: "modified",
              patch: "@@ -1,2 +1,2 @@\n local Runtime = {}\n-old line\n+new line\n",
            },
          ],
        }),
      ],
    ]);

    render(<PackageStudioView />);
    fireEvent.click(await screen.findByText("engineering.pr_table_adapter"));
    const editor = (await screen.findByLabelText("runtime.luau")) as HTMLTextAreaElement;
    await waitFor(() => expect(editor.value).toContain("function Runtime.render(ctx)"));
    expect(screen.getByDisplayValue("pr_table_adapter")).toBeInTheDocument();
    expect(screen.getByText("main")).toBeInTheDocument();
    expect(screen.getByText("no_remote")).toBeInTheDocument();

    // Autocomplete from generated types.
    fireEvent.change(editor, { target: { value: "Que", selectionStart: 3 } });
    expect(await screen.findByRole("option", { name: "QueryParams" })).toBeInTheDocument();
    fireEvent.change(editor, { target: { value: "local Runtime = {}\nfunction Runtime.render(ctx)\n  return ctx.x()\nend\n" } });

    fireEvent.click(screen.getByRole("button", { name: "Save to Draft & Check" }));
    await screen.findByText("Diagnostics");
    expect(screen.getByRole("button", { name: "engineering.pr_table_adapter/runtime.luau:3" })).toBeInTheDocument();
    expect(document.querySelector(".gutter-line.has-diag")?.textContent).toContain("3");
    expect(calls.find((c) => c.url === "/api/drafts/dft_s/definitions")?.body).toMatchObject({
      package: "engineering",
      name: "pr_table_adapter",
    });

    // Diff view against the base Revision.
    await screen.findByText(/Draft diff vs base Revision/);
    expect(screen.getByText("+new line")).toBeInTheDocument();
    expect(screen.getByText("-old line")).toBeInTheDocument();
  });
});

describe("useClientState", () => {
  const Probe: React.FC<{ k: string }> = ({ k }) => {
    const [v] = useClientState<string>(k, "initial");
    return <span data-testid="v">{v}</span>;
  };

  beforeEach(() => localStorage.clear());

  it("re-reads on key change and resets when another tab removes the key", () => {
    const id = getOrCreateClientId();
    localStorage.setItem(clientStateStorageKey(id, "a"), JSON.stringify("from-a"));
    localStorage.setItem(clientStateStorageKey(id, "b"), JSON.stringify("from-b"));
    const { rerender } = render(<Probe k="a" />);
    expect(screen.getByTestId("v").textContent).toBe("from-a");
    rerender(<Probe k="b" />);
    expect(screen.getByTestId("v").textContent).toBe("from-b");
    act(() => {
      window.dispatchEvent(new StorageEvent("storage", { key: clientStateStorageKey(id, "b"), newValue: null }));
    });
    expect(screen.getByTestId("v").textContent).toBe("initial");
  });
});
