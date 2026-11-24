/**
 * Regression tests for workstream F7: /ws subscription errors, live pushes vs in-progress
 * input, Preview diagnostics, serialized Draft mutations and CodeEditor keyboard escape.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import React from "react";
import { DashboardView } from "./DashboardView";
import { GraphEditorView } from "./GraphEditorView";
import { PackageStudioView } from "./PackageStudioView";
import { CodeEditor } from "../components/CodeEditor";
import { SignalClient, setSignalClient } from "../lib/signals";
import { MockWebSocket } from "../test/mockWebSocket";
import type { DashboardDoc, RenderedDashboard } from "../lib/api";

type Handler = (url: string, init: RequestInit | undefined) => unknown | Promise<unknown>;

function mockFetch(routes: Array<[string | RegExp, string, Handler]>) {
  const calls: Array<{ url: string; method: string; body: unknown }> = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
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
  return calls;
}

function deferred<T = void>() {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

const doc = (queryParams: Record<string, unknown> = { team: "core" }): DashboardDoc => ({
  id: "ops",
  title: "Ops",
  layout: "grid",
  initial_state: { filters: { team: "core" } },
  adapters: [{ adapter_instance: "table_adapter", area: "main", query_params: queryParams }],
  overrides: {},
});

const rendered = (filters: unknown = { team: "core" }, generation = 7): RenderedDashboard => ({
  dashboard_id: "ops",
  title: "Ops",
  layout: "grid",
  generation,
  dashboard_state: { filters },
  dashboard_state_versions: { filters: 1 },
  overrides: {},
  adapters: [
    {
      adapter_instance: "table_adapter",
      area: "main",
      tree: { widget_id: "root", widget_type: "dash.text", props: { text: `gen ${generation}` } },
    },
  ],
});

const pushRender = (ws: MockWebSocket, r: RenderedDashboard, d: DashboardDoc | null = doc()) =>
  act(() =>
    ws.receive({
      type: "dashboard_render",
      protocol: "dash.render/1",
      dashboard_id: "ops",
      generation: r.generation,
      reason: "query",
      signals: {},
      data: { document: d, rendered: r },
    })
  );

describe("DashboardView live subscription (F7)", () => {
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

  it("surfaces /ws render errors, drops 'Live' and reloads the user's own writes over HTTP (web-1)", async () => {
    const calls = mockFetch([
      ["/api/dashboards", "GET", () => [doc()]],
      ["/api/dashboards/ops", "GET", () => ({ document: doc(), rendered: rendered() })],
      ["/api/dashboards/ops/state", "POST", () => ({})],
    ]);
    render(<DashboardView />);
    await screen.findByText("gen 7");
    const ws = MockWebSocket.latest();
    act(() => ws.open());
    // An open socket alone is not a healthy subscription.
    expect(screen.queryByText("Live")).not.toBeInTheDocument();
    pushRender(ws, rendered({ team: "core" }, 8));
    expect(await screen.findByText("Live")).toBeInTheDocument();

    // Errors for other Dashboards are not ours.
    act(() => ws.receive({ type: "error", code: "render_failed", dashboard_id: "other", message: "nope" }));
    expect(screen.getByText("Live")).toBeInTheDocument();

    const listLoads = () => calls.filter((c) => c.url === "/api/dashboards" && c.method === "GET").length;
    const before = listLoads();
    act(() =>
      ws.receive({ type: "error", code: "render_failed", dashboard_id: "ops", message: "adapter 'table_adapter' failed" })
    );
    expect(await screen.findByText("Live updates failed")).toBeInTheDocument();
    expect(screen.getByRole("alert").textContent).toContain("adapter 'table_adapter' failed");
    expect(screen.queryByText("Live")).not.toBeInTheDocument();
    // The selector is refreshed in case the Dashboard was removed.
    await waitFor(() => expect(listLoads()).toBeGreaterThan(before));

    // A State write is no longer reflected by a push, so the view reloads it itself.
    const reloads = () => calls.filter((c) => c.url === "/api/dashboards/ops" && c.method === "GET").length;
    const reloadsBefore = reloads();
    const input = screen.getByLabelText("Dashboard State filters");
    fireEvent.change(input, { target: { value: '{"team":"platform"}' } });
    fireEvent.keyDown(input, { key: "Enter" });
    await waitFor(() => expect(reloads()).toBeGreaterThan(reloadsBefore));

    // Recovery: the next render restores the healthy state and clears the error.
    pushRender(ws, rendered({ team: "core" }, 9));
    expect(await screen.findByText("Live")).toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("shows a subscribe-time refusal instead of 'Live' (web-1)", async () => {
    mockFetch([
      ["/api/dashboards", "GET", () => [doc()]],
      ["/api/dashboards/ops", "GET", () => ({ document: doc(), rendered: rendered() })],
    ]);
    render(<DashboardView />);
    await screen.findByText("gen 7");
    const ws = MockWebSocket.latest();
    act(() => ws.open());
    act(() =>
      ws.receive({ type: "error", code: "subscription_limit", dashboard_id: "ops", message: "at most 16 dashboard subscriptions" })
    );
    expect(await screen.findByText("Live updates failed")).toBeInTheDocument();
    expect(screen.getByRole("alert").textContent).toContain("subscription_limit");
  });

  it("keeps in-progress Dashboard State text across pushes with equal or changed content (web-2)", async () => {
    mockFetch([
      ["/api/dashboards", "GET", () => [doc()]],
      ["/api/dashboards/ops", "GET", () => ({ document: doc(), rendered: rendered() })],
    ]);
    render(<DashboardView />);
    await screen.findByText("gen 7");
    const ws = MockWebSocket.latest();
    act(() => ws.open());
    const input = screen.getByLabelText("Dashboard State filters") as HTMLInputElement;
    expect(input.value).toBe('{"team":"core"}');

    // Not dirty: a push with new content updates the field.
    pushRender(ws, rendered({ team: "infra" }, 8));
    await waitFor(() => expect(input.value).toBe('{"team":"infra"}'));

    fireEvent.change(input, { target: { value: '{"team":"platf' } });
    // Same content, fresh object identity (every push re-parses JSON).
    pushRender(ws, rendered({ team: "infra" }, 9));
    await screen.findByText("gen 9");
    expect(input.value).toBe('{"team":"platf');
    // Changed content from another Client: still never clobbers uncommitted typing.
    pushRender(ws, rendered({ team: "ops" }, 10));
    await screen.findByText("gen 10");
    expect(input.value).toBe('{"team":"platf');
  });

  it("keeps in-progress Designer query_params text across pushes (web-2)", async () => {
    mockFetch([
      ["/api/dashboards", "GET", () => [doc()]],
      ["/api/dashboards/ops", "GET", () => ({ document: doc(), rendered: rendered() })],
      ["/api/graph", "GET", () => ({ instances: {} })],
    ]);
    render(<DashboardView />);
    await screen.findByText("gen 7");
    const ws = MockWebSocket.latest();
    act(() => ws.open());
    fireEvent.click(screen.getByRole("button", { name: "Dashboard Designer" }));
    const placements = screen.getByText("Adapter placements").closest(".card") as HTMLElement;
    const params = within(placements).getByLabelText("query_params") as HTMLInputElement;
    expect(params.value).toBe('{"team":"core"}');

    fireEvent.change(params, { target: { value: '{"team":"pla' } });
    pushRender(ws, rendered({ team: "core" }, 8), doc());
    await screen.findByText("gen 8");
    expect(params.value).toBe('{"team":"pla');
  });
});

describe("PackageStudioView Preview diagnostics (F7)", () => {
  beforeEach(() => vi.restoreAllMocks());

  const baseRoutes = (preview: () => unknown): Array<[string | RegExp, string, Handler]> => [
    ["/api/catalog", "GET", () => []],
    ["/api/git/status", "GET", () => ({ branch: "main", head_commit: "abc", dirty: false, changed_files: [], remote_url: null, sync_status: "no_remote" })],
    ["/api/git/history", "GET", () => []],
    ["/api/drafts", "POST", () => ({ draft_id: "dft_p" })],
    ["/api/drafts/dft_p/preview", "POST", preview],
  ];

  it("shows preview render errors and validation issues as rows and diagnostics (web-3)", async () => {
    mockFetch(
      baseRoutes(() => ({
        validation: { valid: true, issues: [] },
        definition_tests: [{ definition_id: "engineering.t", cases: [{ name: "ok", passed: true, duration_ms: 1 }] }],
        scenario_results: [],
        preview_renders: {},
        preview_render_errors: { engineering: "DUPLICATE_WIDGET_KEY: two nodes share key 'row'" },
        all_passed: false,
      }))
    );
    render(<PackageStudioView />);
    fireEvent.click(screen.getByRole("button", { name: "Preview & Scenario Tests" }));
    await screen.findByText("failures");
    const row = screen.getByText("engineering", { selector: "code" }).closest("tr") as HTMLElement;
    expect(within(row).getByText("fail")).toBeInTheDocument();
    expect(within(row).getByText(/DUPLICATE_WIDGET_KEY/)).toBeInTheDocument();
    const diags = screen.getByText("Diagnostics").closest(".card") as HTMLElement;
    expect(within(diags).getByText(/Dashboard 'engineering' failed to render: DUPLICATE_WIDGET_KEY/)).toBeInTheDocument();
  });

  it("lists validation issues when Preview fails validation (web-3)", async () => {
    mockFetch(
      baseRoutes(() => ({
        validation: {
          valid: false,
          issues: [{ severity: "error", code: "MISSING_INPUT", target: "table", message: "input 'prs' is not bound" }],
        },
        definition_tests: [],
        scenario_results: [],
        preview_renders: {},
        all_passed: false,
      }))
    );
    render(<PackageStudioView />);
    fireEvent.click(screen.getByRole("button", { name: "Preview & Scenario Tests" }));
    await screen.findByText("validation failed");
    const row = screen.getByText("MISSING_INPUT", { selector: "code" }).closest("tr") as HTMLElement;
    expect(within(row).getByText("input 'prs' is not bound")).toBeInTheDocument();
    const diags = screen.getByText("Diagnostics").closest(".card") as HTMLElement;
    expect(within(diags).getByText("MISSING_INPUT table: input 'prs' is not bound")).toBeInTheDocument();
  });

  it("opens one Draft when Draft operations overlap (web-4)", async () => {
    const gate = deferred();
    const calls = mockFetch([
      ["/api/catalog", "GET", () => []],
      ["/api/git/status", "GET", () => ({ branch: "main", head_commit: "abc", dirty: false, changed_files: [], remote_url: null, sync_status: "no_remote" })],
      ["/api/git/history", "GET", () => []],
      [
        "/api/drafts",
        "POST",
        async () => {
          await gate.promise;
          return { draft_id: "dft_once" };
        },
      ],
      ["/api/drafts/dft_once/validate", "POST", () => ({ valid: true, issues: [] })],
      ["/api/drafts/dft_once/diff", "GET", () => ({ draft_id: "dft_once", base_revision: "abc", truncated: false, files: [] })],
      ["/api/drafts/dft_once/preview", "POST", () => ({ validation: { valid: true, issues: [] }, definition_tests: [], scenario_results: [], preview_renders: {}, all_passed: true })],
    ]);
    render(<PackageStudioView />);
    const save = screen.getByRole("button", { name: "Save to Draft & Check" });
    fireEvent.click(save);
    fireEvent.click(save);
    fireEvent.click(screen.getByRole("button", { name: "Preview & Scenario Tests" }));
    await act(async () => gate.resolve());
    await screen.findByText("Draft: dft_once");
    await waitFor(() => expect(screen.getByRole("button", { name: "Save to Draft & Check" })).not.toBeDisabled());
    expect(calls.filter((c) => c.url === "/api/drafts" && c.method === "POST")).toHaveLength(1);
    expect(calls.filter((c) => c.url === "/api/drafts/dft_once/definitions")).toHaveLength(1);
  });
});

describe("GraphEditorView serialized Draft mutations (F7)", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("opens one Draft and builds overlapping edits on the latest Draft graph (web-4)", async () => {
    const base = {
      prs: { id: "prs", category: "data_provider", definition: "github.pull_requests", inputs: {}, config: {}, secrets: {}, capabilities: [] },
      issues: { id: "issues", category: "data_provider", definition: "jira.issues", inputs: {}, config: {}, secrets: {}, capabilities: [] },
      table: { id: "table", category: "adapter", definition: "eng.table", inputs: {}, config: {}, secrets: {}, capabilities: [] },
    };
    const defs = {
      "eng.table": {
        package: "eng", category: "adapter", description: "", capabilities: [], required_capabilities: [], optional_capabilities: [],
        inputs: { prs: { ref_kind: "data" }, issues: { ref_kind: "data" } }, accepts_requests: [],
      },
    };
    let draftInstances: Record<string, typeof base.table> = JSON.parse(JSON.stringify(base));
    const gate = deferred();
    let draftCount = 0;
    const calls = mockFetch([
      ["/api/graph", "GET", () => ({ generation_id: 3, instances: base, validation: { valid: true, issues: [] } })],
      ["/api/definitions", "GET", () => defs],
      [
        "/api/drafts",
        "POST",
        async () => {
          draftCount += 1;
          await gate.promise;
          return { draft_id: `dft_${draftCount}` };
        },
      ],
      [
        /\/api\/drafts\/dft_\d+\/graph$/,
        "GET",
        () => ({ instances: draftInstances, definitions: defs, validation: { valid: true, issues: [] } }),
      ],
      [
        /\/api\/drafts\/dft_\d+\/instances$/,
        "POST",
        (_u, init) => {
          const inst = JSON.parse(String(init?.body));
          draftInstances = { ...draftInstances, [inst.id]: inst };
          return {};
        },
      ],
    ]);

    render(<GraphEditorView />);
    await screen.findByText("table");
    const card = (id: string) => document.querySelector(`[data-node-id="${id}"]`) as HTMLElement;
    // Two drag-connects onto the same target while the Draft is still being created.
    fireEvent.dragStart(card("prs"));
    fireEvent.drop(card("table"));
    fireEvent.dragStart(card("issues"));
    fireEvent.drop(card("table"));
    await act(async () => gate.resolve());

    await screen.findByText("Active Draft: dft_1");
    await waitFor(() =>
      expect(calls.filter((c) => /\/instances$/.test(c.url) && c.method === "POST")).toHaveLength(2)
    );
    expect(calls.filter((c) => c.url === "/api/drafts" && c.method === "POST")).toHaveLength(1);
    const upserts = calls.filter((c) => /\/instances$/.test(c.url)).map((c) => c.body as { inputs: Record<string, string> });
    expect(upserts[1].inputs).toEqual({ prs: "prs", issues: "issues" });
    await waitFor(() => expect(within(card("table")).getByText("issues", { selector: "code" })).toBeInTheDocument());
    expect(within(card("table")).getByText("prs", { selector: "code" })).toBeInTheDocument();
  });
});

describe("CodeEditor keyboard (F7)", () => {
  const Harness: React.FC<{ readOnly?: boolean; onChange?: (v: string) => void }> = ({ readOnly, onChange }) => {
    const [v, setV] = React.useState("local x");
    return (
      <CodeEditor
        label="runtime.luau"
        value={v}
        onChange={(n) => {
          onChange?.(n);
          setV(n);
        }}
        completions={["xyzzy"]}
        diagnostics={[]}
        readOnly={readOnly}
      />
    );
  };

  it("indents on Tab but lets Shift+Tab and Esc-then-Tab leave the editor (web-6)", () => {
    render(<Harness />);
    const ta = screen.getByLabelText("runtime.luau") as HTMLTextAreaElement;
    ta.setSelectionRange(0, 0);
    const tab = fireEvent.keyDown(ta, { key: "Tab" });
    expect(tab).toBe(false); // default prevented: indentation inserted
    expect(ta.value).toBe("    local x");

    expect(fireEvent.keyDown(ta, { key: "Tab", shiftKey: true })).toBe(true);
    fireEvent.keyDown(ta, { key: "Escape" });
    expect(fireEvent.keyDown(ta, { key: "Tab" })).toBe(true); // focus may move on
    expect(ta.value).toBe("    local x");

    // Any other key re-arms Tab indentation.
    fireEvent.keyDown(ta, { key: "a" });
    expect(fireEvent.keyDown(ta, { key: "Tab" })).toBe(false);
    expect(screen.getByText(/Esc then Tab/)).toBeInTheDocument();
  });

  it("lets Shift+Tab leave the editor while the completion list is open (F7)", () => {
    const onChange = vi.fn();
    render(<Harness onChange={onChange} />);
    const ta = screen.getByLabelText("runtime.luau") as HTMLTextAreaElement;
    ta.setSelectionRange(7, 7);
    fireEvent.keyDown(ta, { key: " ", ctrlKey: true });
    expect(screen.getByRole("listbox")).toBeInTheDocument();
    expect(fireEvent.keyDown(ta, { key: "Tab", shiftKey: true })).toBe(true); // not intercepted
    expect(screen.queryByRole("listbox")).not.toBeInTheDocument();
    expect(onChange).not.toHaveBeenCalled();
    expect(ta.value).toBe("local x");
  });

  it("never edits a read-only editor via completion (web-6)", () => {
    const onChange = vi.fn();
    render(<Harness readOnly onChange={onChange} />);
    const ta = screen.getByLabelText("runtime.luau") as HTMLTextAreaElement;
    ta.setSelectionRange(7, 7);
    fireEvent.keyDown(ta, { key: " ", ctrlKey: true });
    expect(screen.queryByRole("listbox")).not.toBeInTheDocument();
    fireEvent.keyDown(ta, { key: "Enter" });
    expect(onChange).not.toHaveBeenCalled();
    expect(fireEvent.keyDown(ta, { key: "Tab" })).toBe(true);
  });
});
