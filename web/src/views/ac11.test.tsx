/**
 * §17 criterion 11 (visual UI): inline compatibility / forbidden-binding errors after
 * drag-to-connect, Draft activation from the Graph Editor, the read-only execution graph and
 * traces view, and validation of messages from sandboxed custom widget frames.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { GraphEditorView } from "./GraphEditorView";
import { DiagnosticsView } from "./DiagnosticsView";
import { SandboxedWidgetFrame } from "../components/widgets/BuiltInWidgets";

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
          return new Response(JSON.stringify((await handler(url, init)) ?? {}), { status: 200 });
        } catch (e) {
          return new Response(JSON.stringify({ error: String((e as Error).message) }), { status: 400 });
        }
      }
    }
    return new Response(JSON.stringify({ error: `unmocked ${method} ${url}` }), { status: 404 });
  });
  return calls;
}

interface Inst {
  id: string;
  category: string;
  definition: string;
  inputs: Record<string, string>;
  config: Record<string, unknown>;
  secrets: Record<string, string>;
  capabilities: string[];
}

const inst = (id: string, category: string, definition: string, extra: Partial<Inst> = {}): Inst => ({
  id,
  category,
  definition,
  inputs: {},
  config: {},
  secrets: {},
  capabilities: [],
  ...extra,
});

/** Server-side validation of the forbidden edges (§2.1), mirroring dash-core's messages. */
function validate(instances: Record<string, Inst>) {
  const issues: Array<{ severity: string; code: string; target: string; message: string }> = [];
  for (const i of Object.values(instances)) {
    for (const src of Object.values(i.inputs)) {
      const s = instances[src];
      if (!s) continue;
      if (i.category === "data_provider" && s.category === "action_provider")
        issues.push({
          severity: "error",
          code: "FORBIDDEN_BINDING",
          target: i.id,
          message: `forbidden binding (§2.1): data provider '${i.id}' cannot bind action '${src}'`,
        });
      if (i.category === "adapter" && s.category === "connector")
        issues.push({
          severity: "error",
          code: "FORBIDDEN_BINDING",
          target: i.id,
          message: `forbidden binding (§2.1): adapter '${i.id}' cannot bind connector '${src}'`,
        });
    }
  }
  return { valid: issues.length === 0, issues };
}

describe("GraphEditorView forbidden bindings and activation", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("shows forbidden-binding errors on the dropped-onto node and only activates a valid Draft", async () => {
    const base: Record<string, Inst> = {
      gh: inst("gh", "connector", "github.connector", { capabilities: ["pull_requests.read"] }),
      merge: inst("merge", "action_provider", "github.merge_pr", { inputs: { github: "gh" } }),
      prs: inst("prs", "data_provider", "github.pull_requests", { inputs: { github: "gh" } }),
      table: inst("table", "adapter", "eng.table"),
    };
    const defs = {
      "github.pull_requests": {
        package: "github", category: "data_provider", description: "", capabilities: [], required_capabilities: [],
        optional_capabilities: [], accepts_requests: [],
        inputs: { github: { ref_kind: "connector", required_capabilities: ["pull_requests.read"] } },
      },
      "eng.table": {
        package: "eng", category: "adapter", description: "", capabilities: [], required_capabilities: [],
        optional_capabilities: [], accepts_requests: [], inputs: { prs: { ref_kind: "data" } },
      },
    };
    let draft: Record<string, Inst> = JSON.parse(JSON.stringify(base));
    const calls = mockFetch([
      ["/api/graph", "GET", () => ({ generation_id: 4, instances: base, validation: { valid: true, issues: [] } })],
      ["/api/definitions", "GET", () => defs],
      ["/api/drafts", "POST", () => ({ draft_id: "dft_fb" })],
      ["/api/drafts/dft_fb/graph", "GET", () => ({ draft_id: "dft_fb", instances: draft, definitions: defs, validation: validate(draft) })],
      [
        "/api/drafts/dft_fb/instances",
        "POST",
        (_u, init) => {
          const i = JSON.parse(String(init?.body)) as Inst;
          draft = { ...draft, [i.id]: i };
          return { draft_id: "dft_fb" };
        },
      ],
      [
        "/api/drafts/dft_fb/activate",
        "POST",
        () => {
          const v = validate(draft);
          if (!v.valid) throw new Error(`live validation errors: ${v.issues.map((i) => i.code).join(",")}`);
          return { revision_commit: "abcdef1234567890", generation_id: 5 };
        },
      ],
    ]);

    render(<GraphEditorView />);
    await screen.findByText("table");
    const card = (id: string) => document.querySelector(`[data-node-id="${id}"]`) as HTMLElement;
    const upserts = () => calls.filter((c) => c.url === "/api/drafts/dft_fb/instances");

    // No declared input of github.pull_requests accepts an action: refused client-side.
    fireEvent.dragStart(card("merge"));
    fireEvent.drop(card("prs"));
    await screen.findByText(/'github.pull_requests' declares no input that accepts a action_provider/);
    expect(upserts()).toHaveLength(0);

    // Forcing a slot sends the edge to the Draft; the server rejects it and the error is shown
    // on the binding row of the data provider.
    fireEvent.change(screen.getByPlaceholderText("auto"), { target: { value: "merge" } });
    fireEvent.dragStart(card("merge"));
    fireEvent.drop(card("prs"));
    await waitFor(() => expect(within(card("prs")).getByText("merge", { selector: "code" })).toBeInTheDocument());
    expect(upserts()[0].body).toMatchObject({ id: "prs", inputs: { github: "gh", merge: "merge" } });
    const row = within(card("prs")).getByText("merge", { selector: "code" }).closest(".graph-input-row") as HTMLElement;
    expect(within(row).getByText(/FORBIDDEN_BINDING: .*cannot bind action 'merge'/)).toBeInTheDocument();
    expect(card("prs").style.borderColor).toBe("var(--danger)");
    expect(card("table").style.borderColor).not.toBe("var(--danger)");
    expect(screen.getByText("Draft dft_fb: 1 validation issue(s), shown on the affected nodes")).toBeInTheDocument();

    // Adapter <- connector is forbidden too.
    fireEvent.change(screen.getByPlaceholderText("auto"), { target: { value: "gh" } });
    fireEvent.dragStart(card("gh"));
    fireEvent.drop(card("table"));
    await waitFor(() =>
      expect(within(card("table")).getByText(/FORBIDDEN_BINDING: .*cannot bind connector 'gh'/)).toBeInTheDocument()
    );
    expect(screen.getByText("2 issue(s)")).toBeInTheDocument();

    // Activation of the invalid Draft fails visibly and keeps the Draft open.
    fireEvent.click(screen.getByRole("button", { name: "Validate, Test & Activate Draft" }));
    await screen.findByText(/live validation errors: FORBIDDEN_BINDING,FORBIDDEN_BINDING/);
    expect(screen.getByText("Active Draft: dft_fb")).toBeInTheDocument();

    // Remove both offending bindings, then connect a valid edge with the auto slot.
    fireEvent.click(within(card("prs")).getByRole("button", { name: "Remove binding prs.merge" }));
    await waitFor(() => expect(within(card("prs")).queryByText(/FORBIDDEN_BINDING/)).not.toBeInTheDocument());
    fireEvent.click(within(card("table")).getByRole("button", { name: "Remove binding table.gh" }));
    await waitFor(() => expect(screen.getByText("Valid")).toBeInTheDocument());
    fireEvent.change(screen.getByPlaceholderText("auto"), { target: { value: "" } });
    fireEvent.dragStart(card("prs"));
    fireEvent.drop(card("table"));
    await waitFor(() => expect(within(card("table")).getByText("prs", { selector: "code" })).toBeInTheDocument());
    expect(upserts().at(-1)?.body).toMatchObject({ id: "table", inputs: { prs: "prs" } });

    fireEvent.click(screen.getByRole("button", { name: "Validate, Test & Activate Draft" }));
    await screen.findByText("Activated Generation #5 (abcdef12)");
    expect(calls.filter((c) => c.url === "/api/drafts/dft_fb/activate")).toHaveLength(2);
    expect(screen.queryByText(/Active Draft/)).not.toBeInTheDocument();
  });
});

describe("DiagnosticsView read-only execution graph and traces", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("renders runtime query nodes, dynamic edges, spans and action executions", async () => {
    mockFetch([
      [
        "/api/diagnostics/traces",
        "GET",
        () => ({
          execution_graph: {
            generation_id: 9,
            nodes: [
              { query_key: "qk_open_prs", instance_id: "open_prs", status: "fresh", version: 3, subscribers: 2, cache_hits: 7, cache_misses: 1, kind: "query", poll_interval_secs: 60 },
              { query_key: "rk_trend", instance_id: "pr_trend_adapter", status: "fresh", version: 1, subscribers: 1, cache_hits: 0, cache_misses: 0, kind: "adapter_render", poll_interval_secs: null },
            ],
            edges: [{ from_query_key: "rk_trend", to_query_key: "qk_open_prs" }],
          },
          spans: [{ trace_id: "tr_42", span_id: "sp_1", parent_span_id: null, name: "luau_vm:open_prs:query", status: "ok" }],
        }),
      ],
      ["/api/diagnostics/metrics", "GET", () => ({ total_queries: 12 })],
      [
        "/api/executions",
        "GET",
        () => [
          { execution_id: "act_7", action_instance: "transition_issue", principal_id: "agt_1", principal_kind: "agent", status: "succeeded", reason: null },
          { execution_id: "act_8", action_instance: "merge_pr", principal_id: "admin", principal_kind: "administrator", status: "unknown", reason: "lost_after_send" },
        ],
      ],
      ["/api/diagnostics/logs", "GET", () => []],
      ["/api/secrets", "GET", () => []],
      ["/api/agents", "GET", () => []],
      ["/api/service-principals", "GET", () => []],
      ["/api/diagnostics/audits", "GET", () => []],
    ]);
    render(<DiagnosticsView />);
    const heading = await screen.findByText("Read-Only Execution Graph & Traces");
    const panel = heading.closest(".card") as HTMLElement;
    await waitFor(() => expect(within(panel).getByText(/"qk_open_prs"/)).toBeInTheDocument());
    const text = panel.textContent ?? "";
    expect(text).toContain('"subscribers": 2');
    expect(text).toContain('"cache_hits": 7');
    expect(text).toContain('"kind": "adapter_render"');
    expect(text).toContain('"from_query_key": "rk_trend"');
    expect(text).toContain('"name": "luau_vm:open_prs:query"');
    expect(text).toContain('"trace_id": "tr_42"');

    const rowOf = (id: string) => screen.getByText(id, { selector: "code" }).closest("tr") as HTMLElement;
    expect(within(rowOf("act_7")).getByText("agt_1 (agent)")).toBeInTheDocument();
    expect(within(rowOf("act_7")).queryByRole("button", { name: "Resolve" })).toBeNull();
    expect(within(rowOf("act_8")).getByText("unknown").className).toContain("warn");
    expect(within(rowOf("act_8")).getByRole("button", { name: "Resolve" })).toBeInTheDocument();
  });
});

describe("DiagnosticsView secret form", () => {
  beforeEach(() => vi.restoreAllMocks());

  it("rotates a secret without sending policy fields, so the stored binding policy is kept", async () => {
    const calls = mockFetch([
      ["/api/diagnostics/traces", "GET", () => ({ execution_graph: { nodes: [], edges: [] }, spans: [] })],
      ["/api/diagnostics/metrics", "GET", () => ({})],
      ["/api/executions", "GET", () => []],
      ["/api/diagnostics/logs", "GET", () => []],
      ["/api/secrets", "GET", () => []],
      ["/api/secrets", "POST", () => ({ name: "github-work", key_version: 2 })],
      ["/api/agents", "GET", () => []],
      ["/api/service-principals", "GET", () => []],
      ["/api/diagnostics/audits", "GET", () => []],
    ]);
    render(<DiagnosticsView />);
    fireEvent.change(await screen.findByPlaceholderText("Secret name (e.g. github-work)"), {
      target: { value: "github-work" },
    });
    fireEvent.change(screen.getByPlaceholderText("Secret plaintext"), { target: { value: "ghp_v2" } });
    fireEvent.click(screen.getByRole("button", { name: "Store Secret" }));
    await waitFor(() => expect(calls.some((c) => c.url === "/api/secrets" && c.method === "POST")).toBe(true));
    const post = calls.find((c) => c.url === "/api/secrets" && c.method === "POST");
    expect(post?.body).toEqual({ name: "github-work", plaintext: "ghp_v2" });
    expect(post?.body as Record<string, unknown>).not.toHaveProperty("allowed_bindings");
  });
});

describe("SandboxedWidgetFrame message validation", () => {
  it("accepts only well-formed Requests from its own frame for its own widget_id", () => {
    const onReq = vi.fn().mockResolvedValue(undefined);
    render(
      <div>
        <SandboxedWidgetFrame
          defId="engineering.pr_badge_widget"
          widgetId="w_badge"
          adapterInstance="pr_table_adapter"
          generation={3}
          props={{ open_count: 2 }}
          onWidgetRequest={onReq}
        />
        <SandboxedWidgetFrame
          defId="engineering.pr_badge_widget"
          widgetId="w_other"
          adapterInstance="pr_table_adapter"
          generation={3}
          props={{}}
          onWidgetRequest={vi.fn().mockResolvedValue(undefined)}
        />
      </div>
    );
    const frame = screen.getByTitle("Custom widget w_badge") as HTMLIFrameElement;
    const otherFrame = screen.getByTitle("Custom widget w_other") as HTMLIFrameElement;
    expect(frame.getAttribute("sandbox")).toBe("allow-scripts");
    const ok = { protocol: "dash.widget/1", kind: "request", widget_id: "w_badge", request: "refresh_requested" };
    const send = (data: unknown, source: Window | null) =>
      act(() => {
        window.dispatchEvent(new MessageEvent("message", { data, source }));
      });

    send(ok, window); // the shell itself / a foreign window
    send(ok, otherFrame.contentWindow); // another widget's frame impersonating w_badge
    send(ok, null);
    send({ ...ok, widget_id: "w_other" }, frame.contentWindow); // claims another widget_id
    send({ ...ok, protocol: "dash.widget/2" }, frame.contentWindow);
    send({ ...ok, kind: "props" }, frame.contentWindow);
    send({ ...ok, request: "" }, frame.contentWindow);
    send({ ...ok, request: 42 }, frame.contentWindow);
    send("dash.widget/1 request", frame.contentWindow);
    send({ ...ok, payload: { blob: "x".repeat(20_000) } }, frame.contentWindow); // oversized
    expect(onReq).not.toHaveBeenCalled();

    // An array payload is not trusted as an object; the host-rendered Generation is used.
    send({ ...ok, payload: ["a"] }, frame.contentWindow);
    expect(onReq).toHaveBeenCalledTimes(1);
    expect(onReq.mock.calls[0][0]).toEqual({
      adapterInstance: "pr_table_adapter",
      widgetId: "w_badge",
      generation: 3,
      request: "refresh_requested",
      payload: {},
    });
  });
});
