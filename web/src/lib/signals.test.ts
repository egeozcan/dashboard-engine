import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { applyQueryEnvelope, SignalClient, type QueryEnvelope } from "./signals";
import { MockWebSocket } from "../test/mockWebSocket";

const snap = (version: number, value: unknown, extra: Partial<QueryEnvelope> = {}): QueryEnvelope => ({
  protocol: "dash.query/1",
  instance: "open_prs",
  query_key: "qk1",
  generation: 1,
  version,
  status: "fresh",
  updated_at: "2026-11-24T10:00:00Z",
  kind: "snapshot",
  value,
  ...extra,
});

const delta = (base: number, version: number, operations: QueryEnvelope["operations"]): QueryEnvelope => ({
  protocol: "dash.query/1",
  instance: "open_prs",
  query_key: "qk1",
  generation: 1,
  version,
  base_version: base,
  status: "fresh",
  updated_at: "2026-11-24T10:01:00Z",
  kind: "delta",
  operations,
});

describe("applyQueryEnvelope (dash.query/1)", () => {
  it("applies keyed deltas on their base_version and detects gaps", () => {
    const s1 = applyQueryEnvelope(null, snap(16, [{ id: "a#1", state: "open" }, { id: "a#2", state: "open" }]));
    expect(s1.ok).toBe(true);
    if (!s1.ok) return;
    const s2 = applyQueryEnvelope(
      s1.state,
      delta(16, 17, [
        { op: "update", key: "a#1", value: { id: "a#1", state: "merged" } },
        { op: "delete", key: "a#2" },
        { op: "insert", key: "a#3", value: { id: "a#3", state: "open" } },
      ])
    );
    expect(s2.ok).toBe(true);
    if (!s2.ok) return;
    expect(s2.state.version).toBe(17);
    expect(s2.state.value).toEqual([
      { id: "a#1", state: "merged" },
      { id: "a#3", state: "open" },
    ]);

    // Duplicate delivery of an already-applied delta is ignored.
    const dup = applyQueryEnvelope(s2.state, delta(16, 17, []));
    expect(dup.ok && !dup.changed).toBe(true);

    // A delta whose base is not the local version must never be applied out of order.
    const gap = applyQueryEnvelope(s2.state, delta(18, 19, [{ op: "delete", key: "a#1" }]));
    expect(gap).toEqual({ ok: false, reason: "gap" });

    // A delta before any snapshot needs a snapshot first.
    expect(applyQueryEnvelope(null, delta(1, 2, []))).toEqual({ ok: false, reason: "no_snapshot" });
  });

  it("keeps stale metadata and ignores late older snapshots", () => {
    const s = applyQueryEnvelope(null, snap(5, [1], { status: "stale", error: "upstream down" }));
    expect(s.ok && s.state.status).toBe("stale");
    if (!s.ok) return;
    const late = applyQueryEnvelope(s.state, snap(4, [0]));
    expect(late.ok && late.state.version).toBe(5);
  });
});

describe("SignalClient", () => {
  beforeEach(() => MockWebSocket.reset());
  afterEach(() => vi.useRealTimers());

  const makeClient = () =>
    new SignalClient({
      url: "ws://test/ws",
      WebSocketImpl: MockWebSocket as unknown as typeof WebSocket,
      reconnectDelayMs: () => 10,
    });

  it("subscribes Dashboards on open, delivers pushed renders and replays after reconnect", () => {
    vi.useFakeTimers();
    const client = makeClient();
    const onRender = vi.fn();
    const unsubscribe = client.subscribeDashboard("engineering", onRender);
    const ws = MockWebSocket.latest();
    ws.open();
    expect(ws.sent).toContainEqual({ type: "subscribe_dashboard", dashboard_id: "engineering" });

    ws.receive({
      type: "dashboard_render",
      protocol: "dash.render/1",
      dashboard_id: "engineering",
      generation: 3,
      reason: "state",
      signals: {},
      data: { document: null, rendered: { dashboard_id: "engineering", generation: 3 } },
    });
    expect(onRender).toHaveBeenCalledTimes(1);
    expect(onRender.mock.calls[0][0].reason).toBe("state");

    // Unknown protocol versions are ignored.
    ws.receive({ type: "dashboard_render", protocol: "dash.render/9", dashboard_id: "engineering", data: {} });
    expect(onRender).toHaveBeenCalledTimes(1);

    // Connection drops -> reconnect with backoff and resubscribe.
    ws.drop();
    expect(client.getStatus()).toBe("closed");
    vi.advanceTimersByTime(10);
    const ws2 = MockWebSocket.latest();
    expect(ws2).not.toBe(ws);
    ws2.open();
    expect(ws2.sent).toContainEqual({ type: "subscribe_dashboard", dashboard_id: "engineering" });

    unsubscribe();
    expect(ws2.sent).toContainEqual({ type: "unsubscribe_dashboard", dashboard_id: "engineering" });
    client.close();
  });

  it("resubscribes a query for a fresh snapshot on a sequence gap", () => {
    const client = makeClient();
    const states: unknown[] = [];
    client.subscribeQuery("open_prs", { repository: "acme/platform" }, (s) => states.push(s.value));
    const ws = MockWebSocket.latest();
    ws.open();
    const subscribe = { type: "subscribe_query", instance: "open_prs", params: { repository: "acme/platform" } };
    expect(ws.sent.filter((m) => m.type === "subscribe_query")).toEqual([subscribe]);

    ws.receive({ type: "query_signal", envelope: snap(1, [{ id: "x", n: 1 }]) });
    ws.receive({
      type: "query_signal",
      envelope: delta(1, 2, [{ op: "update", key: "x", value: { id: "x", n: 2 } }]),
    });
    expect(states).toEqual([[{ id: "x", n: 1 }], [{ id: "x", n: 2 }]]);

    ws.receive({ type: "query_signal", envelope: delta(5, 6, [{ op: "delete", key: "x" }]) });
    expect(states).toHaveLength(2);
    expect(ws.sent.filter((m) => m.type === "subscribe_query")).toEqual([subscribe, subscribe]);

    ws.receive({ type: "query_signal", envelope: snap(6, [{ id: "y", n: 1 }]) });
    expect(states[2]).toEqual([{ id: "y", n: 1 }]);
    client.close();
  });

  it("follows a query subscription re-bound to a new query_key by a Generation change", () => {
    const client = makeClient();
    const states: unknown[] = [];
    client.subscribeQuery("open_prs", { repository: "acme/platform" }, (s) => states.push(s.value));
    const ws = MockWebSocket.latest();
    ws.open();
    ws.receive({ type: "subscribed", query_key: "qk1", instance: "open_prs" });
    ws.receive({ type: "query_signal", envelope: snap(1, [{ id: "x", n: 1 }]) });

    ws.receive({ type: "subscribed", query_key: "qk2", instance: "open_prs", replaces: "qk1" });
    ws.receive({
      type: "query_signal",
      resync: true,
      envelope: snap(4, [{ id: "x", n: 2 }], { query_key: "qk2", generation: 2 }),
    });
    ws.receive({
      type: "query_signal",
      envelope: { ...delta(4, 5, [{ op: "delete", key: "x" }]), query_key: "qk2", generation: 2 },
    });
    expect(states).toEqual([[{ id: "x", n: 1 }], [{ id: "x", n: 2 }], []]);
    // No resubscribe was needed.
    expect(ws.sent.filter((m) => m.type === "subscribe_query")).toHaveLength(1);
    client.close();
  });

  it("forwards Dashboard State Signals and errors", () => {
    const client = makeClient();
    const onState = vi.fn();
    const onError = vi.fn();
    client.onState(onState);
    client.onError(onError);
    client.subscribeDashboard("engineering", vi.fn());
    const ws = MockWebSocket.latest();
    ws.open();
    ws.receive({
      type: "state_signal",
      payload: { scope: "dashboard", owner: "engineering", state_key: "selected_repository", value: "acme/payments" },
    });
    ws.receive({ type: "error", dashboard_id: "engineering", message: "boom" });
    expect(onState).toHaveBeenCalledWith(expect.objectContaining({ value: "acme/payments" }));
    expect(onError).toHaveBeenCalledWith("boom", "engineering");
    client.close();
  });
});
