/**
 * Browser client for the `/ws` Signal protocol (§6.2, §7.1, §8, §10.1).
 *
 * - `subscribe_dashboard`: the server pushes a `dashboard_render` (`protocol: "dash.render/1"`)
 *   initially and whenever the Dashboard's State, an upstream Query Signal or the active
 *   Generation changes. Renders coalesce server-side; the latest one always wins here.
 * - `subscribe_query`: `query_signal` envelopes (`dash.query/1`) carry snapshots and keyed
 *   deltas. Deltas are applied only on top of their `base_version`; a gap triggers a
 *   resubscribe, which makes the server send a fresh snapshot.
 * - `state_signal`: Dashboard/Instance State Signals.
 *
 * The socket reconnects with backoff and replays every active subscription.
 */
import { useEffect, useRef, useState } from "react";
import type { DashboardDoc, RenderedDashboard } from "./api";

export const DASHBOARD_RENDER_PROTOCOL = "dash.render/1";
export const QUERY_PROTOCOL = "dash.query/1";

export type DeltaOp =
  | { op: "insert" | "update"; key: string; value: unknown }
  | { op: "delete"; key: string };

export interface QueryEnvelope {
  protocol: string;
  instance: string;
  query_key: string;
  generation: number;
  version: number;
  base_version?: number;
  status: "fresh" | "stale" | "loading" | "error";
  updated_at: string;
  schema?: string;
  kind: "snapshot" | "delta";
  value?: unknown;
  operations?: DeltaOp[];
  error?: string;
}

export interface LocalQueryState {
  instance: string;
  query_key: string;
  version: number;
  generation: number;
  status: QueryEnvelope["status"];
  updated_at: string;
  error?: string;
  /** Latest value; for keyed results this is the row array after applying deltas. */
  value: unknown;
}

export type ApplyResult =
  | { ok: true; state: LocalQueryState; changed: boolean }
  | { ok: false; reason: "gap" | "no_snapshot" };

export interface SignalFreshness {
  status: QueryEnvelope["status"];
  updated_at: string;
  version: number;
  generation: number;
}

export interface DashboardRenderMessage {
  type: "dashboard_render";
  protocol: string;
  dashboard_id: string;
  generation: number;
  reason: string;
  signals: Record<string, SignalFreshness>;
  data: { document: DashboardDoc | null; rendered: RenderedDashboard };
}

export interface StateSignal {
  scope: string;
  owner: string;
  state_key: string;
  version: number;
  value: unknown;
  updated_at: string;
}

export type ConnectionStatus = "connecting" | "open" | "closed";

function rowKey(row: unknown, keyField: string): string | null {
  if (!row || typeof row !== "object") return null;
  const r = row as Record<string, unknown>;
  for (const f of [keyField, "id", "key", "number"]) {
    const v = r[f];
    if (v === undefined || v === null) continue;
    return typeof v === "string" ? v : typeof v === "number" ? String(v) : JSON.stringify(v);
  }
  return null;
}

/**
 * Apply one `dash.query/1` envelope to a local copy (mirrors the engine's
 * `apply_delta_envelope`). Snapshots replace; deltas must match `base_version`.
 */
export function applyQueryEnvelope(
  local: LocalQueryState | null,
  env: QueryEnvelope,
  keyField = "id"
): ApplyResult {
  const meta = {
    instance: env.instance,
    query_key: env.query_key,
    generation: env.generation,
    status: env.status,
    updated_at: env.updated_at,
    error: env.error,
  };
  if (env.kind === "snapshot") {
    if (local && env.version < local.version && env.generation === local.generation) {
      // Older snapshot delivered late: keep the newer local copy.
      return { ok: true, state: local, changed: false };
    }
    return { ok: true, state: { ...meta, version: env.version, value: env.value ?? null }, changed: true };
  }
  if (!local) return { ok: false, reason: "no_snapshot" };
  if (env.version <= local.version) {
    // Duplicate delivery of a delta already reflected locally.
    return { ok: true, state: local, changed: false };
  }
  if (env.base_version !== local.version) return { ok: false, reason: "gap" };
  const rows = Array.isArray(local.value) ? [...(local.value as unknown[])] : [];
  for (const op of env.operations ?? []) {
    const idx = rows.findIndex((r) => rowKey(r, keyField) === op.key);
    if (op.op === "delete") {
      if (idx >= 0) rows.splice(idx, 1);
    } else if (op.op === "update" && idx >= 0) {
      rows[idx] = op.value;
    } else {
      if (idx >= 0) rows.splice(idx, 1);
      rows.push(op.value);
    }
  }
  return { ok: true, state: { ...meta, version: env.version, value: rows }, changed: true };
}

type DashboardListener = (msg: DashboardRenderMessage) => void;
type QueryListener = (state: LocalQueryState) => void;
type StateListener = (sig: StateSignal) => void;
type StatusListener = (status: ConnectionStatus) => void;
type ErrorListener = (message: string, dashboardId?: string) => void;

interface QuerySub {
  instance: string;
  params: unknown;
  queryKey: string | null;
  state: LocalQueryState | null;
  listeners: Set<QueryListener>;
}

export interface SignalClientOptions {
  url?: string;
  WebSocketImpl?: typeof WebSocket;
  /** Delay before reconnect attempt `n` (1-based). */
  reconnectDelayMs?: (attempt: number) => number;
}

function defaultUrl(): string {
  const loc = typeof window !== "undefined" ? window.location : undefined;
  if (!loc) return "ws://127.0.0.1/ws";
  return `${loc.protocol === "https:" ? "wss" : "ws"}://${loc.host}/ws`;
}

export class SignalClient {
  private ws: WebSocket | null = null;
  private status: ConnectionStatus = "closed";
  private attempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closedByUser = false;
  private readonly url: string;
  private readonly WS: typeof WebSocket | undefined;
  private readonly delay: (attempt: number) => number;
  private dashboards = new Map<string, Set<DashboardListener>>();
  private queries = new Map<string, QuerySub>();
  private stateListeners = new Set<StateListener>();
  private statusListeners = new Set<StatusListener>();
  private errorListeners = new Set<ErrorListener>();

  constructor(opts: SignalClientOptions = {}) {
    this.url = opts.url ?? defaultUrl();
    this.WS = opts.WebSocketImpl ?? (typeof WebSocket !== "undefined" ? WebSocket : undefined);
    this.delay = opts.reconnectDelayMs ?? ((n) => Math.min(30_000, 500 * 2 ** Math.min(n - 1, 6)));
  }

  getStatus(): ConnectionStatus {
    return this.status;
  }

  connect(): void {
    if (!this.WS || this.ws) return;
    this.closedByUser = false;
    this.setStatus("connecting");
    let ws: WebSocket;
    try {
      ws = new this.WS(this.url);
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.onopen = () => {
      this.attempts = 0;
      this.setStatus("open");
      for (const id of this.dashboards.keys()) this.send({ type: "subscribe_dashboard", dashboard_id: id });
      for (const sub of this.queries.values()) {
        sub.queryKey = null;
        this.send({ type: "subscribe_query", instance: sub.instance, params: sub.params });
      }
    };
    ws.onmessage = (ev: MessageEvent) => this.handleMessage(ev.data);
    ws.onclose = () => {
      this.ws = null;
      this.setStatus("closed");
      if (!this.closedByUser) this.scheduleReconnect();
    };
    ws.onerror = () => {
      // `onclose` follows and handles reconnection.
    };
  }

  close(): void {
    this.closedByUser = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.ws?.close();
    this.ws = null;
    this.setStatus("closed");
  }

  subscribeDashboard(dashboardId: string, listener: DashboardListener): () => void {
    let set = this.dashboards.get(dashboardId);
    if (!set) {
      set = new Set();
      this.dashboards.set(dashboardId, set);
      this.send({ type: "subscribe_dashboard", dashboard_id: dashboardId });
    }
    set.add(listener);
    this.connect();
    return () => {
      const s = this.dashboards.get(dashboardId);
      if (!s) return;
      s.delete(listener);
      if (s.size === 0) {
        this.dashboards.delete(dashboardId);
        this.send({ type: "unsubscribe_dashboard", dashboard_id: dashboardId });
      }
    };
  }

  subscribeQuery(instance: string, params: unknown, listener: QueryListener): () => void {
    const id = `${instance}\u0000${JSON.stringify(params ?? {})}`;
    let sub = this.queries.get(id);
    if (!sub) {
      sub = { instance, params: params ?? {}, queryKey: null, state: null, listeners: new Set() };
      this.queries.set(id, sub);
      this.send({ type: "subscribe_query", instance, params: sub.params });
    } else if (sub.state) {
      listener(sub.state);
    }
    sub.listeners.add(listener);
    this.connect();
    return () => {
      const s = this.queries.get(id);
      if (!s) return;
      s.listeners.delete(listener);
      if (s.listeners.size === 0) this.queries.delete(id);
    };
  }

  onState(listener: StateListener): () => void {
    this.stateListeners.add(listener);
    return () => this.stateListeners.delete(listener);
  }

  onStatus(listener: StatusListener): () => void {
    this.statusListeners.add(listener);
    return () => this.statusListeners.delete(listener);
  }

  onError(listener: ErrorListener): () => void {
    this.errorListeners.add(listener);
    return () => this.errorListeners.delete(listener);
  }

  private setStatus(s: ConnectionStatus) {
    if (this.status === s) return;
    this.status = s;
    for (const l of this.statusListeners) l(s);
  }

  private scheduleReconnect() {
    if (this.closedByUser || this.reconnectTimer) return;
    if (this.dashboards.size === 0 && this.queries.size === 0) return;
    this.attempts += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, this.delay(this.attempts));
  }

  private send(msg: Record<string, unknown>) {
    const ws = this.ws;
    // 1 === WebSocket.OPEN; subscriptions made while connecting are replayed by `onopen`.
    if (ws && ws.readyState === 1) ws.send(JSON.stringify(msg));
  }

  private handleMessage(raw: unknown) {
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(String(raw));
    } catch {
      return;
    }
    switch (msg.type) {
      case "dashboard_render": {
        const m = msg as unknown as DashboardRenderMessage;
        if (m.protocol !== undefined && m.protocol !== DASHBOARD_RENDER_PROTOCOL) return;
        for (const l of this.dashboards.get(m.dashboard_id) ?? []) l(m);
        return;
      }
      case "subscribed":
        // A new Generation moved the query to a new query_key: follow it (a resync snapshot
        // on the new key follows).
        if (typeof msg.replaces === "string" && typeof msg.query_key === "string") {
          for (const s of this.queries.values()) {
            if (s.queryKey === msg.replaces) s.queryKey = msg.query_key;
          }
        }
        return;
      case "query_signal":
        this.handleQuerySignal(msg.envelope as QueryEnvelope);
        return;
      case "state_signal":
        for (const l of this.stateListeners) l(msg.payload as StateSignal);
        return;
      case "error":
        for (const l of this.errorListeners) {
          l(String(msg.message ?? "error"), msg.dashboard_id as string | undefined);
        }
        return;
      default:
        return;
    }
  }

  private handleQuerySignal(env: QueryEnvelope) {
    if (!env || env.protocol !== QUERY_PROTOCOL) return;
    let sub: QuerySub | undefined;
    for (const s of this.queries.values()) {
      if (s.queryKey === env.query_key) {
        sub = s;
        break;
      }
    }
    if (!sub && env.kind === "snapshot") {
      // First snapshot after (re)subscribing binds the pending subscription for this Instance.
      for (const s of this.queries.values()) {
        if (s.queryKey === null && s.instance === env.instance) {
          sub = s;
          sub.queryKey = env.query_key;
          break;
        }
      }
    }
    if (!sub) return;
    const res = applyQueryEnvelope(sub.state, env);
    if (!res.ok) {
      // Sequence gap: never apply out of order; ask for a fresh snapshot.
      sub.state = null;
      sub.queryKey = null;
      this.send({ type: "subscribe_query", instance: sub.instance, params: sub.params });
      return;
    }
    sub.state = res.state;
    if (res.changed) for (const l of sub.listeners) l(res.state);
  }
}

let sharedClient: SignalClient | null = null;

/** The tab's single Signal connection. */
export function getSignalClient(): SignalClient {
  if (!sharedClient) sharedClient = new SignalClient();
  return sharedClient;
}

/** Replace the shared client (tests). */
export function setSignalClient(client: SignalClient | null) {
  sharedClient?.close();
  sharedClient = client;
}

/**
 * Subscribe the active Dashboard to live renders. `onRender` receives every pushed
 * `dashboard_render` for `dashboardId`; the hook reports connection status and the latest
 * per-Instance Signal freshness.
 */
export function useDashboardSignals(
  dashboardId: string | null,
  onRender: (msg: DashboardRenderMessage) => void,
  onStateSignal?: (sig: StateSignal) => void
): { status: ConnectionStatus; freshness: Record<string, SignalFreshness>; lastPushAt: string | null } {
  const client = getSignalClient();
  const [status, setStatus] = useState<ConnectionStatus>(client.getStatus());
  const [freshness, setFreshness] = useState<Record<string, SignalFreshness>>({});
  const [lastPushAt, setLastPushAt] = useState<string | null>(null);
  const renderRef = useRef(onRender);
  renderRef.current = onRender;
  const stateRef = useRef(onStateSignal);
  stateRef.current = onStateSignal;

  useEffect(() => client.onStatus(setStatus), [client]);

  useEffect(() => {
    if (!dashboardId) return;
    setFreshness({});
    const offState = client.onState((sig) => {
      if (sig.scope === "dashboard" && sig.owner === dashboardId) stateRef.current?.(sig);
    });
    const offRender = client.subscribeDashboard(dashboardId, (msg) => {
      setFreshness(msg.signals ?? {});
      setLastPushAt(new Date().toISOString());
      renderRef.current(msg);
    });
    return () => {
      offRender();
      offState();
    };
  }, [client, dashboardId]);

  return { status, freshness, lastPushAt };
}
