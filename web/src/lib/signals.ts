/**
 * Browser client for the `/ws` Signal protocol (§6.2, §7.1, §8, §10.1).
 *
 * - `subscribe_dashboard`: the server pushes a `dashboard_render` (`protocol: "dash.render/1"`)
 *   initially and whenever the Dashboard's State, an upstream Query Signal or the active
 *   Generation changes. Renders coalesce server-side; the latest one always wins here.
 * - `subscribe_query`: `query_signal` envelopes (`dash.query/1`) carry snapshots and keyed
 *   deltas. The server answers each `subscribe_query` (in request order) with `subscribed
 *   {query_key, instance}` or an `error {instance}`; that binds the subscription to its exact
 *   `query_key`, so subscriptions to one Instance with different params never mix. Deltas are
 *   applied only on top of their `base_version`; a gap sends `resync_query`, which makes the
 *   server send a fresh snapshot. Dropping the last listener sends `unsubscribe_query`.
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
/** A `/ws` `error` frame: `code` (e.g. `render_failed`, `forbidden`, `subscription_limit`). */
export interface SignalError {
  code: string;
  message: string;
  dashboardId?: string;
  instance?: string;
  queryKey?: string;
}
type ErrorListener = (message: string, dashboardId?: string, error?: SignalError) => void;

interface QuerySub {
  instance: string;
  params: unknown;
  queryKey: string | null;
  state: LocalQueryState | null;
  listeners: Set<QueryListener>;
  /** Removed locally (last listener gone); a late `subscribed` is answered with an unsubscribe. */
  closed: boolean;
  /** Waiting for the snapshot requested by `resync_query` after a sequence gap. */
  resyncing: boolean;
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
  /** Per Instance, subscriptions awaiting the server's `subscribed`/`error` reply, in request order. */
  private pendingQueries = new Map<string, QuerySub[]>();
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
      this.pendingQueries.clear();
      for (const sub of this.queries.values()) {
        sub.queryKey = null;
        sub.resyncing = false;
        this.requestQuery(sub);
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
      sub = {
        instance,
        params: params ?? {},
        queryKey: null,
        state: null,
        listeners: new Set(),
        closed: false,
        resyncing: false,
      };
      this.queries.set(id, sub);
      this.requestQuery(sub);
    } else if (sub.state) {
      listener(sub.state);
    }
    sub.listeners.add(listener);
    this.connect();
    return () => {
      const s = this.queries.get(id);
      if (!s) return;
      s.listeners.delete(listener);
      if (s.listeners.size > 0) return;
      this.queries.delete(id);
      s.closed = true;
      // Unbound subscriptions are released when their `subscribed` reply arrives.
      if (s.queryKey !== null) this.releaseQueryKey(s.queryKey);
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

  /** Send `subscribe_query` and queue the subscription for the server's in-order reply. */
  private requestQuery(sub: QuerySub) {
    const ws = this.ws;
    if (!ws || ws.readyState !== 1) return; // replayed by `onopen`
    const q = this.pendingQueries.get(sub.instance) ?? [];
    q.push(sub);
    this.pendingQueries.set(sub.instance, q);
    this.send({ type: "subscribe_query", instance: sub.instance, params: sub.params });
  }

  private takePending(instance: string): QuerySub | undefined {
    const q = this.pendingQueries.get(instance);
    const sub = q?.shift();
    if (q && q.length === 0) this.pendingQueries.delete(instance);
    return sub;
  }

  /** Stop the server forwarder for `queryKey` unless another live subscription shares it. */
  private releaseQueryKey(queryKey: string) {
    for (const s of this.queries.values()) if (s.queryKey === queryKey) return;
    this.send({ type: "unsubscribe_query", query_key: queryKey });
  }

  private handleSubscribed(queryKey: string, instance: string) {
    const sub = this.takePending(instance);
    if (!sub) return;
    if (sub.closed) {
      this.releaseQueryKey(queryKey);
      return;
    }
    sub.queryKey = queryKey;
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
      case "query_signal":
        this.handleQuerySignal(msg.envelope as QueryEnvelope);
        return;
      case "state_signal":
        for (const l of this.stateListeners) l(msg.payload as StateSignal);
        return;
      case "subscribed":
        if (typeof msg.query_key !== "string") return;
        if (typeof msg.replaces === "string") {
          // Unsolicited: a new Generation moved an existing query to a new query_key. Follow
          // it (a resync snapshot on the new key follows); this is not a reply to a pending
          // `subscribe_query`.
          for (const s of this.queries.values()) {
            if (s.queryKey === msg.replaces) s.queryKey = msg.query_key;
          }
          return;
        }
        // The in-order reply to a `subscribe_query`: bind it to its exact query_key.
        if (typeof msg.instance === "string") this.handleSubscribed(msg.query_key, msg.instance);
        return;
      case "error": {
        const err: SignalError = {
          code: String(msg.code ?? "error"),
          message: String(msg.message ?? "error"),
          dashboardId: typeof msg.dashboard_id === "string" ? msg.dashboard_id : undefined,
          instance: typeof msg.instance === "string" ? msg.instance : undefined,
          queryKey: typeof msg.query_key === "string" ? msg.query_key : undefined,
        };
        // A refused `subscribe_query` (query_denied, subscription_limit) answers the oldest
        // pending request for its Instance; it carries no query_key.
        if (err.instance && !err.queryKey && !err.dashboardId) this.takePending(err.instance);
        for (const l of this.errorListeners) l(err.message, err.dashboardId, err);
        return;
      }
      default:
        return;
    }
  }

  private handleQuerySignal(env: QueryEnvelope) {
    if (!env || env.protocol !== QUERY_PROTOCOL) return;
    // Bound by the exact query_key from `subscribed`; several local subscriptions may share
    // one key when their params canonicalize to the same query.
    let gap = false;
    for (const sub of this.queries.values()) {
      if (sub.queryKey !== env.query_key) continue;
      if (sub.resyncing && env.kind !== "snapshot") continue;
      const res = applyQueryEnvelope(sub.state, env);
      if (!res.ok) {
        // Sequence gap: never apply out of order; ask for a fresh snapshot.
        sub.state = null;
        sub.resyncing = true;
        gap = true;
        continue;
      }
      sub.resyncing = false;
      sub.state = res.state;
      if (res.changed) for (const l of sub.listeners) l(res.state);
    }
    if (gap) this.send({ type: "resync_query", query_key: env.query_key });
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

export interface DashboardSignals {
  status: ConnectionStatus;
  /**
   * True once the server delivered a `dashboard_render` for this Dashboard on the current
   * connection, and until it reports an `error` for it (render failure, refusal) or the socket
   * leaves `open`. Only then do pushes reflect the user's own writes.
   */
  subscribed: boolean;
  /** Last subscription error for this Dashboard; cleared by the next successful render. */
  lastError: SignalError | null;
  freshness: Record<string, SignalFreshness>;
  lastPushAt: string | null;
}

/**
 * Subscribe the active Dashboard to live renders. `onRender` receives every pushed
 * `dashboard_render` for `dashboardId`; `onError` every `/ws` error reported for it. The hook
 * reports connection status, whether the subscription is healthy, the last error and the
 * latest per-Instance Signal freshness.
 */
export function useDashboardSignals(
  dashboardId: string | null,
  onRender: (msg: DashboardRenderMessage) => void,
  onStateSignal?: (sig: StateSignal) => void,
  onError?: (err: SignalError) => void
): DashboardSignals {
  const client = getSignalClient();
  const [status, setStatus] = useState<ConnectionStatus>(client.getStatus());
  const [subscribed, setSubscribed] = useState(false);
  const [lastError, setLastError] = useState<SignalError | null>(null);
  const [freshness, setFreshness] = useState<Record<string, SignalFreshness>>({});
  const [lastPushAt, setLastPushAt] = useState<string | null>(null);
  const renderRef = useRef(onRender);
  renderRef.current = onRender;
  const stateRef = useRef(onStateSignal);
  stateRef.current = onStateSignal;
  const errorRef = useRef(onError);
  errorRef.current = onError;

  useEffect(
    () =>
      client.onStatus((s) => {
        setStatus(s);
        // A new connection must deliver a fresh render before it counts as subscribed.
        if (s !== "open") setSubscribed(false);
      }),
    [client]
  );

  useEffect(() => {
    setSubscribed(false);
    setLastError(null);
    if (!dashboardId) return;
    setFreshness({});
    const offState = client.onState((sig) => {
      if (sig.scope === "dashboard" && sig.owner === dashboardId) stateRef.current?.(sig);
    });
    const offError = client.onError((_message, id, err) => {
      if (id !== dashboardId || !err) return;
      // The server reports each distinct render error once and sends no render until it
      // recovers, so stay unsubscribed until the next `dashboard_render`.
      setSubscribed(false);
      setLastError(err);
      errorRef.current?.(err);
    });
    const offRender = client.subscribeDashboard(dashboardId, (msg) => {
      setSubscribed(true);
      setLastError(null);
      setFreshness(msg.signals ?? {});
      setLastPushAt(new Date().toISOString());
      renderRef.current(msg);
    });
    return () => {
      offRender();
      offError();
      offState();
    };
  }, [client, dashboardId]);

  return { status, subscribed: status === "open" && subscribed, lastError, freshness, lastPushAt };
}
