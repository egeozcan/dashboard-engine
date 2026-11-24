import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  apiFetch,
  type DashboardDoc,
  type DashboardPlacement,
  type GraphInspectResponse,
  type LayoutOverride,
  type RenderedDashboard,
  type WidgetNode,
  type WidgetRequestDryRun,
} from "../lib/api";
import { WidgetRenderer, type WidgetRequestContext } from "../components/widgets/BuiltInWidgets";
import {
  applyDesignerEdits,
  applyOverridesToTree,
  configurablePropsFor,
  describeEdits,
  GRID_COLUMNS,
  gridItemStyle,
  groupPlacementsByArea,
  ROW_UNIT_PX,
  type DesignerEdit,
  type OverridePatch,
} from "../lib/designer";
import { useDashboardSignals, type DashboardRenderMessage, type SignalError } from "../lib/signals";

function findNode(node: WidgetNode, widgetId: string): WidgetNode | null {
  if (node.widget_id === widgetId) return node;
  for (const c of node.children ?? []) {
    const hit = findNode(c, widgetId);
    if (hit) return hit;
  }
  return null;
}

function formatTime(iso: string | null | undefined): string {
  if (!iso) return "";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleTimeString();
}

export const DashboardView: React.FC = () => {
  const [dashboards, setDashboards] = useState<DashboardDoc[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [rendered, setRendered] = useState<RenderedDashboard | null>(null);
  const [doc, setDoc] = useState<DashboardDoc | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [banner, setBanner] = useState<string | null>(null);
  const [designerMode, setDesignerMode] = useState(false);
  const [viewport, setViewport] = useState<"desktop" | "tablet" | "mobile">("desktop");
  const [selectedWidgetId, setSelectedWidgetId] = useState<string | null>(null);
  const [edits, setEdits] = useState<DesignerEdit[]>([]);
  const [saving, setSaving] = useState(false);
  const [adapterInstances, setAdapterInstances] = useState<string[]>([]);

  // Action confirmation modal showing the Adapter-resolved action and its Dry run.
  const confirmDialogRef = useRef<HTMLDialogElement>(null);
  const [pendingRequest, setPendingRequest] = useState<{
    ctx: WidgetRequestContext;
    dryRun: WidgetRequestDryRun;
  } | null>(null);

  const loadDashboards = async () => {
    try {
      const list = await apiFetch<DashboardDoc[]>("/api/dashboards");
      setDashboards(list);
      setActiveId((cur) => (cur && list.some((d) => d.id === cur) ? cur : list[0]?.id ?? null));
    } catch (e) {
      setError(String(e));
    }
  };

  const loadActiveDashboard = useCallback(async (id: string) => {
    try {
      setError(null);
      const resp = await apiFetch<{ document: DashboardDoc | null; rendered: RenderedDashboard }>(
        `/api/dashboards/${encodeURIComponent(id)}`
      );
      setDoc(resp.document);
      setRendered(resp.rendered);
    } catch (e) {
      setError(String(e));
    }
  }, []);

  useEffect(() => {
    void loadDashboards();
  }, []);

  useEffect(() => {
    setEdits([]);
    setSelectedWidgetId(null);
    if (activeId) {
      void loadActiveDashboard(activeId);
    }
  }, [activeId, loadActiveDashboard]);

  // Live Signals: the server re-renders and pushes whenever Dashboard State, an upstream
  // Query Signal or the active Generation changes (other Clients, polling, Agents, automations).
  const onRender = useCallback(
    (msg: DashboardRenderMessage) => {
      if (msg.dashboard_id !== activeId) return;
      setRendered(msg.data.rendered);
      if (msg.data.document) setDoc(msg.data.document);
    },
    [activeId]
  );
  const onStateSignal = useCallback(
    (sig: { state_key: string; value: unknown; version?: number }) => {
      // Keep the CAS version in step with the value so the next write is not a false conflict.
      setRendered((r) =>
        r
          ? {
              ...r,
              dashboard_state: { ...r.dashboard_state, [sig.state_key]: sig.value },
              dashboard_state_versions:
                typeof sig.version === "number"
                  ? { ...(r.dashboard_state_versions ?? {}), [sig.state_key]: sig.version }
                  : r.dashboard_state_versions,
            }
          : r
      );
    },
    []
  );
  const onSignalError = useCallback((err: SignalError) => {
    // The Dashboard may have been removed or broken by a newly activated Generation: refresh
    // the selector so a vanished Dashboard drops out.
    if (err.code === "render_failed") void loadDashboards();
  }, []);
  const live = useDashboardSignals(activeId, onRender, onStateSignal, onSignalError);

  // Pushes reflect the user's own writes only while the subscription is healthy (socket open
  // AND the server delivering renders for this Dashboard); otherwise reload over HTTP.
  const refreshIfNotLive = async () => {
    if (!live.subscribed && activeId) await loadActiveDashboard(activeId);
  };

  const handleDashboardStateChange = async (key: string, value: unknown) => {
    if (!activeId) return;
    // Compare-and-swap against the version this view rendered (0 = key not yet stored). On a
    // conflict nothing is merged: the view reloads to show the other writer's value.
    const versions = rendered?.dashboard_state_versions;
    const expected_version = versions ? (versions[key] ?? 0) : undefined;
    try {
      setError(null);
      await apiFetch(`/api/dashboards/${encodeURIComponent(activeId)}/state`, {
        method: "POST",
        body: JSON.stringify({ key, value, expected_version }),
      });
      await refreshIfNotLive();
    } catch (e) {
      await loadActiveDashboard(activeId);
      setError(String(e));
    }
  };

  // `dryRunToken` binds the dispatch to the Dry run the user saw (or skipped confirming for a
  // refresh): the server refuses a dispatch that resolves differently (R7:SEC-1).
  const envelopeFor = (ctx: WidgetRequestContext, dryRunToken?: string | null) => ({
    protocol: "dash.widget/1",
    dashboard_id: activeId,
    adapter_instance: ctx.adapterInstance,
    widget_id: ctx.widgetId,
    generation: ctx.generation,
    sequence: Date.now(),
    kind: "request",
    request: ctx.request,
    payload: ctx.payload,
    ...(dryRunToken ? { dry_run_token: dryRunToken } : {}),
  });

  const executeWidgetRequest = async (ctx: WidgetRequestContext, dryRunToken?: string | null) => {
    try {
      const exec = await apiFetch<{
        outcome?: "action" | "refreshed";
        execution_id?: string;
        status: string;
        refreshed?: string[];
      }>("/api/widget-request", {
        method: "POST",
        body: JSON.stringify(envelopeFor(ctx, dryRunToken)),
      });
      setBanner(
        exec.outcome === "refreshed" || !exec.execution_id
          ? `Request '${ctx.request}' -> refreshed ${(exec.refreshed ?? []).join(", ") || "bound data"} (${exec.status})`
          : `Request '${ctx.request}' -> execution ${exec.execution_id} (${exec.status})`
      );
      await refreshIfNotLive();
    } catch (e) {
      setError(String(e));
    }
  };

  // Every Request is first resolved through its Adapter without side effects. Requests that
  // resolve to an action are confirmed with the action's Dry run; others run directly.
  const handleWidgetRequest = async (ctx: WidgetRequestContext) => {
    setError(null);
    let dryRun: WidgetRequestDryRun;
    try {
      dryRun = await apiFetch<WidgetRequestDryRun>("/api/widget-request/dry-run", {
        method: "POST",
        body: JSON.stringify(envelopeFor(ctx)),
      });
    } catch (e) {
      setError(String(e));
      return;
    }
    if (!dryRun.requires_confirmation) {
      await executeWidgetRequest(ctx, dryRun.dry_run_token);
      return;
    }
    setPendingRequest({ ctx, dryRun });
    confirmDialogRef.current?.showModal();
  };

  // ---- Dashboard Designer ----

  const effectiveDoc = useMemo(() => (doc ? applyDesignerEdits(doc, edits) : null), [doc, edits]);
  const overrides: Record<string, LayoutOverride> =
    effectiveDoc?.overrides ?? ((rendered?.overrides ?? {}) as Record<string, LayoutOverride>);

  const addEdit = (e: DesignerEdit) => setEdits((prev) => [...prev, e]);
  const onLayoutChange = (widgetId: string, patch: OverridePatch) =>
    addEdit({ kind: "override", widgetId, patch });

  useEffect(() => {
    if (!designerMode) return;
    apiFetch<GraphInspectResponse>("/api/graph")
      .then((g) =>
        setAdapterInstances(
          Object.values(g.instances ?? {})
            .filter((i) => i.category === "adapter")
            .map((i) => i.id)
            .sort()
        )
      )
      .catch(() => setAdapterInstances([]));
  }, [designerMode]);

  /**
   * Save all pending Designer edits as one Draft: begin a Draft, read the Dashboard from the
   * Draft (its base Revision), apply the edits there, write it and activate. Concurrent changes
   * since this page loaded are kept; a stale base is rejected by activation. Failed saves
   * abort the Draft so it does not leak; the edits stay here for a retry.
   */
  const saveDesignerEdits = async () => {
    if (!activeId || edits.length === 0) return;
    setSaving(true);
    setError(null);
    let draftId: string | null = null;
    try {
      const draft = await apiFetch<{ draft_id: string }>("/api/drafts", { method: "POST" });
      draftId = draft.draft_id;
      const dg = await apiFetch<GraphInspectResponse>(`/api/drafts/${draftId}/graph`);
      const baseDoc = dg.dashboards?.[activeId];
      if (!baseDoc) throw new Error(`dashboard '${activeId}' no longer exists in the active Revision`);
      const changedSinceLoad = doc !== null && JSON.stringify(baseDoc) !== JSON.stringify(doc);
      const nextDoc = applyDesignerEdits(baseDoc, edits);
      await apiFetch(`/api/drafts/${draftId}/dashboards`, {
        method: "POST",
        body: JSON.stringify(nextDoc),
      });
      const res = await apiFetch<{ generation_id?: number }>(`/api/drafts/${draftId}/activate`, {
        method: "POST",
      });
      draftId = null;
      setEdits([]);
      setBanner(
        `Saved ${describeEdits(edits)} and activated Generation #${res.generation_id ?? "?"}` +
          (changedSinceLoad ? " (applied on top of changes made since this page loaded)" : "")
      );
      await loadActiveDashboard(activeId);
    } catch (e) {
      setError(`Save failed: ${String(e)}`);
    } finally {
      if (draftId) {
        await apiFetch(`/api/drafts/${draftId}`, { method: "DELETE" }).catch(() => undefined);
      }
      setSaving(false);
    }
  };

  const viewportWidth =
    viewport === "mobile" ? "390px" : viewport === "tablet" ? "768px" : "100%";

  const stateKeys = useMemo(() => {
    const keys = new Set<string>([
      ...Object.keys(doc?.initial_state ?? {}),
      ...Object.keys(rendered?.dashboard_state ?? {}),
    ]);
    return [...keys].sort();
  }, [doc, rendered]);

  const placements: DashboardPlacement[] =
    effectiveDoc?.adapters ??
    (rendered?.adapters ?? []).map((a) => ({ adapter_instance: a.adapter_instance, area: a.area, query_params: {} }));
  const treeByAdapter = new Map((rendered?.adapters ?? []).map((a) => [a.adapter_instance, a.tree]));
  const isGridLayout = (rendered?.layout ?? effectiveDoc?.layout ?? "grid") !== "stack";

  const selectedNode = useMemo(() => {
    if (!selectedWidgetId || !rendered) return null;
    for (const a of rendered.adapters) {
      const hit = findNode(a.tree, selectedWidgetId);
      if (hit) return hit;
    }
    return null;
  }, [rendered, selectedWidgetId]);

  return (
    <div>
      <div className="card row" style={{ justifyContent: "space-between" }}>
        <div className="row">
          <label className="row">
            <strong>Dashboard:</strong>
            <select value={activeId ?? ""} onChange={(e) => setActiveId(e.target.value)}>
              {dashboards.map((d) => (
                <option key={d.id} value={d.id}>
                  {d.title} ({d.id})
                </option>
              ))}
            </select>
          </label>
          {rendered && <span className="badge">Generation #{rendered.generation}</span>}
          {activeId && (
            <span
              className={`badge ${live.subscribed ? "ok" : live.lastError ? "danger" : "warn"}`}
              title={live.lastPushAt ? `Last pushed render ${formatTime(live.lastPushAt)}` : undefined}
            >
              {live.subscribed
                ? "Live"
                : live.lastError
                  ? "Live updates failed"
                  : live.status === "open"
                    ? "Subscribing…"
                    : live.status === "connecting"
                      ? "Connecting…"
                      : "Offline (last render)"}
            </span>
          )}
        </div>
        <div className="row">
          <button
            type="button"
            className={`btn ${designerMode ? "primary" : ""}`}
            onClick={() => {
              const next = !designerMode;
              setDesignerMode(next);
              if (!next) {
                setSelectedWidgetId(null);
                setError(null);
              }
            }}
          >
            {designerMode ? "Exit Designer" : "Dashboard Designer"}
          </button>
          <button
            type="button"
            className="btn"
            onClick={() => activeId && void loadActiveDashboard(activeId)}
          >
            Refresh
          </button>
        </div>
      </div>

      {activeId && live.lastError && (
        <div className="card badge danger" role="alert" style={{ display: "block" }}>
          Live updates for this Dashboard stopped ({live.lastError.code}): {live.lastError.message}. Showing the
          last render; your changes reload it directly.
        </div>
      )}

      {stateKeys.length > 0 && (
        <DashboardStateBar
          keys={stateKeys}
          values={rendered?.dashboard_state ?? {}}
          onCommit={(k, v) => void handleDashboardStateChange(k, v)}
        />
      )}

      {Object.keys(live.freshness).length > 0 && (
        <div className="card row signal-strip" aria-label="Query Signal freshness">
          <strong style={{ fontSize: 12 }}>Signals:</strong>
          {Object.entries(live.freshness).map(([inst, f]) => (
            <span
              key={inst}
              className={`badge ${f.status === "fresh" ? "ok" : f.status === "error" ? "danger" : "warn"}`}
              title={`v${f.version}, updated ${f.updated_at}`}
            >
              {inst}: {f.status} · {formatTime(f.updated_at)}
            </span>
          ))}
        </div>
      )}

      {designerMode && (
        <div className="card col">
          <div className="row" style={{ justifyContent: "space-between" }}>
            <div className="row">
              <strong>Screen Preview:</strong>
              {(["desktop", "tablet", "mobile"] as const).map((vp) => (
                <button
                  key={vp}
                  type="button"
                  className={`btn ${viewport === vp ? "primary" : ""}`}
                  onClick={() => setViewport(vp)}
                >
                  {vp}
                </button>
              ))}
            </div>
            <div className="row">
              <span style={{ fontSize: 12, color: "var(--text-muted)" }}>
                {edits.length ? `Pending: ${describeEdits(edits)}` : "No pending changes"}
              </span>
              <button
                type="button"
                className="btn"
                disabled={edits.length === 0 || saving}
                onClick={() => setEdits((prev) => prev.slice(0, -1))}
              >
                Undo
              </button>
              <button
                type="button"
                className="btn"
                disabled={edits.length === 0 || saving}
                onClick={() => setEdits([])}
              >
                Discard changes
              </button>
              <button
                type="button"
                className="btn primary"
                disabled={edits.length === 0 || saving}
                onClick={() => void saveDesignerEdits()}
              >
                {saving ? "Saving…" : "Save & Activate"}
              </button>
            </div>
          </div>
          <div className="grid-2">
            <WidgetPropertyEditor
              node={selectedNode}
              override={selectedWidgetId ? overrides[selectedWidgetId] : undefined}
              onPatch={(patch) => selectedWidgetId && addEdit({ kind: "override", widgetId: selectedWidgetId, patch })}
              onProps={(props) =>
                selectedWidgetId && addEdit({ kind: "override_props", widgetId: selectedWidgetId, props })
              }
              onReset={() => selectedWidgetId && addEdit({ kind: "reset_override", widgetId: selectedWidgetId })}
            />
            <PlacementEditor
              placements={placements}
              adapterInstances={adapterInstances}
              onEdit={addEdit}
            />
          </div>
        </div>
      )}

      {error && <div className="card badge danger" style={{ display: "block" }}>{error}</div>}
      {banner && <div className="card badge ok" style={{ display: "block" }}>{banner}</div>}

      <div style={{ maxWidth: viewportWidth, margin: "0 auto", transition: "max-width 0.15s ease" }}>
        {rendered &&
          groupPlacementsByArea(placements).map((group) => {
            const fallbackSpan = Math.max(1, Math.floor(GRID_COLUMNS / group.placements.length));
            return (
              <section key={group.area} className="dashboard-area" aria-label={`Area ${group.area}`}>
                {designerMode && <div className="designer-area-label">area: {group.area}</div>}
                <div
                  style={
                    isGridLayout
                      ? {
                          display: "grid",
                          gridTemplateColumns: "repeat(12, minmax(0, 1fr))",
                          gridAutoRows: `minmax(${ROW_UNIT_PX}px, auto)`,
                          gap: 16,
                        }
                      : { display: "flex", flexDirection: "column", gap: 16 }
                  }
                >
                  {group.placements.map((p) => {
                    const tree = treeByAdapter.get(p.adapter_instance);
                    if (!tree) {
                      return (
                        <div
                          key={p.adapter_instance}
                          className="card"
                          style={isGridLayout ? { gridColumn: `span ${fallbackSpan}` } : undefined}
                        >
                          <code>{p.adapter_instance}</code>: rendered after Save &amp; Activate
                        </div>
                      );
                    }
                    const node = applyOverridesToTree(tree, overrides);
                    return (
                      <WidgetRenderer
                        key={p.adapter_instance}
                        node={node}
                        adapterInstance={p.adapter_instance}
                        generation={rendered.generation}
                        dashboardId={rendered.dashboard_id}
                        onWidgetRequest={handleWidgetRequest}
                        onDashboardStateChange={handleDashboardStateChange}
                        designerMode={designerMode}
                        onSelectWidget={setSelectedWidgetId}
                        selectedWidgetId={selectedWidgetId}
                        onLayoutChange={onLayoutChange}
                        inGrid={isGridLayout}
                        layoutStyle={
                          isGridLayout ? gridItemStyle(node.layout_override, fallbackSpan) : undefined
                        }
                      />
                    );
                  })}
                </div>
              </section>
            );
          })}
      </div>

      {/* Native modal <dialog>: confirm an action Request with its Dry run */}
      <dialog
        ref={confirmDialogRef}
        aria-labelledby="dryrun-dialog-title"
        onClose={() => {
          const dlg = confirmDialogRef.current;
          if (!dlg) return;
          const choice = dlg.returnValue;
          dlg.returnValue = "";
          if (choice === "confirm" && pendingRequest) {
            void executeWidgetRequest(pendingRequest.ctx, pendingRequest.dryRun.dry_run_token);
          }
          setPendingRequest(null);
        }}
      >
        <h2 id="dryrun-dialog-title" style={{ marginTop: 0 }}>
          Confirm action (Dry run)
        </h2>
        {pendingRequest && <DryRunSummary report={pendingRequest.dryRun} />}
        <div className="row" style={{ justifyContent: "flex-end", marginTop: 14 }}>
          <button
            type="button"
            className="btn"
            autoFocus
            onClick={() => confirmDialogRef.current?.close("cancel")}
          >
            Cancel
          </button>
          <button
            type="button"
            className="btn primary"
            onClick={() => confirmDialogRef.current?.close("confirm")}
          >
            Confirm &amp; Execute
          </button>
        </div>
      </dialog>
    </div>
  );
};

const DryRunSummary: React.FC<{ report: WidgetRequestDryRun }> = ({ report }) => (
  <div className="col" style={{ gap: 6 }}>
    <div>
      Request <code>{report.request}</code> from <code>{report.widget_id}</code>
      {report.action_instance ? (
        <>
          {" "}
          resolves to action <code>{report.action_instance}</code>
        </>
      ) : (
        <> could not be resolved without side effects</>
      )}
      .
    </div>
    {report.dry_run && (
      <div className="row">
        <span className={`badge ${report.dry_run.feasible ? "ok" : "danger"}`}>
          {report.dry_run.feasible ? "feasible" : "not feasible"}
        </span>
        <span className="badge">advisory estimate, no side effects performed</span>
      </div>
    )}
    {report.dry_run?.warnings?.map((w) => (
      <div key={w} className="badge warn" style={{ display: "block" }}>
        {w}
      </div>
    ))}
    {report.dry_run_error && (
      <div className="badge warn" style={{ display: "block" }}>
        Dry run unavailable: {report.dry_run_error}
      </div>
    )}
    {report.args !== undefined && report.args !== null && (
      <>
        <strong style={{ fontSize: 12 }}>Arguments</strong>
        <pre className="json-block">{JSON.stringify(report.args, null, 2)}</pre>
      </>
    )}
    {report.dry_run && (
      <>
        <strong style={{ fontSize: 12 }}>Estimated effects</strong>
        <pre className="json-block">{JSON.stringify(report.dry_run.effects, null, 2)}</pre>
      </>
    )}
  </div>
);

/** Generic editor for the Dashboard State keys this Dashboard declares or currently holds. */
const DashboardStateBar: React.FC<{
  keys: string[];
  values: Record<string, unknown>;
  onCommit: (key: string, value: unknown) => void;
}> = ({ keys, values, onCommit }) => (
  <div className="card row" aria-label="Dashboard State">
    <strong style={{ fontSize: 12 }}>Dashboard State:</strong>
    {keys.map((k) => (
      <DashboardStateField key={k} stateKey={k} value={values[k]} onCommit={onCommit} />
    ))}
  </div>
);

const DashboardStateField: React.FC<{
  stateKey: string;
  value: unknown;
  onCommit: (key: string, value: unknown) => void;
}> = ({ stateKey, value, onCommit }) => {
  // Reset on content, not identity: every pushed render re-parses object values. While the user
  // has uncommitted text, pushes never overwrite it.
  const serialized = typeof value === "string" ? value : JSON.stringify(value ?? null);
  const [draft, setDraft] = useState(serialized);
  const dirtyRef = useRef(false);
  useEffect(() => {
    if (!dirtyRef.current) setDraft(serialized);
  }, [serialized]);
  if (typeof value === "boolean") {
    return (
      <label className="row" style={{ fontSize: 12 }}>
        <input type="checkbox" checked={value} onChange={(e) => onCommit(stateKey, e.target.checked)} />
        <span>{stateKey}</span>
      </label>
    );
  }
  const commit = () => {
    dirtyRef.current = false;
    if (draft === serialized) return;
    if (typeof value === "string" || value === undefined) {
      onCommit(stateKey, draft);
      return;
    }
    try {
      onCommit(stateKey, JSON.parse(draft));
    } catch {
      onCommit(stateKey, draft);
    }
  };
  return (
    <label className="row" style={{ fontSize: 12 }}>
      <span>{stateKey}</span>
      <input
        type="text"
        aria-label={`Dashboard State ${stateKey}`}
        value={draft}
        onChange={(e) => {
          dirtyRef.current = true;
          setDraft(e.target.value);
        }}
        onBlur={commit}
        onKeyDown={(e) => {
          if (e.key === "Enter") commit();
        }}
      />
    </label>
  );
};

function numOrNull(raw: string): number | null {
  if (raw.trim() === "") return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

/** Property editor for the selected Widget node's layout override (keyed by `widget_id`). */
const WidgetPropertyEditor: React.FC<{
  node: WidgetNode | null;
  override: LayoutOverride | undefined;
  onPatch: (patch: OverridePatch) => void;
  onProps: (props: Record<string, unknown | null>) => void;
  onReset: () => void;
}> = ({ node, override, onPatch, onProps, onReset }) => {
  if (!node) {
    return (
      <div className="card" style={{ marginBottom: 0 }}>
        <strong>Widget properties</strong>
        <p style={{ color: "var(--text-muted)", fontSize: 13 }}>
          Select a widget to edit its layout override. Drag the ⠿ handle to move it and the corner
          handle to resize it on the grid.
        </p>
      </div>
    );
  }
  const ov = override ?? {};
  const specs = configurablePropsFor(node.widget_type);
  const geometry: Array<["x" | "y" | "w" | "h", string, number, number]> = [
    ["x", "Column start (x)", 0, GRID_COLUMNS - 1],
    ["y", "Row start (y)", 0, 999],
    ["w", "Width (w, columns)", 1, GRID_COLUMNS],
    ["h", "Height (h, rows)", 1, 24],
  ];
  return (
    <div className="card col" style={{ marginBottom: 0 }}>
      <div className="row" style={{ justifyContent: "space-between" }}>
        <strong>Widget properties</strong>
        <button type="button" className="btn" onClick={onReset}>
          Reset override
        </button>
      </div>
      <div style={{ fontSize: 12 }}>
        <code>{node.widget_id}</code> ({node.widget_type})
      </div>
      <div className="row">
        {geometry.map(([field, label, min, max]) => (
          <label key={field} className="col" style={{ fontSize: 12 }}>
            <span>{label}</span>
            <input
              type="number"
              min={min}
              max={max}
              style={{ width: 90 }}
              value={typeof ov[field] === "number" ? String(ov[field]) : ""}
              placeholder="auto"
              onChange={(e) => onPatch({ [field]: numOrNull(e.target.value) })}
            />
          </label>
        ))}
      </div>
      <label className="row" style={{ fontSize: 12 }}>
        <input
          type="checkbox"
          checked={ov.visible !== false}
          onChange={(e) => onPatch({ visible: e.target.checked ? null : false })}
        />
        <span>Visible</span>
      </label>
      {specs.length > 0 ? (
        <>
          <strong style={{ fontSize: 12 }}>Configurable props</strong>
          {specs.map((spec) => {
            const current = ov.props?.[spec.key];
            const placeholder = String(node.props?.[spec.key] ?? "");
            if (spec.type === "enum") {
              return (
                <label key={spec.key} className="row" style={{ fontSize: 12 }}>
                  <span>{spec.label}</span>
                  <select
                    value={current === undefined ? "" : String(current)}
                    onChange={(e) => onProps({ [spec.key]: e.target.value === "" ? null : e.target.value })}
                  >
                    <option value="">(Adapter: {placeholder || "default"})</option>
                    {spec.options?.map((o) => (
                      <option key={o} value={o}>
                        {o}
                      </option>
                    ))}
                  </select>
                </label>
              );
            }
            return (
              <label key={spec.key} className="row" style={{ fontSize: 12 }}>
                <span>{spec.label}</span>
                <input
                  type={spec.type === "number" ? "number" : "text"}
                  value={current === undefined || current === null ? "" : String(current)}
                  placeholder={placeholder}
                  onChange={(e) =>
                    onProps({
                      [spec.key]:
                        e.target.value === ""
                          ? null
                          : spec.type === "number"
                            ? numOrNull(e.target.value)
                            : e.target.value,
                    })
                  }
                />
              </label>
            );
          })}
        </>
      ) : (
        <span style={{ fontSize: 12, color: "var(--text-muted)" }}>
          This widget type has no configurable props.
        </span>
      )}
    </div>
  );
};

/** Adapter placement editor: bind Adapter Instances to areas, edit query params, reorder. */
const PlacementEditor: React.FC<{
  placements: DashboardPlacement[];
  adapterInstances: string[];
  onEdit: (e: DesignerEdit) => void;
}> = ({ placements, adapterInstances, onEdit }) => {
  const [dragging, setDragging] = useState<string | null>(null);
  const [newAdapter, setNewAdapter] = useState("");
  const [newArea, setNewArea] = useState("main");
  const unplaced = adapterInstances.filter((id) => !placements.some((p) => p.adapter_instance === id));
  return (
    <div className="card col" style={{ marginBottom: 0 }}>
      <strong>Adapter placements</strong>
      <ol className="placement-list">
        {placements.map((p, idx) => (
          <li
            key={p.adapter_instance}
            draggable
            onDragStart={() => setDragging(p.adapter_instance)}
            onDragOver={(e) => e.preventDefault()}
            onDrop={() => {
              if (dragging && dragging !== p.adapter_instance) {
                onEdit({ kind: "placement_move", adapterInstance: dragging, toIndex: idx });
              }
              setDragging(null);
            }}
          >
            <PlacementRow placement={p} onEdit={onEdit} />
          </li>
        ))}
      </ol>
      <form
        className="row"
        onSubmit={(e) => {
          e.preventDefault();
          if (!newAdapter) return;
          onEdit({
            kind: "placement_add",
            placement: { adapter_instance: newAdapter, area: newArea || "main", query_params: {} },
          });
          setNewAdapter("");
        }}
      >
        <select aria-label="Adapter to place" value={newAdapter} onChange={(e) => setNewAdapter(e.target.value)}>
          <option value="">Add Adapter…</option>
          {unplaced.map((id) => (
            <option key={id} value={id}>
              {id}
            </option>
          ))}
        </select>
        <input
          type="text"
          aria-label="Area for new Adapter"
          value={newArea}
          onChange={(e) => setNewArea(e.target.value)}
          style={{ width: 90 }}
        />
        <button type="submit" className="btn" disabled={!newAdapter}>
          Place
        </button>
      </form>
    </div>
  );
};

const PlacementRow: React.FC<{ placement: DashboardPlacement; onEdit: (e: DesignerEdit) => void }> = ({
  placement,
  onEdit,
}) => {
  // Reset on content, not identity (pushes and every Designer edit clone query_params), and
  // never while the user has uncommitted text in the field.
  const paramsJson = JSON.stringify(placement.query_params ?? {});
  const [paramsText, setParamsText] = useState(paramsJson);
  const [paramsError, setParamsError] = useState<string | null>(null);
  const dirtyRef = useRef(false);
  useEffect(() => {
    if (!dirtyRef.current) setParamsText(paramsJson);
  }, [paramsJson]);
  return (
    <div className="col" style={{ gap: 4 }}>
      <div className="row" style={{ justifyContent: "space-between" }}>
        <span>
          <span aria-hidden="true" style={{ cursor: "grab" }}>
            ⠿{" "}
          </span>
          <code>{placement.adapter_instance}</code>
        </span>
        <button
          type="button"
          className="btn danger"
          aria-label={`Remove ${placement.adapter_instance}`}
          onClick={() => onEdit({ kind: "placement_remove", adapterInstance: placement.adapter_instance })}
        >
          Remove
        </button>
      </div>
      <div className="row">
        <label className="row" style={{ fontSize: 12 }}>
          <span>area</span>
          <input
            type="text"
            style={{ width: 90 }}
            defaultValue={placement.area}
            onBlur={(e) => {
              if (e.target.value && e.target.value !== placement.area) {
                onEdit({ kind: "placement_update", adapterInstance: placement.adapter_instance, area: e.target.value });
              }
            }}
          />
        </label>
        <label className="row" style={{ fontSize: 12, flex: 1 }}>
          <span>query_params</span>
          <input
            type="text"
            className="code"
            style={{ flex: 1 }}
            value={paramsText}
            onChange={(e) => {
              dirtyRef.current = true;
              setParamsText(e.target.value);
            }}
            onBlur={() => {
              try {
                const parsed = JSON.parse(paramsText || "{}");
                if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
                  throw new Error("query_params must be a JSON object");
                }
                setParamsError(null);
                dirtyRef.current = false;
                if (JSON.stringify(parsed) !== paramsJson) {
                  onEdit({
                    kind: "placement_update",
                    adapterInstance: placement.adapter_instance,
                    queryParams: parsed as Record<string, unknown>,
                  });
                } else {
                  setParamsText(paramsJson);
                }
              } catch (err) {
                setParamsError(String(err));
              }
            }}
          />
        </label>
      </div>
      {paramsError && <span className="badge danger">{paramsError}</span>}
    </div>
  );
};
