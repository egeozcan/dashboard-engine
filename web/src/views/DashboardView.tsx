import React, { useEffect, useRef, useState } from "react";
import {
  apiFetch,
  type DashboardDoc,
  type RenderedDashboard,
} from "../lib/api";
import { WidgetRenderer, type WidgetRequestContext } from "../components/widgets/BuiltInWidgets";

export const DashboardView: React.FC = () => {
  const [dashboards, setDashboards] = useState<DashboardDoc[]>([]);
  const [activeId, setActiveId] = useState<string>("engineering");
  const [rendered, setRendered] = useState<RenderedDashboard | null>(null);
  const [doc, setDoc] = useState<DashboardDoc | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [banner, setBanner] = useState<string | null>(null);
  const [designerMode, setDesignerMode] = useState(false);
  const [viewport, setViewport] = useState<"desktop" | "tablet" | "mobile">("desktop");
  const [selectedWidgetId, setSelectedWidgetId] = useState<string | null>(null);

  // Action Dry-run confirmation modal (<dialog>, dialog guide)
  const confirmDialogRef = useRef<HTMLDialogElement>(null);
  const [pendingRequest, setPendingRequest] = useState<{
    ctx: WidgetRequestContext;
    dryRun?: Record<string, unknown>;
  } | null>(null);

  const loadDashboards = async () => {
    try {
      const list = await apiFetch<DashboardDoc[]>("/api/dashboards");
      setDashboards(list);
      if (list.length > 0 && !list.some((d) => d.id === activeId)) {
        setActiveId(list[0].id);
      }
    } catch (e) {
      setError(String(e));
    }
  };

  const loadActiveDashboard = async (id: string) => {
    try {
      setError(null);
      const resp = await apiFetch<{ document: DashboardDoc; rendered: RenderedDashboard }>(
        `/api/dashboards/${encodeURIComponent(id)}`
      );
      setDoc(resp.document);
      setRendered(resp.rendered);
    } catch (e) {
      setError(String(e));
    }
  };

  useEffect(() => {
    void loadDashboards();
  }, []);

  useEffect(() => {
    if (activeId) {
      void loadActiveDashboard(activeId);
    }
  }, [activeId]);

  const handleDashboardStateChange = async (key: string, value: unknown) => {
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
      await loadActiveDashboard(activeId);
    } catch (e) {
      await loadActiveDashboard(activeId);
      setError(String(e));
    }
  };

  const handleWidgetRequest = async (ctx: WidgetRequestContext) => {
    setError(null);
    if (ctx.request === "merge_requested") {
      try {
        const dryRun = await apiFetch<Record<string, unknown>>("/api/actions/merge_pr/dry-run", {
          method: "POST",
          body: JSON.stringify({
            args: {
              repository: ctx.payload.repository ?? "acme/platform",
              number: ctx.payload.number,
              issue_key: ctx.payload.jira_key,
            },
          }),
        });
        setPendingRequest({ ctx, dryRun });
        confirmDialogRef.current?.showModal();
        return;
      } catch (e) {
        setError(String(e));
        return;
      }
    }
    await executeWidgetRequest(ctx);
  };

  const executeWidgetRequest = async (ctx: WidgetRequestContext) => {
    try {
      const exec = await apiFetch<{
        outcome?: "action" | "refreshed";
        execution_id?: string;
        status: string;
        refreshed?: string[];
      }>("/api/widget-request", {
        method: "POST",
        body: JSON.stringify({
          protocol: "dash.widget/1",
          dashboard_id: activeId,
          adapter_instance: ctx.adapterInstance,
          widget_id: ctx.widgetId,
          generation: ctx.generation,
          sequence: Date.now(),
          kind: "request",
          request: ctx.request,
          payload: ctx.payload,
        }),
      });
      setBanner(
        exec.outcome === "refreshed" || !exec.execution_id
          ? `Request '${ctx.request}' -> refreshed ${(exec.refreshed ?? []).join(", ") || "bound data"} (${exec.status})`
          : `Request '${ctx.request}' -> execution ${exec.execution_id} (${exec.status})`
      );
      await loadActiveDashboard(activeId);
    } catch (e) {
      setError(String(e));
    }
  };

  const saveLayoutOverride = async (
    widgetId: string,
    patch: { w?: number; h?: number; visible?: boolean }
  ) => {
    if (!doc) return;
    setError(null);
    const nextDoc: DashboardDoc = {
      ...doc,
      overrides: {
        ...doc.overrides,
        [widgetId]: {
          ...doc.overrides[widgetId],
          ...patch,
        },
      },
    };
    try {
      const draft = await apiFetch<{ draft_id: string }>("/api/drafts", { method: "POST" });
      await apiFetch(`/api/drafts/${draft.draft_id}/dashboards`, {
        method: "POST",
        body: JSON.stringify(nextDoc),
      });
      await apiFetch(`/api/drafts/${draft.draft_id}/activate`, { method: "POST" });
      setBanner(`Saved layout override for ${widgetId} and activated new Generation`);
      await loadActiveDashboard(activeId);
    } catch (e) {
      setError(String(e));
    }
  };

  const viewportWidth =
    viewport === "mobile" ? "390px" : viewport === "tablet" ? "768px" : "100%";

  const selectedRepo = String(rendered?.dashboard_state?.selected_repository ?? "acme/platform");

  return (
    <div>
      <div className="card row" style={{ justifyContent: "space-between" }}>
        <div className="row">
          <label className="row">
            <strong>Dashboard:</strong>
            <select value={activeId} onChange={(e) => setActiveId(e.target.value)}>
              {dashboards.map((d) => (
                <option key={d.id} value={d.id}>
                  {d.title} ({d.id})
                </option>
              ))}
            </select>
          </label>
          <label className="row">
            <span>Shared repository state:</span>
            <select
              value={selectedRepo}
              onChange={(e) => void handleDashboardStateChange("selected_repository", e.target.value)}
            >
              <option value="acme/platform">acme/platform</option>
              <option value="acme/payments">acme/payments</option>
            </select>
          </label>
          {rendered && <span className="badge">Generation #{rendered.generation}</span>}
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
          <button type="button" className="btn" onClick={() => void loadActiveDashboard(activeId)}>
            Refresh
          </button>
        </div>
      </div>

      {designerMode && (
        <div className="card row" style={{ justifyContent: "space-between" }}>
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
          {selectedWidgetId ? (
            <div className="row">
              <span>Selected: <code>{selectedWidgetId}</code></span>
              <button
                type="button"
                className="btn"
                onClick={() => void saveLayoutOverride(selectedWidgetId, { w: 6 })}
              >
                Half Width (w=6)
              </button>
              <button
                type="button"
                className="btn"
                onClick={() => void saveLayoutOverride(selectedWidgetId, { w: 12 })}
              >
                Full Width (w=12)
              </button>
              <button
                type="button"
                className="btn"
                onClick={() =>
                  void saveLayoutOverride(selectedWidgetId, {
                    visible: doc?.overrides[selectedWidgetId]?.visible === false,
                  })
                }
              >
                Toggle Visibility
              </button>
            </div>
          ) : (
            <span style={{ color: "var(--text-muted)", fontSize: 13 }}>
              Click any widget node to edit layout overrides keyed by stable widget_id
            </span>
          )}
        </div>
      )}

      {error && <div className="card badge danger" style={{ display: "block" }}>{error}</div>}
      {banner && <div className="card badge ok" style={{ display: "block" }}>{banner}</div>}

      <div style={{ maxWidth: viewportWidth, margin: "0 auto", transition: "max-width 0.15s ease" }}>
        {rendered?.adapters.map((a) => (
          <div key={a.adapter_instance} style={{ marginBottom: 16 }}>
            <WidgetRenderer
              node={a.tree}
              adapterInstance={a.adapter_instance}
              generation={rendered.generation}
              onWidgetRequest={handleWidgetRequest}
              onDashboardStateChange={handleDashboardStateChange}
              designerMode={designerMode}
              onSelectWidget={setSelectedWidgetId}
              selectedWidgetId={selectedWidgetId}
            />
          </div>
        ))}
      </div>

      {/* Native modal <dialog> for Dry run preview before action confirmation */}
      <dialog
        ref={confirmDialogRef}
        aria-labelledby="dryrun-dialog-title"
        onClose={() => {
          const dlg = confirmDialogRef.current;
          if (!dlg) return;
          const choice = dlg.returnValue;
          dlg.returnValue = "";
          if (choice === "confirm" && pendingRequest) {
            void executeWidgetRequest(pendingRequest.ctx);
          }
          setPendingRequest(null);
        }}
      >
        <h2 id="dryrun-dialog-title" style={{ marginTop: 0 }}>
          Confirm Action (Dry Run Preview)
        </h2>
        <pre style={{ background: "var(--bg)", padding: 10, borderRadius: 6, overflow: "auto", fontSize: 12 }}>
          {JSON.stringify(pendingRequest?.dryRun ?? {}, null, 2)}
        </pre>
        <form method="dialog" className="row" style={{ justifyContent: "flex-end", marginTop: 14 }}>
          <button value="cancel" className="btn" autoFocus>
            Cancel
          </button>
          <button value="confirm" className="btn primary">
            Confirm &amp; Execute
          </button>
        </form>
      </dialog>
    </div>
  );
};
