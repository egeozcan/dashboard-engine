import React, { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import {
  apiFetch,
  type DefinitionSummary,
  type GraphInspectResponse,
  type InstanceConfig,
  type ValidationIssue,
} from "../lib/api";
import { capabilityRequirements, groupIssues, pickInputSlot } from "../lib/graphEditor";

interface EdgeGeom {
  key: string;
  d: string;
  invalid: boolean;
}

const CATEGORIES = ["connector", "data_provider", "action_provider", "adapter", "automation"];
const NO_INSTANCES: Record<string, InstanceConfig> = {};
const NO_ISSUES: ValidationIssue[] = [];

function attrSelectorValue(v: string): string {
  return v.replace(/["\\]/g, "\\$&");
}

export const GraphEditorView: React.FC = () => {
  const [activeGraph, setActiveGraph] = useState<GraphInspectResponse | null>(null);
  const [activeDefs, setActiveDefs] = useState<Record<string, DefinitionSummary>>({});
  const [draftId, setDraftId] = useState<string | null>(null);
  const [draftGraph, setDraftGraph] = useState<GraphInspectResponse | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [dragFrom, setDragFrom] = useState<string | null>(null);
  const [newInstId, setNewInstId] = useState("");
  const [newInstDef, setNewInstDef] = useState("");
  const [newInstCat, setNewInstCat] = useState("data_provider");
  const [dragBindSlot, setDragBindSlot] = useState("");
  const [newInstBindSlot, setNewInstBindSlot] = useState("prs");
  const [bindTarget, setBindTarget] = useState("");
  const [statusMsg, setStatusMsg] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [edges, setEdges] = useState<EdgeGeom[]>([]);
  const [layoutTick, setLayoutTick] = useState(0);
  const canvasRef = useRef<HTMLDivElement>(null);

  const loadGraph = async () => {
    try {
      const [g, defs] = await Promise.all([
        apiFetch<GraphInspectResponse>("/api/graph"),
        apiFetch<Record<string, DefinitionSummary>>("/api/definitions").catch(() => ({})),
      ]);
      setActiveGraph(g);
      setActiveDefs(defs ?? {});
    } catch (e) {
      setError(String(e));
    }
  };

  useEffect(() => {
    void loadGraph();
  }, []);

  /** Once a Draft exists the editor shows and builds on the Draft graph (cumulative edits). */
  const shown: GraphInspectResponse | null = draftGraph ?? activeGraph;
  const instances: Record<string, InstanceConfig> = shown?.instances ?? NO_INSTANCES;
  const defs: Record<string, DefinitionSummary> = draftGraph?.definitions ?? activeDefs;
  const issues: ValidationIssue[] = shown?.validation?.issues ?? NO_ISSUES;
  const issuesByNode = useMemo(() => groupIssues(issues, instances), [issues, instances]);

  const loadDraftGraph = async (id: string): Promise<GraphInspectResponse> => {
    const g = await apiFetch<GraphInspectResponse>(`/api/drafts/${id}/graph`);
    setDraftGraph(g);
    const vr = g.validation;
    if (vr) {
      setStatusMsg(
        vr.valid
          ? `Draft ${id}: configuration valid`
          : `Draft ${id}: ${vr.issues.length} validation issue(s), shown on the affected nodes`
      );
    }
    return g;
  };

  const ensureDraft = async (): Promise<{ id: string; graph: GraphInspectResponse }> => {
    if (draftId && draftGraph) return { id: draftId, graph: draftGraph };
    const d = await apiFetch<{ draft_id: string }>("/api/drafts", { method: "POST" });
    setDraftId(d.draft_id);
    const graph = await loadDraftGraph(d.draft_id);
    return { id: d.draft_id, graph };
  };

  /** Run one Draft mutation, then reload the Draft graph (and its validation). */
  const mutate = async (fn: (id: string, graph: GraphInspectResponse) => Promise<void>) => {
    setError(null);
    try {
      const { id, graph } = await ensureDraft();
      await fn(id, graph);
      await loadDraftGraph(id);
    } catch (e) {
      setError(String(e));
    }
  };

  const upsertInstance = (inst: InstanceConfig) =>
    mutate(async (id) => {
      await apiFetch(`/api/drafts/${id}/instances`, {
        method: "POST",
        body: JSON.stringify(inst),
      });
    });

  const connectEdge = (fromId: string, toId: string) =>
    mutate(async (id, graph) => {
      const g = graph.instances ?? {};
      const target = g[toId] ?? activeGraph?.instances?.[toId];
      const source = g[fromId] ?? activeGraph?.instances?.[fromId];
      if (!target || !source) throw new Error(`instance '${toId}' not found in draft`);
      const slot =
        dragBindSlot.trim() ||
        pickInputSlot(source, target, (graph.definitions ?? activeDefs)[target.definition]);
      if (!slot) {
        throw new Error(
          `'${target.definition}' declares no input that accepts a ${source.category}; type a slot name`
        );
      }
      await apiFetch(`/api/drafts/${id}/instances`, {
        method: "POST",
        body: JSON.stringify({ ...target, inputs: { ...(target.inputs ?? {}), [slot]: fromId } }),
      });
    });

  const removeBinding = (instId: string, slot: string) =>
    mutate(async (id, graph) => {
      const target = (graph.instances ?? {})[instId];
      if (!target) throw new Error(`instance '${instId}' not found in draft`);
      const inputs = { ...(target.inputs ?? {}) };
      delete inputs[slot];
      await apiFetch(`/api/drafts/${id}/instances`, {
        method: "POST",
        body: JSON.stringify({ ...target, inputs }),
      });
    });

  const deleteInstance = (instId: string) =>
    mutate(async (id) => {
      await apiFetch(`/api/drafts/${id}/instances/${encodeURIComponent(instId)}`, { method: "DELETE" });
      if (selectedId === instId) setSelectedId(null);
    });

  const discardDraft = async () => {
    if (!draftId) return;
    setError(null);
    try {
      await apiFetch(`/api/drafts/${draftId}`, { method: "DELETE" });
      setDraftId(null);
      setDraftGraph(null);
      setStatusMsg(null);
    } catch (e) {
      setError(String(e));
    }
  };

  const activateDraft = async () => {
    if (!draftId) return;
    setError(null);
    try {
      const res = await apiFetch<{ revision_commit: string; generation_id: number }>(
        `/api/drafts/${draftId}/activate`,
        { method: "POST" }
      );
      setDraftId(null);
      setDraftGraph(null);
      setStatusMsg(`Activated Generation #${res.generation_id} (${res.revision_commit.slice(0, 8)})`);
      await loadGraph();
    } catch (e) {
      setError(String(e));
    }
  };

  // Draw configured edges (source -> consumer input) between node cards.
  useLayoutEffect(() => {
    const root = canvasRef.current;
    if (!root) return;
    const base = root.getBoundingClientRect();
    const out: EdgeGeom[] = [];
    for (const inst of Object.values(instances)) {
      for (const [slot, src] of Object.entries(inst.inputs ?? {})) {
        const a = root.querySelector<HTMLElement>(`[data-node-id="${attrSelectorValue(src)}"]`);
        const b = root.querySelector<HTMLElement>(`[data-node-id="${attrSelectorValue(inst.id)}"]`);
        if (!a || !b) continue;
        const ra = a.getBoundingClientRect();
        const rb = b.getBoundingClientRect();
        if (ra.width === 0 && rb.width === 0) continue;
        const x1 = ra.right - base.left;
        const y1 = ra.top + ra.height / 2 - base.top;
        const x2 = rb.left - base.left;
        const y2 = rb.top + rb.height / 2 - base.top;
        const bend = Math.max(40, Math.abs(x2 - x1) / 2);
        out.push({
          key: `${src}->${inst.id}.${slot}`,
          d: `M ${x1} ${y1} C ${x1 + bend} ${y1}, ${x2 - bend} ${y2}, ${x2} ${y2}`,
          invalid: Boolean(issuesByNode[inst.id]?.inputs[slot]?.length),
        });
      }
    }
    setEdges(out);
  }, [instances, issuesByNode, layoutTick]);

  useEffect(() => {
    const onResize = () => setLayoutTick((t) => t + 1);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  const instanceList = Object.values(instances);
  const definitionIds = Object.keys(defs).sort();

  return (
    <div>
      <div className="card row" style={{ justifyContent: "space-between" }}>
        <div className="row">
          <strong>Configured Dependency Graph</strong>
          {shown?.validation && (
            <span className={`badge ${shown.validation.valid ? "ok" : "danger"}`}>
              {shown.validation.valid ? "Valid" : `${shown.validation.issues.length} issue(s)`}
            </span>
          )}
          {draftId ? (
            <span className="badge warn">Active Draft: {draftId}</span>
          ) : (
            <span className="badge">Active Generation #{activeGraph?.generation_id ?? "?"}</span>
          )}
        </div>
        <div className="row">
          {draftId && (
            <>
              <button type="button" className="btn primary" onClick={() => void activateDraft()}>
                Validate, Test &amp; Activate Draft
              </button>
              <button type="button" className="btn danger" onClick={() => void discardDraft()}>
                Discard Draft
              </button>
            </>
          )}
          <button
            type="button"
            className="btn"
            onClick={() => void (draftId ? loadDraftGraph(draftId).catch((e) => setError(String(e))) : loadGraph())}
          >
            Reload Graph
          </button>
        </div>
      </div>

      {error && <div className="card badge danger" style={{ display: "block" }}>{error}</div>}
      {statusMsg && <div className="card badge warn" style={{ display: "block" }}>{statusMsg}</div>}

      {/* Drag-to-connect configured nodes, grouped by architectural layer */}
      <div className="card">
        <div className="row" style={{ marginBottom: 12 }}>
          <label className="row">
            <span>Target input slot for drag-connect:</span>
            <input
              type="text"
              value={dragBindSlot}
              placeholder="auto"
              onChange={(e) => setDragBindSlot(e.target.value)}
              style={{ width: 110 }}
            />
          </label>
          <span style={{ fontSize: 12, color: "var(--text-muted)" }}>
            Drag a node onto a downstream node to bind it inside a Draft. With no slot typed, the
            first declared input accepting that category is used.
          </span>
        </div>

        <div ref={canvasRef} className="graph-canvas">
          <svg className="graph-edges" aria-hidden="true">
            {edges.map((e) => (
              <path key={e.key} d={e.d} className={e.invalid ? "edge invalid" : "edge"} />
            ))}
          </svg>
          {CATEGORIES.map((cat) => (
            <div key={cat} className="card graph-column" style={{ marginBottom: 0 }}>
              <div className="graph-column-title">{cat.replace("_", " ")}s</div>
              <div className="col">
                {instanceList
                  .filter((inst) => inst.category === cat)
                  .map((inst) => (
                    <GraphNodeCard
                      key={inst.id}
                      inst={inst}
                      def={defs[inst.definition]}
                      instances={instances}
                      issues={issuesByNode[inst.id]}
                      selected={selectedId === inst.id}
                      onSelect={() => setSelectedId(inst.id)}
                      onDragStart={() => setDragFrom(inst.id)}
                      onDrop={() => {
                        if (dragFrom && dragFrom !== inst.id) {
                          void connectEdge(dragFrom, inst.id);
                        }
                        setDragFrom(null);
                      }}
                      onRemoveBinding={(slot) => void removeBinding(inst.id, slot)}
                      onDelete={() => void deleteInstance(inst.id)}
                    />
                  ))}
              </div>
            </div>
          ))}
        </div>
      </div>

      {issues.filter((i) => !instances[i.target]).length > 0 && (
        <div className="card">
          <strong>Other diagnostics</strong>
          <ul>
            {issues
              .filter((i) => !instances[i.target])
              .map((iss, i) => (
                <li key={i}>
                  <span className={`badge ${iss.severity === "error" ? "danger" : "warn"}`}>{iss.code}</span>{" "}
                  <strong>{iss.target}:</strong> {iss.message}
                </li>
              ))}
          </ul>
        </div>
      )}

      {/* Create / Bind Instance Form */}
      <div className="card">
        <strong>Create Instance in Draft</strong>
        <form
          className="row"
          style={{ marginTop: 10 }}
          onSubmit={(e) => {
            e.preventDefault();
            if (!newInstId.trim()) return;
            void upsertInstance({
              id: newInstId.trim(),
              category: newInstCat,
              definition: newInstDef.trim(),
              inputs: bindTarget && newInstBindSlot ? { [newInstBindSlot]: bindTarget } : {},
              config: {},
              secrets: {},
              capabilities: [],
            });
          }}
        >
          <input
            type="text"
            placeholder="instance_id"
            value={newInstId}
            onChange={(e) => setNewInstId(e.target.value)}
            required
          />
          <select value={newInstCat} onChange={(e) => setNewInstCat(e.target.value)}>
            {CATEGORIES.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
          <input
            type="text"
            list="graph-definition-ids"
            placeholder="definition (pkg.def)"
            value={newInstDef}
            onChange={(e) => setNewInstDef(e.target.value)}
            required
          />
          <datalist id="graph-definition-ids">
            {definitionIds
              .filter((id) => defs[id]?.category === newInstCat)
              .map((id) => (
                <option key={id} value={id} />
              ))}
          </datalist>
          <input
            type="text"
            placeholder="input slot (e.g. prs)"
            value={newInstBindSlot}
            onChange={(e) => setNewInstBindSlot(e.target.value)}
          />
          <input
            type="text"
            placeholder="source instance"
            value={bindTarget}
            onChange={(e) => setBindTarget(e.target.value)}
          />
          <button type="submit" className="btn primary">
            Add to Draft
          </button>
        </form>
      </div>
    </div>
  );
};

const GraphNodeCard: React.FC<{
  inst: InstanceConfig;
  def: DefinitionSummary | undefined;
  instances: Record<string, InstanceConfig>;
  issues: { node: ValidationIssue[]; inputs: Record<string, ValidationIssue[]> } | undefined;
  selected: boolean;
  onSelect: () => void;
  onDragStart: () => void;
  onDrop: () => void;
  onRemoveBinding: (slot: string) => void;
  onDelete: () => void;
}> = ({ inst, def, instances, issues, selected, onSelect, onDragStart, onDrop, onRemoveBinding, onDelete }) => {
  const reqs = capabilityRequirements(inst, def, instances);
  const nodeIssues = issues?.node ?? [];
  const inputIssues = issues?.inputs ?? {};
  const hasErrors =
    nodeIssues.some((i) => i.severity === "error") ||
    Object.values(inputIssues).some((l) => l.some((i) => i.severity === "error"));
  const declaredSlots = Object.keys(def?.inputs ?? {});
  const unboundDeclared = declaredSlots.filter((s) => !(s in (inst.inputs ?? {})));
  return (
    <div
      data-node-id={inst.id}
      draggable
      onDragStart={onDragStart}
      onDragOver={(e) => e.preventDefault()}
      onDrop={onDrop}
      onClick={onSelect}
      className="card graph-node"
      style={{
        borderColor: hasErrors ? "var(--danger)" : selected ? "var(--accent)" : "var(--border)",
      }}
    >
      <div className="row" style={{ justifyContent: "space-between" }}>
        <span style={{ fontWeight: 600 }}>{inst.id}</span>
        <button
          type="button"
          className="btn-icon"
          aria-label={`Delete instance ${inst.id}`}
          title="Delete Instance in Draft"
          onClick={(e) => {
            e.stopPropagation();
            onDelete();
          }}
        >
          ×
        </button>
      </div>
      <div style={{ fontSize: 11, color: "var(--text-muted)" }}>{inst.definition}</div>

      {nodeIssues.map((iss, i) => (
        <div key={i} className={`badge ${iss.severity === "error" ? "danger" : "warn"} issue`} title={iss.message}>
          {iss.code}: {iss.message}
        </div>
      ))}

      {Object.entries(inst.inputs ?? {}).length > 0 && (
        <div style={{ fontSize: 11, marginTop: 4 }}>
          {Object.entries(inst.inputs ?? {}).map(([slot, src]) => (
            <div key={slot} className="graph-input-row">
              <span>
                <code>{slot}</code> ← <strong>{src}</strong>
              </span>
              <button
                type="button"
                className="btn-icon"
                aria-label={`Remove binding ${inst.id}.${slot}`}
                title="Remove binding"
                onClick={(e) => {
                  e.stopPropagation();
                  onRemoveBinding(slot);
                }}
              >
                ×
              </button>
              {(inputIssues[slot] ?? []).map((iss, i) => (
                <div
                  key={i}
                  className={`badge ${iss.severity === "error" ? "danger" : "warn"} issue`}
                  title={iss.message}
                >
                  {iss.code}: {iss.message}
                </div>
              ))}
            </div>
          ))}
        </div>
      )}
      {Object.entries(inputIssues)
        .filter(([slot]) => !(slot in (inst.inputs ?? {})))
        .flatMap(([slot, list]) =>
          list.map((iss, i) => (
            <div
              key={`${slot}:${i}`}
              className={`badge ${iss.severity === "error" ? "danger" : "warn"} issue`}
              title={iss.message}
            >
              {iss.code} ({slot}): {iss.message}
            </div>
          ))
        )}
      {unboundDeclared.length > 0 && (
        <div style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 4 }}>
          unbound inputs: {unboundDeclared.map((s) => `${s} (${def?.inputs[s].ref_kind})`).join(", ")}
        </div>
      )}

      {reqs.length > 0 && (
        <div className="row" style={{ marginTop: 6, gap: 4 }} aria-label={`Required capabilities of ${inst.id}`}>
          {reqs.map((r) => (
            <span
              key={`${r.slot ?? "_"}:${r.capability}`}
              className={`badge ${r.satisfied ? "ok" : "danger"}`}
              style={{ fontSize: 10 }}
              title={
                r.satisfied
                  ? `granted by ${r.grantedBy}`
                  : r.slot
                    ? `input '${r.slot}' needs '${r.capability}' from ${r.grantedBy ?? "an unbound connector"}`
                    : `no Instance grants '${r.capability}'`
              }
            >
              requires {r.capability}
              {r.slot ? ` via ${r.slot}` : ""}: {r.satisfied ? "granted" : "missing"}
            </span>
          ))}
        </div>
      )}
      {(inst.capabilities ?? []).length > 0 && (
        <div className="row" style={{ marginTop: 6, gap: 4 }}>
          {(inst.capabilities ?? []).map((cap) => (
            <span key={cap} className="badge" style={{ fontSize: 10 }} title="granted by this Instance">
              grants {cap}
            </span>
          ))}
        </div>
      )}
    </div>
  );
};
