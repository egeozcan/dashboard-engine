import React, { useEffect, useState } from "react";
import { apiFetch, type GraphInspectResponse, type InstanceConfig } from "../lib/api";

export const GraphEditorView: React.FC = () => {
  const [graph, setGraph] = useState<GraphInspectResponse | null>(null);
  const [draftId, setDraftId] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [dragFrom, setDragFrom] = useState<string | null>(null);
  const [newInstId, setNewInstId] = useState("");
  const [newInstDef, setNewInstDef] = useState("engineering.linked_prs");
  const [newInstCat, setNewInstCat] = useState("data_provider");
  const [dragBindSlot, setDragBindSlot] = useState("prs");
  const [newInstBindSlot, setNewInstBindSlot] = useState("prs");
  const [bindTarget, setBindTarget] = useState("open_prs");
  const [statusMsg, setStatusMsg] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const loadGraph = async () => {
    try {
      const g = await apiFetch<GraphInspectResponse>("/api/graph");
      setGraph(g);
    } catch (e) {
      setError(String(e));
    }
  };

  useEffect(() => {
    void loadGraph();
  }, []);

  const ensureDraft = async (): Promise<string> => {
    if (draftId) return draftId;
    const d = await apiFetch<{ draft_id: string }>("/api/drafts", { method: "POST" });
    setDraftId(d.draft_id);
    return d.draft_id;
  };

  const upsertInstance = async (inst: InstanceConfig) => {
    setError(null);
    try {
      const did = await ensureDraft();
      await apiFetch(`/api/drafts/${did}/instances`, {
        method: "POST",
        body: JSON.stringify(inst),
      });
      const val = await apiFetch<{ valid: boolean; issues: Array<{ severity: string; message: string }> }>(
        `/api/drafts/${did}/validate`,
        { method: "POST" }
      );
      setStatusMsg(
        val.valid
          ? `Draft ${did}: configuration valid`
          : `Draft ${did} validation issues: ${val.issues.map((i) => i.message).join("; ")}`
      );
    } catch (e) {
      setError(String(e));
    }
  };

  const connectEdge = async (fromId: string, toId: string) => {
    if (!graph) return;
    const target = graph.instances[toId];
    if (!target) return;
    const updated: InstanceConfig = {
      ...target,
      inputs: { ...target.inputs, [dragBindSlot]: fromId },
    };
    await upsertInstance(updated);
  };

  const discardDraft = async () => {
    if (!draftId) return;
    setError(null);
    try {
      await apiFetch(`/api/drafts/${draftId}`, { method: "DELETE" });
      setDraftId(null);
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
      setStatusMsg(`Activated Generation #${res.generation_id} (${res.revision_commit.slice(0, 8)})`);
      await loadGraph();
    } catch (e) {
      setError(String(e));
    }
  };

  const instances = Object.values(graph?.instances ?? {});
  const categories = ["connector", "data_provider", "action_provider", "adapter", "automation"];

  return (
    <div>
      <div className="card row" style={{ justifyContent: "space-between" }}>
        <div className="row">
          <strong>Configured Dependency Graph</strong>
          {graph && (
            <span className={`badge ${graph.validation.valid ? "ok" : "danger"}`}>
              {graph.validation.valid ? "Valid" : `${graph.validation.issues.length} issue(s)`}
            </span>
          )}
          {draftId && <span className="badge warn">Active Draft: {draftId}</span>}
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
          <button type="button" className="btn" onClick={() => void loadGraph()}>
            Reload Graph
          </button>
        </div>
      </div>

      {error && <div className="card badge danger" style={{ display: "block" }}>{error}</div>}
      {statusMsg && <div className="card badge warn" style={{ display: "block" }}>{statusMsg}</div>}

      {graph && graph.validation.issues.length > 0 && (
        <div className="card">
          <strong>Compatibility &amp; Capability Diagnostics</strong>
          <ul>
            {graph.validation.issues.map((iss, i) => (
              <li key={i}>
                <span className={`badge ${iss.severity === "error" ? "danger" : "warn"}`}>
                  {iss.code}
                </span>{" "}
                <strong>{iss.target}:</strong> {iss.message}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Drag-to-connect Configured Nodes Canvas grouped by architectural layer */}
      <div className="card">
        <div className="row" style={{ marginBottom: 12 }}>
          <label className="row">
            <span>Target input slot for drag-connect:</span>
            <input
              type="text"
              value={dragBindSlot}
              onChange={(e) => setDragBindSlot(e.target.value)}
              style={{ width: 110 }}
            />
          </label>
          <span style={{ fontSize: 12, color: "var(--text-muted)" }}>
            Drag any node onto a downstream node to wire an input binding inside a Draft.
          </span>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(210px, 1fr))",
            gap: 12,
          }}
        >
          {categories.map((cat) => (
            <div key={cat} className="card" style={{ marginBottom: 0, background: "var(--bg)" }}>
              <div style={{ fontWeight: 700, marginBottom: 8, textTransform: "uppercase", fontSize: 11, color: "var(--text-muted)" }}>
                {cat.replace("_", " ")}s
              </div>
              <div className="col">
                {instances
                  .filter((inst) => inst.category === cat)
                  .map((inst) => (
                    <div
                      key={inst.id}
                      draggable
                      onDragStart={() => setDragFrom(inst.id)}
                      onDragOver={(e) => e.preventDefault()}
                      onDrop={() => {
                        if (dragFrom && dragFrom !== inst.id) {
                          void connectEdge(dragFrom, inst.id);
                          setDragFrom(null);
                        }
                      }}
                      onClick={() => setSelectedId(inst.id)}
                      className="card"
                      style={{
                        marginBottom: 0,
                        padding: 10,
                        cursor: "grab",
                        borderColor: selectedId === inst.id ? "var(--accent)" : "var(--border)",
                      }}
                    >
                      <div style={{ fontWeight: 600 }}>{inst.id}</div>
                      <div style={{ fontSize: 11, color: "var(--text-muted)" }}>{inst.definition}</div>
                      {Object.entries(inst.inputs).length > 0 && (
                        <div style={{ fontSize: 11, marginTop: 4 }}>
                          {Object.entries(inst.inputs).map(([slot, src]) => (
                            <div key={slot}>
                              <code>{slot}</code> ← <strong>{src}</strong>
                            </div>
                          ))}
                        </div>
                      )}
                      {inst.capabilities.length > 0 && (
                        <div className="row" style={{ marginTop: 6, gap: 4 }}>
                          {inst.capabilities.map((cap) => (
                            <span key={cap} className="badge" style={{ fontSize: 10 }}>
                              {cap}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
              </div>
            </div>
          ))}
        </div>
      </div>

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
              inputs: bindTarget ? { [newInstBindSlot]: bindTarget } : {},
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
            {categories.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
          <input
            type="text"
            placeholder="definition (pkg.def)"
            value={newInstDef}
            onChange={(e) => setNewInstDef(e.target.value)}
            required
          />
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
