import React, { useEffect, useRef, useState } from "react";
import { apiFetch } from "../lib/api";

export const DiagnosticsView: React.FC = () => {
  const [traceData, setTraceData] = useState<Record<string, unknown> | null>(null);
  const [metrics, setMetrics] = useState<Record<string, unknown> | null>(null);
  const [executions, setExecutions] = useState<Array<Record<string, unknown>>>([]);
  const [logs, setLogs] = useState<Array<Record<string, unknown>>>([]);
  const [secrets, setSecrets] = useState<Array<{ name: string; key_version: number; description: string }>>([]);
  const [agents, setAgents] = useState<Array<{ agent_id: string; name: string; revoked_at?: string | null }>>([]);
  const [sps, setSps] = useState<Array<{ id: string; description: string; requested_ceiling: Record<string, unknown>; granted_ceiling?: Record<string, unknown> | null }>>([]);
  const [audits, setAudits] = useState<Array<Record<string, unknown>>>([]);

  const [secName, setSecName] = useState("");
  const [secVal, setSecVal] = useState("");
  const [agentName, setAgentName] = useState("");
  const [createdToken, setCreatedToken] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const resolveDialogRef = useRef<HTMLDialogElement>(null);
  const [resolveTargetId, setResolveTargetId] = useState<string | null>(null);
  const [resolveReason, setResolveReason] = useState("");

  const loadAll = async () => {
    try {
      const [tr, mt, ex, lg, sc, ag, sp, au] = await Promise.all([
        apiFetch<Record<string, unknown>>("/api/diagnostics/traces"),
        apiFetch<Record<string, unknown>>("/api/diagnostics/metrics"),
        apiFetch<Array<Record<string, unknown>>>("/api/executions"),
        apiFetch<Array<Record<string, unknown>>>("/api/diagnostics/logs"),
        apiFetch<Array<{ name: string; key_version: number; description: string }>>("/api/secrets"),
        apiFetch<Array<{ agent_id: string; name: string; revoked_at?: string | null }>>("/api/agents"),
        apiFetch<Array<{ id: string; description: string; requested_ceiling: Record<string, unknown>; granted_ceiling?: Record<string, unknown> | null }>>("/api/service-principals"),
        apiFetch<Array<Record<string, unknown>>>("/api/diagnostics/audits"),
      ]);
      setTraceData(tr);
      setMetrics(mt);
      setExecutions(ex);
      setLogs(lg);
      setSecrets(sc);
      setAgents(ag);
      setSps(sp);
      setAudits(au);
    } catch (e) {
      setError(String(e));
    }
  };

  useEffect(() => {
    void loadAll();
  }, []);

  const createSecret = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      await apiFetch("/api/secrets", {
        method: "POST",
        body: JSON.stringify({
          name: secName,
          plaintext: secVal,
          allowed_bindings: ["*"],
        }),
      });
      setSecName("");
      setSecVal("");
      await loadAll();
    } catch (err) {
      setError(String(err));
    }
  };

  const createAgent = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      const resp = await apiFetch<{ token: string }>("/api/agents", {
        method: "POST",
        body: JSON.stringify({
          name: agentName,
          ceiling: {
            allowed_providers: ["*"],
            can_author_drafts: true,
            can_activate_drafts: true,
          },
        }),
      });
      setCreatedToken(resp.token);
      setAgentName("");
      await loadAll();
    } catch (err) {
      setError(String(err));
    }
  };

  const approveServicePrincipal = async (sp: { id: string; requested_ceiling: Record<string, unknown> }) => {
    await apiFetch(`/api/service-principals/${encodeURIComponent(sp.id)}/grant`, {
      method: "POST",
      body: JSON.stringify(sp.requested_ceiling),
    });
    await loadAll();
  };

  return (
    <div>
      {error && <div className="card badge danger" style={{ display: "block" }}>{error}</div>}

      <div className="grid-2">
        <div className="card">
          <div className="row" style={{ justifyContent: "space-between" }}>
            <strong>Read-Only Execution Graph &amp; Traces</strong>
            <button type="button" className="btn" onClick={() => void loadAll()}>
              Refresh
            </button>
          </div>
          <p style={{ fontSize: 12, color: "var(--text-muted)" }}>
            Runtime-derived query nodes, cache hit/miss counts, dynamic edges, and spans (read-only; distinct from Configured Graph).
          </p>
          <pre style={{ fontSize: 11, maxHeight: 240, overflow: "auto", background: "var(--bg)", padding: 8, borderRadius: 6 }}>
            {JSON.stringify(traceData, null, 2)}
          </pre>
        </div>

        <div className="card">
          <strong>Runtime Metrics &amp; Sanitized Logs</strong>
          <pre style={{ fontSize: 11, background: "var(--bg)", padding: 8, borderRadius: 6 }}>
            {JSON.stringify(metrics, null, 2)}
          </pre>
          <pre style={{ fontSize: 11, maxHeight: 150, overflow: "auto", background: "var(--bg)", padding: 8, borderRadius: 6 }}>
            {JSON.stringify(logs.slice(0, 15), null, 2)}
          </pre>
        </div>
      </div>

      {/* Action Executions + Unknown Manual Resolution */}
      <div className="card">
        <strong>Action Executions &amp; Unknown Resolution</strong>
        <div className="table-wrapper" style={{ marginTop: 8 }}>
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Action</th>
                <th>Principal</th>
                <th>Status</th>
                <th>Reason</th>
                <th>Resolve</th>
              </tr>
            </thead>
            <tbody>
              {executions.map((ex) => (
                <tr key={String(ex.execution_id)}>
                  <td><code>{String(ex.execution_id)}</code></td>
                  <td>{String(ex.action_instance)}</td>
                  <td>{String(ex.principal_id)} ({String(ex.principal_kind)})</td>
                  <td>
                    <span className={`badge ${ex.status === "succeeded" ? "ok" : ex.status === "unknown" ? "warn" : "danger"}`}>
                      {String(ex.status)}
                    </span>
                  </td>
                  <td>{String(ex.reason ?? "—")}</td>
                  <td>
                    {ex.status === "unknown" && (
                      <button
                        type="button"
                        className="btn"
                        onClick={() => {
                          setResolveTargetId(String(ex.execution_id));
                          setResolveReason("");
                          resolveDialogRef.current?.showModal();
                        }}
                      >
                        Resolve
                      </button>
                    )}
                  </td>
                </tr>
              ))}
              {executions.length === 0 && (
                <tr><td colSpan={6}>No action executions yet</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Secrets, Agents, Service Principals, Audit Trail */}
      <div className="grid-2">
        <div className="card col">
          <strong>Central Encrypted Secrets (Write-Only)</strong>
          <form className="row" onSubmit={(e) => void createSecret(e)}>
            <input type="text" placeholder="Secret name (e.g. github-work)" value={secName} onChange={(e) => setSecName(e.target.value)} required />
            <input type="password" placeholder="Secret plaintext" value={secVal} onChange={(e) => setSecVal(e.target.value)} required />
            <button type="submit" className="btn primary">Store Secret</button>
          </form>
          <ul>
            {secrets.map((s) => (
              <li key={s.name}><code>{s.name}</code> (v{s.key_version}) — plaintext hidden</li>
            ))}
          </ul>

          <strong style={{ marginTop: 8 }}>Service Principal Grants (§5.1)</strong>
          {sps.map((sp) => (
            <div key={sp.id} className="row" style={{ justifyContent: "space-between", padding: "4px 0" }}>
              <div>
                <strong>{sp.id}</strong>{" "}
                <span className={`badge ${sp.granted_ceiling ? "ok" : "warn"}`}>
                  {sp.granted_ceiling ? "Granted" : "Pending Admin Grant"}
                </span>
              </div>
              <button type="button" className="btn" onClick={() => void approveServicePrincipal(sp)}>
                Grant Requested Ceiling
              </button>
            </div>
          ))}
        </div>

        <div className="card col">
          <strong>Agent MCP Principals &amp; Audit Trail</strong>
          <form className="row" onSubmit={(e) => void createAgent(e)}>
            <input type="text" placeholder="Agent name" value={agentName} onChange={(e) => setAgentName(e.target.value)} required />
            <button type="submit" className="btn primary">Issue Agent Token</button>
          </form>
          {createdToken && (
            <div className="badge warn" style={{ wordBreak: "break-all" }}>
              Token (shown once): <code>{createdToken}</code>
            </div>
          )}
          <ul>
            {agents.map((a) => (
              <li key={a.agent_id} className="row" style={{ justifyContent: "space-between" }}>
                <span>{a.name} (<code>{a.agent_id}</code>) {a.revoked_at ? "[REVOKED]" : ""}</span>
                {!a.revoked_at && (
                  <button
                    type="button"
                    className="btn danger"
                    onClick={async () => {
                      await apiFetch(`/api/agents/${a.agent_id}/revoke`, { method: "POST" });
                      await loadAll();
                    }}
                  >
                    Revoke
                  </button>
                )}
              </li>
            ))}
          </ul>
          <strong>Recent Audit Records</strong>
          <pre style={{ fontSize: 11, maxHeight: 140, overflow: "auto", background: "var(--bg)", padding: 8, borderRadius: 6 }}>
            {JSON.stringify(audits.slice(0, 12), null, 2)}
          </pre>
        </div>
      </div>

      <dialog
        ref={resolveDialogRef}
        aria-labelledby="resolve-dialog-title"
        onClose={() => {
          const dlg = resolveDialogRef.current;
          if (!dlg) return;
          const status = dlg.returnValue;
          dlg.returnValue = "";
          if ((status === "succeeded" || status === "failed") && resolveTargetId) {
            void apiFetch(`/api/executions/${resolveTargetId}/resolve`, {
              method: "POST",
              body: JSON.stringify({ status, reason: resolveReason || "resolved in UI" }),
            }).then(loadAll);
          }
        }}
      >
        <h2 id="resolve-dialog-title" style={{ marginTop: 0 }}>Resolve Unknown Action Execution</h2>
        <label className="col">
          <span>Audit Reason</span>
          <input type="text" value={resolveReason} onChange={(e) => setResolveReason(e.target.value)} placeholder="Verified upstream outcome" />
        </label>
        <form method="dialog" className="row" style={{ justifyContent: "flex-end", marginTop: 14 }}>
          <button value="cancel" className="btn" autoFocus>Cancel</button>
          <button value="failed" className="btn danger">Mark Failed</button>
          <button value="succeeded" className="btn primary">Mark Succeeded</button>
        </form>
      </dialog>
    </div>
  );
};
