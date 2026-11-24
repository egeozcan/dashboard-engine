import React, { useEffect, useState } from "react";
import { apiFetch } from "../lib/api";

interface CatalogItem {
  id: string;
  package: string;
  category: string;
  description: string;
  identity_version: string;
}

export const PackageStudioView: React.FC = () => {
  const [catalog, setCatalog] = useState<CatalogItem[]>([]);
  const [selectedId, setSelectedId] = useState<string>("");
  const [contract, setContract] = useState<Record<string, unknown> | null>(null);
  const [draftId, setDraftId] = useState<string | null>(null);
  const [pkgName, setPkgName] = useState("engineering");
  const [defName, setDefName] = useState("custom_metric");
  const [defSource, setDefSource] = useState(
    'local dash = require("dash")\n\nreturn dash.data_provider {\n    id = "engineering.custom_metric",\n    runtime = "runtime.luau",\n    handler = "query",\n}\n'
  );
  const [runtimeSource, setRuntimeSource] = useState(
    "--!strict\nlocal Runtime = {}\n\nfunction Runtime.query(ctx)\n    return { ok = true, instance = ctx.instance_id }\nend\n\nreturn Runtime\n"
  );
  const [testsSource, setTestsSource] = useState(
    'local dash = require("dash")\nlocal Runtime = require("runtime")\n\ndash.test("smoke", function(make_ctx)\n    local res = Runtime.query(make_ctx({}))\n    dash.assert(res.ok == true, "expected ok")\nend)\n'
  );
  const [gitStatus, setGitStatus] = useState<Record<string, unknown> | null>(null);
  const [gitHistory, setGitHistory] = useState<Array<{ hash: string; author: string; subject: string }>>([]);
  const [output, setOutput] = useState<string>("");
  const [error, setError] = useState<string | null>(null);

  const loadAll = async () => {
    try {
      const [cat, gs, gh] = await Promise.all([
        apiFetch<CatalogItem[]>("/api/catalog"),
        apiFetch<Record<string, unknown>>("/api/git/status"),
        apiFetch<Array<{ hash: string; author: string; subject: string }>>("/api/git/history"),
      ]);
      setCatalog(cat);
      setGitStatus(gs);
      setGitHistory(gh);
      if (cat.length > 0 && !selectedId) {
        setSelectedId(cat[0].id);
      }
    } catch (e) {
      setError(String(e));
    }
  };

  useEffect(() => {
    void loadAll();
  }, []);

  useEffect(() => {
    if (!selectedId) return;
    void apiFetch<Record<string, unknown>>(`/api/contracts/${encodeURIComponent(selectedId)}`)
      .then(setContract)
      .catch((e) => setError(String(e)));
  }, [selectedId]);

  const ensureDraft = async (): Promise<string> => {
    if (draftId) return draftId;
    const d = await apiFetch<{ draft_id: string }>("/api/drafts", { method: "POST" });
    setDraftId(d.draft_id);
    return d.draft_id;
  };

  const saveDefinitionToDraft = async () => {
    setError(null);
    try {
      const did = await ensureDraft();
      const d = await apiFetch(`/api/drafts/${did}/definitions`, {
        method: "POST",
        body: JSON.stringify({
          package: pkgName,
          name: defName,
          definition_luau: defSource,
          runtime_luau: runtimeSource,
          tests_luau: testsSource,
        }),
      });
      setOutput(JSON.stringify(d, null, 2));
    } catch (e) {
      setError(String(e));
    }
  };

  const runPreviewTests = async () => {
    setError(null);
    try {
      const did = await ensureDraft();
      const rep = await apiFetch(`/api/drafts/${did}/preview`, { method: "POST" });
      setOutput(JSON.stringify(rep, null, 2));
    } catch (e) {
      setError(String(e));
    }
  };

  const runSingleDefTest = async () => {
    if (!selectedId) return;
    setError(null);
    try {
      const rep = await apiFetch(`/api/definitions/${encodeURIComponent(selectedId)}/test`, {
        method: "POST",
        body: JSON.stringify({ draft_id: draftId }),
      });
      setOutput(JSON.stringify(rep, null, 2));
    } catch (e) {
      setError(String(e));
    }
  };

  const activateDraft = async () => {
    if (!draftId) return;
    setError(null);
    try {
      const res = await apiFetch(`/api/drafts/${draftId}/activate`, { method: "POST" });
      setDraftId(null);
      setOutput(JSON.stringify(res, null, 2));
      await loadAll();
    } catch (e) {
      setError(String(e));
    }
  };

  const syncGit = async () => {
    setError(null);
    try {
      const res = await apiFetch("/api/git/sync", { method: "POST" });
      setOutput(JSON.stringify(res, null, 2));
      await loadAll();
    } catch (e) {
      setError(String(e));
    }
  };

  return (
    <div className="grid-2">
      <div className="col">
        <div className="card">
          <strong>Package Definitions &amp; Contracts</strong>
          <div style={{ maxHeight: 240, overflow: "auto", marginTop: 8 }}>
            {catalog.map((c) => (
              <div
                key={c.id}
                onClick={() => setSelectedId(c.id)}
                style={{
                  padding: "6px 8px",
                  cursor: "pointer",
                  borderRadius: 6,
                  background: selectedId === c.id ? "var(--surface-elevated)" : "transparent",
                }}
              >
                <strong>{c.id}</strong>{" "}
                <span className="badge">{c.category}</span>
                <div style={{ fontSize: 12, color: "var(--text-muted)" }}>{c.description}</div>
              </div>
            ))}
          </div>
          <div className="row" style={{ marginTop: 10 }}>
            <button type="button" className="btn" onClick={() => void runSingleDefTest()}>
              Run Definition Tests
            </button>
          </div>
        </div>

        {contract && (
          <div className="card">
            <strong>Generated Manifest &amp; Luau Types ({selectedId})</strong>
            {Boolean(contract.generated_luau_types) && (
              <pre style={{ fontSize: 12, background: "var(--bg)", padding: 8, borderRadius: 6, overflow: "auto" }}>
                {String(contract.generated_luau_types)}
              </pre>
            )}
            <pre style={{ fontSize: 11, maxHeight: 220, overflow: "auto", background: "var(--bg)", padding: 8, borderRadius: 6 }}>
              {JSON.stringify(contract, null, 2)}
            </pre>
          </div>
        )}

        <div className="card">
          <div className="row" style={{ justifyContent: "space-between" }}>
            <strong>Git Workspace Status</strong>
            <button type="button" className="btn" onClick={() => void syncGit()}>
              Git Sync Push
            </button>
          </div>
          <pre style={{ fontSize: 11, background: "var(--bg)", padding: 8, borderRadius: 6, overflow: "auto" }}>
            {JSON.stringify({ status: gitStatus, recent_commits: gitHistory.slice(0, 5) }, null, 2)}
          </pre>
        </div>
      </div>

      <div className="col">
        <div className="card col">
          <div className="row" style={{ justifyContent: "space-between" }}>
            <strong>Luau Studio (Draft Authoring)</strong>
            {draftId && <span className="badge warn">Draft: {draftId}</span>}
          </div>
          {error && <div className="badge danger">{error}</div>}
          <div className="row">
            <input type="text" value={pkgName} onChange={(e) => setPkgName(e.target.value)} placeholder="package" />
            <input type="text" value={defName} onChange={(e) => setDefName(e.target.value)} placeholder="definition dir" />
          </div>
          <label className="col">
            <span>definition.luau</span>
            <textarea className="code" style={{ minHeight: 110 }} value={defSource} onChange={(e) => setDefSource(e.target.value)} />
          </label>
          <label className="col">
            <span>runtime.luau</span>
            <textarea className="code" style={{ minHeight: 130 }} value={runtimeSource} onChange={(e) => setRuntimeSource(e.target.value)} />
          </label>
          <label className="col">
            <span>tests.luau</span>
            <textarea className="code" style={{ minHeight: 100 }} value={testsSource} onChange={(e) => setTestsSource(e.target.value)} />
          </label>
          <div className="row">
            <button type="button" className="btn" onClick={() => void saveDefinitionToDraft()}>
              Save to Draft
            </button>
            <button type="button" className="btn" onClick={() => void runPreviewTests()}>
              Preview &amp; Scenario Tests
            </button>
            <button type="button" className="btn primary" disabled={!draftId} onClick={() => void activateDraft()}>
              Commit &amp; Activate
            </button>
          </div>
        </div>

        {output && (
          <div className="card">
            <strong>Studio / Test Output</strong>
            <pre style={{ fontSize: 11, maxHeight: 280, overflow: "auto", background: "var(--bg)", padding: 8, borderRadius: 6 }}>
              {output}
            </pre>
          </div>
        )}
      </div>
    </div>
  );
};
