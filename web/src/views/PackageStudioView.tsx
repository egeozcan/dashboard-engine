import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  apiFetch,
  type DefinitionSources,
  type DefinitionTestReport,
  type DraftDiff,
  type GitCommitInfo,
  type GitStatusReport,
  type PreviewTestReport,
  type ValidationReport,
} from "../lib/api";
import { CodeEditor } from "../components/CodeEditor";
import {
  completionCandidates,
  parseLuauDiagnostics,
  parseUnifiedDiff,
  type Diagnostic,
  type LuauFile,
} from "../lib/luau";

interface CatalogItem {
  id: string;
  package: string;
  category: string;
  description: string;
  identity_version: string;
}

const NEW_DEFINITION_TEMPLATE = {
  definition:
    'local dash = require("dash")\n\nreturn dash.data_provider {\n    id = "engineering.custom_metric",\n    runtime = "runtime.luau",\n    handler = "query",\n}\n',
  runtime:
    "--!strict\nlocal Runtime = {}\n\nfunction Runtime.query(ctx)\n    return { ok = true, instance = ctx.instance_id }\nend\n\nreturn Runtime\n",
  tests:
    'local dash = require("dash")\nlocal Runtime = require("runtime")\n\ndash.test("smoke", function(make_ctx)\n    local res = Runtime.query(make_ctx({}))\n    dash.assert(res.ok == true, "expected ok")\nend)\n',
};

/** Validation issues as (file-less) diagnostics. */
function validationDiagnostics(report: ValidationReport | null | undefined): Diagnostic[] {
  return (report?.issues ?? []).map((i) => ({
    file: null,
    line: null,
    message: `${i.code} ${i.target}: ${i.message}`,
    severity: i.severity === "error" ? "error" : "warning",
  }));
}

type Results =
  | { kind: "tests"; report: DefinitionTestReport }
  | { kind: "preview"; report: PreviewTestReport }
  | { kind: "validation"; report: ValidationReport }
  | { kind: "message"; text: string };

export const PackageStudioView: React.FC = () => {
  const [catalog, setCatalog] = useState<CatalogItem[]>([]);
  const [filter, setFilter] = useState("");
  const [selectedId, setSelectedId] = useState<string>("");
  const [contract, setContract] = useState<Record<string, unknown> | null>(null);
  const [sources, setSources] = useState<DefinitionSources | null>(null);
  const [draftId, setDraftId] = useState<string | null>(null);
  const [pkgName, setPkgName] = useState("engineering");
  const [defName, setDefName] = useState("custom_metric");
  const [files, setFiles] = useState<Record<LuauFile, string>>(NEW_DEFINITION_TEMPLATE);
  const [dirty, setDirty] = useState(false);
  const [activeFile, setActiveFile] = useState<LuauFile>("runtime");
  const [gitStatus, setGitStatus] = useState<GitStatusReport | null>(null);
  const [gitHistory, setGitHistory] = useState<GitCommitInfo[]>([]);
  const [diagnostics, setDiagnostics] = useState<Diagnostic[]>([]);
  const [results, setResults] = useState<Results | null>(null);
  const [diff, setDiff] = useState<DraftDiff | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [focus, setFocus] = useState<{ line: number; seq: number } | null>(null);
  const [busy, setBusy] = useState(false);
  const draftRef = useRef<Promise<string> | null>(null);

  const loadAll = async () => {
    try {
      const [cat, gs, gh] = await Promise.all([
        apiFetch<CatalogItem[]>("/api/catalog"),
        apiFetch<GitStatusReport>("/api/git/status"),
        apiFetch<GitCommitInfo[]>("/api/git/history"),
      ]);
      setCatalog(Array.isArray(cat) ? cat : []);
      setGitStatus(gs);
      setGitHistory(Array.isArray(gh) ? gh : []);
    } catch (e) {
      setError(String(e));
    }
  };

  useEffect(() => {
    void loadAll();
  }, []);

  /** Open a Definition: contract + its sources (from the Draft when one is open). */
  const openDefinition = async (id: string, fromDraft: string | null = draftId) => {
    setSelectedId(id);
    setError(null);
    setDiagnostics([]);
    try {
      const [c, src] = await Promise.all([
        apiFetch<Record<string, unknown>>(`/api/contracts/${encodeURIComponent(id)}`).catch(() => null),
        apiFetch<DefinitionSources>(
          `/api/definitions/${encodeURIComponent(id)}/sources${fromDraft ? `?draft_id=${encodeURIComponent(fromDraft)}` : ""}`
        ),
      ]);
      setContract(c);
      setSources(src);
      if (src.editable && src.package_dir && src.definition_dir) {
        setPkgName(src.package_dir);
        setDefName(src.definition_dir);
      }
      setFiles({
        definition: src.definition_luau,
        runtime: src.runtime_luau,
        tests: src.tests_luau ?? "",
      });
      setDirty(false);
    } catch (e) {
      setError(String(e));
    }
  };

  const newDefinition = () => {
    setSelectedId("");
    setSources(null);
    setContract(null);
    setPkgName("engineering");
    setDefName("custom_metric");
    setFiles(NEW_DEFINITION_TEMPLATE);
    setDiagnostics([]);
    setDirty(true);
  };

  /**
   * The open Draft, begun at most once: concurrent callers share the in-flight creation, so
   * overlapping saves/previews never open (and orphan) a second Draft.
   */
  const ensureDraft = (): Promise<string> => {
    if (draftRef.current) return draftRef.current;
    const p = apiFetch<{ draft_id: string }>("/api/drafts", { method: "POST" }).then((d) => {
      setDraftId(d.draft_id);
      return d.draft_id;
    });
    draftRef.current = p;
    p.catch(() => {
      if (draftRef.current === p) draftRef.current = null;
    });
    return p;
  };
  const forgetDraft = () => {
    draftRef.current = null;
    setDraftId(null);
  };

  const refreshDiff = async (id: string) => {
    try {
      setDiff(await apiFetch<DraftDiff>(`/api/drafts/${id}/diff`));
    } catch (e) {
      setError(String(e));
    }
  };

  /** Validate the Draft; declaration errors and binding issues become diagnostics. */
  const checkDraft = async (id: string) => {
    try {
      const report = await apiFetch<ValidationReport>(`/api/drafts/${id}/validate`, { method: "POST" });
      setResults({ kind: "validation", report });
      setDiagnostics(validationDiagnostics(report));
    } catch (e) {
      setDiagnostics(parseLuauDiagnostics(String(e)));
      setResults({ kind: "message", text: String(e) });
    }
  };

  /** Run one Draft operation at a time; the Draft buttons stay disabled meanwhile. */
  const busyRef = useRef(false);
  const exclusive = (fn: () => Promise<void>) => async () => {
    if (busyRef.current) return;
    busyRef.current = true;
    setBusy(true);
    try {
      await fn();
    } finally {
      busyRef.current = false;
      setBusy(false);
    }
  };

  const saveDefinitionToDraft = exclusive(async () => {
    setError(null);
    try {
      const did = await ensureDraft();
      await apiFetch(`/api/drafts/${did}/definitions`, {
        method: "POST",
        body: JSON.stringify({
          package: pkgName,
          name: defName,
          definition_luau: files.definition,
          runtime_luau: files.runtime,
          tests_luau: files.tests || null,
        }),
      });
      setDirty(false);
      await checkDraft(did);
      await refreshDiff(did);
    } catch (e) {
      setError(String(e));
    }
  });

  const runPreviewTests = exclusive(async () => {
    setError(null);
    try {
      const did = await ensureDraft();
      const rep = await apiFetch<PreviewTestReport>(`/api/drafts/${did}/preview`, { method: "POST" });
      setResults({ kind: "preview", report: rep });
      // Validation failures skip tests and renders, so their issues are the only reason given.
      const diags: Diagnostic[] = validationDiagnostics(rep.validation);
      for (const [dash, err] of Object.entries(rep.preview_render_errors ?? {})) {
        diags.push(
          ...parseLuauDiagnostics(err).map((d) =>
            d.file === null ? { ...d, message: `Dashboard '${dash}' failed to render: ${d.message}` } : d
          )
        );
      }
      for (const t of rep.definition_tests ?? []) {
        for (const c of t.cases ?? []) {
          if (!c.passed && c.error) {
            diags.push(
              ...parseLuauDiagnostics(c.error).map((d) => ({ ...d, definitionId: d.definitionId ?? t.definition_id }))
            );
          }
        }
      }
      for (const s of rep.scenario_results ?? []) {
        if (!s.passed && s.error) diags.push(...parseLuauDiagnostics(s.error));
      }
      setDiagnostics(diags);
    } catch (e) {
      setDiagnostics(parseLuauDiagnostics(String(e)));
      setError(String(e));
    }
  });

  const runSingleDefTest = async () => {
    if (!selectedId) return;
    setError(null);
    try {
      const rep = await apiFetch<DefinitionTestReport>(`/api/definitions/${encodeURIComponent(selectedId)}/test`, {
        method: "POST",
        body: JSON.stringify({ draft_id: draftId }),
      });
      setResults({ kind: "tests", report: rep });
      setDiagnostics(
        (rep.cases ?? [])
          .filter((c) => !c.passed && c.error)
          .flatMap((c) => parseLuauDiagnostics(c.error ?? "").map((d) => ({ ...d, definitionId: d.definitionId ?? selectedId })))
      );
    } catch (e) {
      setDiagnostics(parseLuauDiagnostics(String(e)));
      setError(String(e));
    }
  };

  const activateDraft = exclusive(async () => {
    if (!draftId) return;
    setError(null);
    try {
      const res = await apiFetch<{ generation_id: number; revision_commit: string; sync_status?: string }>(
        `/api/drafts/${draftId}/activate`,
        { method: "POST" }
      );
      forgetDraft();
      setDiff(null);
      setResults({
        kind: "message",
        text: `Activated Generation #${res.generation_id} at ${res.revision_commit.slice(0, 10)}${res.sync_status ? ` (${res.sync_status})` : ""}`,
      });
      await loadAll();
    } catch (e) {
      setError(String(e));
    }
  });

  const discardDraft = exclusive(async () => {
    if (!draftId) return;
    setError(null);
    try {
      await apiFetch(`/api/drafts/${draftId}`, { method: "DELETE" });
      forgetDraft();
      setDiff(null);
      setResults(null);
      if (selectedId) await openDefinition(selectedId, null);
    } catch (e) {
      setError(String(e));
    }
  });

  const syncGit = async () => {
    setError(null);
    try {
      const res = await apiFetch<{ sync_status?: string; last_error?: string | null }>("/api/git/sync", { method: "POST" });
      setResults({ kind: "message", text: `Git sync: ${res.sync_status ?? "done"}${res.last_error ? ` — ${res.last_error}` : ""}` });
      await loadAll();
    } catch (e) {
      setError(String(e));
    }
  };

  const currentDefId = sources?.definition_id ?? selectedId;
  const completions = useMemo(() => {
    const inputs = contract?.inputs && typeof contract.inputs === "object" ? Object.keys(contract.inputs as object) : [];
    return completionCandidates({
      generatedTypes: sources?.generated_luau_types ?? String(contract?.generated_luau_types ?? ""),
      inputNames: inputs,
      source: files[activeFile],
    });
  }, [contract, sources, files, activeFile]);

  const diagnosticsFor = (file: LuauFile) =>
    diagnostics.filter(
      (d) => d.file === file && (!d.definitionId || !currentDefId || d.definitionId === currentDefId)
    );

  const visibleCatalog = catalog.filter(
    (c) =>
      !filter ||
      c.id.toLowerCase().includes(filter.toLowerCase()) ||
      c.category.includes(filter.toLowerCase())
  );
  const readOnly = Boolean(sources && !sources.editable);
  // The Draft definition endpoint writes `runtime.luau`; Definitions declaring another runtime
  // file are edited externally until that endpoint accepts a file name.
  const customRuntimeFile = Boolean(sources && sources.runtime_file !== "runtime.luau");

  return (
    <div className="grid-2">
      <div className="col">
        <div className="card">
          <div className="row" style={{ justifyContent: "space-between" }}>
            <strong>Package Definitions &amp; Contracts</strong>
            <button type="button" className="btn" onClick={newDefinition}>
              New Definition
            </button>
          </div>
          <input
            type="search"
            aria-label="Filter Definitions"
            placeholder="Filter by id or category…"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            style={{ marginTop: 8, width: "100%" }}
          />
          <div style={{ maxHeight: 240, overflow: "auto", marginTop: 8 }} role="listbox" aria-label="Definitions">
            {visibleCatalog.map((c) => (
              <div
                key={c.id}
                role="option"
                aria-selected={selectedId === c.id}
                tabIndex={0}
                onClick={() => void openDefinition(c.id)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") void openDefinition(c.id);
                }}
                style={{
                  padding: "6px 8px",
                  cursor: "pointer",
                  borderRadius: 6,
                  background: selectedId === c.id ? "var(--surface-elevated)" : "transparent",
                }}
              >
                <strong>{c.id}</strong> <span className="badge">{c.category}</span>
                <div style={{ fontSize: 12, color: "var(--text-muted)" }}>{c.description}</div>
              </div>
            ))}
          </div>
          <div className="row" style={{ marginTop: 10 }}>
            <button type="button" className="btn" disabled={!selectedId} onClick={() => void runSingleDefTest()}>
              Run Definition Tests
            </button>
          </div>
        </div>

        {(sources?.generated_luau_types || contract) && (
          <div className="card">
            <strong>Generated Luau Types ({currentDefId || "new"})</strong>
            <pre className="json-block">{sources?.generated_luau_types || String(contract?.generated_luau_types ?? "")}</pre>
            {contract && (
              <details>
                <summary style={{ fontSize: 12 }}>Generated manifest</summary>
                <pre className="json-block" style={{ maxHeight: 220 }}>
                  {JSON.stringify(contract, null, 2)}
                </pre>
              </details>
            )}
          </div>
        )}

        <GitStatusCard status={gitStatus} history={gitHistory} onSync={() => void syncGit()} />
      </div>

      <div className="col">
        <div className="card col">
          <div className="row" style={{ justifyContent: "space-between" }}>
            <strong>Luau Studio (Draft Authoring)</strong>
            <div className="row">
              {dirty && <span className="badge warn">unsaved</span>}
              {draftId && <span className="badge warn">Draft: {draftId}</span>}
            </div>
          </div>
          {error && <div className="badge danger">{error}</div>}
          {readOnly && (
            <div className="badge warn" style={{ display: "block" }}>
              {sources?.package === "dash"
                ? "Built-in Definition: read-only."
                : "Locked external Package: read-only here; edit it in its own repository or copy it into a Workspace Package."}
            </div>
          )}
          <div className="row">
            <label className="col" style={{ fontSize: 12 }}>
              <span>Package directory</span>
              <input type="text" value={pkgName} disabled={readOnly} onChange={(e) => setPkgName(e.target.value)} placeholder="package" />
            </label>
            <label className="col" style={{ fontSize: 12 }}>
              <span>Definition directory</span>
              <input type="text" value={defName} disabled={readOnly} onChange={(e) => setDefName(e.target.value)} placeholder="definition dir" />
            </label>
          </div>
          <div role="tablist" aria-label="Definition files" className="row">
            {(["definition", "runtime", "tests"] as const).map((f) => {
              const n = diagnosticsFor(f).length;
              return (
                <button
                  key={f}
                  type="button"
                  role="tab"
                  aria-selected={activeFile === f}
                  className={`btn ${activeFile === f ? "primary" : ""}`}
                  onClick={() => setActiveFile(f)}
                >
                  {f === "runtime" ? sources?.runtime_file ?? "runtime.luau" : `${f}.luau`}
                  {n > 0 && <span className="badge danger" style={{ marginLeft: 6 }}>{n}</span>}
                </button>
              );
            })}
          </div>
          <CodeEditor
            key={activeFile}
            label={`${activeFile}.luau`}
            value={files[activeFile]}
            readOnly={readOnly}
            completions={completions}
            diagnostics={diagnosticsFor(activeFile)}
            focusRequest={focus}
            minRows={activeFile === "runtime" ? 14 : 10}
            onChange={(next) => {
              setFiles((prev) => ({ ...prev, [activeFile]: next }));
              setDirty(true);
            }}
          />
          <div className="row">
            <button
              type="button"
              className="btn"
              disabled={busy || readOnly || customRuntimeFile}
              title={customRuntimeFile ? `declares runtime = "${sources?.runtime_file}"; edit it in an external IDE` : undefined}
              onClick={() => void saveDefinitionToDraft()}
            >
              Save to Draft &amp; Check
            </button>
            <button type="button" className="btn" disabled={busy} onClick={() => void runPreviewTests()}>
              Preview &amp; Scenario Tests
            </button>
            <button type="button" className="btn" disabled={!draftId} onClick={() => draftId && void refreshDiff(draftId)}>
              Show Diff
            </button>
            <button type="button" className="btn primary" disabled={busy || !draftId} onClick={() => void activateDraft()}>
              Commit &amp; Activate
            </button>
            {draftId && (
              <button type="button" className="btn danger" disabled={busy} onClick={() => void discardDraft()}>
                Discard Draft
              </button>
            )}
          </div>
        </div>

        {diagnostics.length > 0 && (
          <div className="card">
            <strong>Diagnostics</strong>
            <ul className="diagnostic-list">
              {diagnostics.map((d, i) => (
                <li key={i}>
                  <span className={`badge ${d.severity === "error" ? "danger" : "warn"}`}>{d.severity}</span>{" "}
                  {d.file && d.line !== null ? (
                    <button
                      type="button"
                      className="link-btn"
                      onClick={() => {
                        setActiveFile(d.file as LuauFile);
                        setFocus({ line: d.line ?? 1, seq: Date.now() });
                      }}
                    >
                      {d.definitionId ? `${d.definitionId}/` : ""}
                      {d.file}.luau:{d.line}
                    </button>
                  ) : null}{" "}
                  {d.message}
                </li>
              ))}
            </ul>
          </div>
        )}

        {results && <ResultsCard results={results} />}
        {diff && <DiffView diff={diff} />}
      </div>
    </div>
  );
};

const GitStatusCard: React.FC<{
  status: GitStatusReport | null;
  history: GitCommitInfo[];
  onSync: () => void;
}> = ({ status, history, onSync }) => (
  <div className="card col">
    <div className="row" style={{ justifyContent: "space-between" }}>
      <strong>Git Workspace Status</strong>
      <button type="button" className="btn" onClick={onSync}>
        Git Sync Push
      </button>
    </div>
    {status ? (
      <dl className="kv">
        <dt>Branch</dt>
        <dd>
          <code>{status.branch}</code>
        </dd>
        <dt>Active Revision</dt>
        <dd>
          <code>{status.head_commit ? status.head_commit.slice(0, 12) : "none"}</code>
        </dd>
        <dt>Remote</dt>
        <dd>
          <span
            className={`badge ${status.sync_status === "synced" ? "ok" : status.sync_status === "unsynced" ? "warn" : ""}`}
          >
            {status.sync_status}
          </span>{" "}
          {status.remote_url ?? "no remote configured"}
        </dd>
        <dt>Working tree</dt>
        <dd>
          <span className={`badge ${status.dirty ? "warn" : "ok"}`}>{status.dirty ? "uncommitted changes" : "clean"}</span>
        </dd>
        {status.last_error && (
          <>
            <dt>Last push error</dt>
            <dd className="badge danger">{status.last_error}</dd>
          </>
        )}
      </dl>
    ) : (
      <span style={{ color: "var(--text-muted)", fontSize: 12 }}>Loading…</span>
    )}
    {status && status.changed_files.length > 0 && (
      <ul style={{ fontSize: 12, margin: 0 }}>
        {status.changed_files.map((f) => (
          <li key={f}>
            <code>{f}</code>
          </li>
        ))}
      </ul>
    )}
    {history.length > 0 && (
      <>
        <strong style={{ fontSize: 12 }}>Recent commits</strong>
        <ul style={{ fontSize: 12, margin: 0 }}>
          {history.slice(0, 5).map((c) => (
            <li key={c.hash}>
              <code>{c.hash.slice(0, 8)}</code> {c.subject} <span style={{ color: "var(--text-muted)" }}>({c.author})</span>
            </li>
          ))}
        </ul>
      </>
    )}
  </div>
);

const ResultsCard: React.FC<{ results: Results }> = ({ results }) => {
  if (results.kind === "message") {
    return (
      <div className="card">
        <strong>Studio Output</strong>
        <pre className="json-block">{results.text}</pre>
      </div>
    );
  }
  if (results.kind === "validation") {
    const r = results.report;
    return (
      <div className="card">
        <strong>Draft validation</strong>{" "}
        <span className={`badge ${r.valid ? "ok" : "danger"}`}>{r.valid ? "valid" : `${r.issues.length} issue(s)`}</span>
      </div>
    );
  }
  const testReports = results.kind === "tests" ? [results.report] : results.report.definition_tests ?? [];
  return (
    <div className="card col">
      <strong>{results.kind === "tests" ? "Definition tests" : "Preview, Definition & Scenario tests"}</strong>
      {results.kind === "preview" && (
        <div className="row">
          <span className={`badge ${results.report.all_passed ? "ok" : "danger"}`}>
            {results.report.all_passed ? "all passed" : "failures"}
          </span>
          <span className={`badge ${results.report.validation?.valid ? "ok" : "danger"}`}>
            validation {results.report.validation?.valid ? "ok" : "failed"}
          </span>
        </div>
      )}
      <table className="results-table">
        <thead>
          <tr>
            <th scope="col">Test</th>
            <th scope="col">Result</th>
            <th scope="col">Detail</th>
          </tr>
        </thead>
        <tbody>
          {testReports.flatMap((t) =>
            (t.cases ?? []).map((c) => (
              <tr key={`${t.definition_id}:${c.name}`}>
                <td>
                  <code>{t.definition_id}</code> {c.name}
                </td>
                <td>
                  <span className={`badge ${c.passed ? "ok" : "danger"}`}>{c.passed ? "pass" : "fail"}</span>
                </td>
                <td style={{ fontSize: 11 }}>{c.error ?? `${c.duration_ms} ms`}</td>
              </tr>
            ))
          )}
          {results.kind === "preview" &&
            (results.report.scenario_results ?? []).map((s) => (
              <tr key={`${s.suite}:${s.scenario}`}>
                <td>
                  scenario <code>{s.suite}</code> {s.scenario}
                </td>
                <td>
                  <span className={`badge ${s.passed ? "ok" : "danger"}`}>{s.passed ? "pass" : "fail"}</span>
                </td>
                <td style={{ fontSize: 11 }}>{s.error ?? ""}</td>
              </tr>
            ))}
          {results.kind === "preview" &&
            Object.entries(results.report.preview_render_errors ?? {}).map(([dash, err]) => (
              <tr key={`render:${dash}`}>
                <td>
                  render <code>{dash}</code>
                </td>
                <td>
                  <span className="badge danger">fail</span>
                </td>
                <td style={{ fontSize: 11 }}>{err}</td>
              </tr>
            ))}
          {results.kind === "preview" &&
            (results.report.validation?.issues ?? []).map((i, idx) => (
              <tr key={`issue:${idx}`}>
                <td>
                  validation <code>{i.code}</code> {i.target}
                </td>
                <td>
                  <span className={`badge ${i.severity === "error" ? "danger" : "warn"}`}>{i.severity}</span>
                </td>
                <td style={{ fontSize: 11 }}>{i.message}</td>
              </tr>
            ))}
        </tbody>
      </table>
    </div>
  );
};

const DiffView: React.FC<{ diff: DraftDiff }> = ({ diff }) => (
  <div className="card col">
    <div className="row" style={{ justifyContent: "space-between" }}>
      <strong>Draft diff vs base Revision {diff.base_revision.slice(0, 10)}</strong>
      {diff.truncated && <span className="badge warn">truncated</span>}
    </div>
    {diff.files.length === 0 && <span style={{ fontSize: 12, color: "var(--text-muted)" }}>No changes in this Draft.</span>}
    {diff.files.map((f) => (
      <details key={f.path} open>
        <summary>
          <span className={`badge ${f.status === "added" ? "ok" : f.status === "deleted" ? "danger" : "warn"}`}>{f.status}</span>{" "}
          <code>{f.path}</code>
        </summary>
        <table className="diff-table" aria-label={`Diff of ${f.path}`}>
          <tbody>
            {parseUnifiedDiff(f.patch)
              .filter((l) => l.kind !== "meta")
              .map((l, i) => (
                <tr key={i} className={`diff-${l.kind}`}>
                  <td className="diff-no">{l.oldNo ?? ""}</td>
                  <td className="diff-no">{l.newNo ?? ""}</td>
                  <td className="diff-text">
                    {l.kind === "add" ? "+" : l.kind === "del" ? "-" : l.kind === "hunk" ? "" : " "}
                    {l.text}
                  </td>
                </tr>
              ))}
          </tbody>
        </table>
      </details>
    ))}
  </div>
);
