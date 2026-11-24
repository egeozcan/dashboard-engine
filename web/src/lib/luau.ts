/**
 * Package Studio language helpers: autocomplete candidates from generated Luau type
 * declarations and the dash SDK, diagnostics mapped to source lines, unified diff parsing.
 */

export type LuauFile = "definition" | "runtime" | "tests";

export interface Diagnostic {
  file: LuauFile | null;
  line: number | null;
  message: string;
  /** Definition ID named by the chunk (`<def>/runtime.luau`), when known. */
  definitionId?: string;
  severity: "error" | "warning";
}

const LUAU_KEYWORDS = [
  "and", "break", "continue", "do", "else", "elseif", "end", "export", "false", "for",
  "function", "if", "in", "local", "nil", "not", "or", "repeat", "return", "then", "true",
  "type", "typeof", "until", "while",
];

/** dash declaration SDK (definition.luau) and test helpers (tests.luau). */
const DASH_SDK = [
  "dash.connector", "dash.mock", "dash.data_provider", "dash.action_provider", "dash.adapter",
  "dash.widget_provider", "dash.library", "dash.automation", "dash.string", "dash.number",
  "dash.integer", "dash.boolean", "dash.any", "dash.enum", "dash.list", "dash.optional",
  "dash.map", "dash.object", "dash.semantic", "dash.connector_ref", "dash.data_ref",
  "dash.action_ref", "dash.test", "dash.assert",
];

/** Host context available to runtime handlers (§6.3). */
const CTX_API = [
  "ctx:await", "ctx.inputs", "ctx.query", "ctx.args", "ctx.config", "ctx.request",
  "ctx.dashboard_state", "ctx.instance_id", "ctx.trace_id", "ctx.now", "ctx:emit", "ctx.state",
];

/** Identifiers declared by generated Luau type declarations (`export type X = { a: T }`). */
export function identifiersFromLuauTypes(src: string): string[] {
  const out = new Set<string>();
  for (const m of src.matchAll(/export\s+type\s+([A-Za-z_][A-Za-z0-9_]*)/g)) out.add(m[1]);
  for (const m of src.matchAll(/([A-Za-z_][A-Za-z0-9_]*)\s*:/g)) out.add(m[1]);
  return [...out];
}

export function completionCandidates(opts: {
  generatedTypes?: string;
  inputNames?: string[];
  source?: string;
}): string[] {
  const set = new Set<string>([...LUAU_KEYWORDS, ...DASH_SDK, ...CTX_API]);
  for (const id of identifiersFromLuauTypes(opts.generatedTypes ?? "")) set.add(id);
  for (const name of opts.inputNames ?? []) set.add(`ctx.inputs.${name}`);
  // Identifiers already used in the buffer.
  for (const m of (opts.source ?? "").matchAll(/[A-Za-z_][A-Za-z0-9_]{2,}/g)) set.add(m[0]);
  return [...set].sort();
}

/** Token being typed at `cursor` (identifiers joined by `.` or `:`). */
export function tokenBeforeCursor(text: string, cursor: number): { token: string; start: number } {
  let i = cursor;
  while (i > 0 && /[A-Za-z0-9_.:]/.test(text[i - 1])) i -= 1;
  return { token: text.slice(i, cursor), start: i };
}

export function filterCompletions(candidates: string[], token: string, limit = 8): string[] {
  if (!token) return [];
  const lower = token.toLowerCase();
  return candidates
    .filter((c) => c.toLowerCase().startsWith(lower) && c !== token)
    .slice(0, limit);
}

function fileFromName(name: string): LuauFile | null {
  const base = name.split("/").pop() ?? name;
  if (base.startsWith("definition")) return "definition";
  if (base.startsWith("runtime")) return "runtime";
  if (base.startsWith("tests")) return "tests";
  return null;
}

/**
 * Extract source-located diagnostics from engine error text: Luau chunk locations
 * (`[string "<def>/runtime.luau"]:12: msg`) and declaration errors
 * (`declaration '<path>/definition.luau': ...:3: msg`).
 */
export function parseLuauDiagnostics(text: string, severity: "error" | "warning" = "error"): Diagnostic[] {
  const out: Diagnostic[] = [];
  const seen = new Set<string>();
  const push = (d: Diagnostic) => {
    const k = `${d.definitionId ?? ""}|${d.file}|${d.line}|${d.message}`;
    if (!seen.has(k)) {
      seen.add(k);
      out.push(d);
    }
  };
  for (const m of text.matchAll(/\[string "([^"]+)"\]:(\d+):\s*([^\n]*)/g)) {
    const chunk = m[1];
    const parts = chunk.split("/");
    push({
      file: fileFromName(chunk),
      line: Number(m[2]),
      message: m[3].trim(),
      definitionId: parts.length > 1 ? parts.slice(0, -1).join("/") : undefined,
      severity,
    });
  }
  for (const m of text.matchAll(/declaration '([^']*definition\.luau)':\s*([^\n]*)/g)) {
    const lineMatch = /:(\d+):\s*(.*)$/.exec(m[2]);
    if (lineMatch && /\[string "/.test(m[2])) continue; // already captured above
    push({
      file: "definition",
      line: lineMatch ? Number(lineMatch[1]) : null,
      message: (lineMatch ? lineMatch[2] : m[2]).trim(),
      severity,
    });
  }
  if (out.length === 0 && text.trim()) {
    push({ file: null, line: null, message: text.trim(), severity });
  }
  return out;
}

export interface DiffLine {
  kind: "add" | "del" | "ctx" | "hunk" | "meta";
  text: string;
  oldNo: number | null;
  newNo: number | null;
}

/** Parse one file's unified diff into numbered lines. */
export function parseUnifiedDiff(patch: string): DiffLine[] {
  const out: DiffLine[] = [];
  let oldNo = 0;
  let newNo = 0;
  let inHunk = false;
  for (const raw of patch.split("\n")) {
    const hunk = /^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@/.exec(raw);
    if (hunk) {
      oldNo = Number(hunk[1]);
      newNo = Number(hunk[2]);
      inHunk = true;
      out.push({ kind: "hunk", text: raw, oldNo: null, newNo: null });
      continue;
    }
    if (!inHunk) {
      if (raw) out.push({ kind: "meta", text: raw, oldNo: null, newNo: null });
      continue;
    }
    if (raw.startsWith("+")) {
      out.push({ kind: "add", text: raw.slice(1), oldNo: null, newNo: newNo++ });
    } else if (raw.startsWith("-")) {
      out.push({ kind: "del", text: raw.slice(1), oldNo: oldNo++, newNo: null });
    } else if (raw.startsWith("\\")) {
      out.push({ kind: "meta", text: raw, oldNo: null, newNo: null });
    } else if (raw !== "") {
      out.push({ kind: "ctx", text: raw.slice(1), oldNo: oldNo++, newNo: newNo++ });
    }
  }
  return out;
}
