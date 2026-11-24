export interface MeResponse {
  authenticated: boolean;
  principal_id: string;
  principal_kind: string;
  csrf_token?: string;
}

export interface LayoutOverride {
  x?: number | null;
  y?: number | null;
  w?: number | null;
  h?: number | null;
  visible?: boolean | null;
  props?: Record<string, unknown>;
}

export interface WidgetNode {
  key?: string;
  widget_id: string;
  widget_type: string;
  props?: Record<string, unknown>;
  children?: WidgetNode[];
  layout_override?: LayoutOverride;
  /** Entry point of a custom widget provider's declared asset (server-provided). */
  asset_url?: string;
}

export interface AdapterRender {
  adapter_instance: string;
  area: string;
  tree: WidgetNode;
}

export interface RenderedDashboard {
  dashboard_id: string;
  title: string;
  layout: string;
  generation: number;
  dashboard_state: Record<string, unknown>;
  /** Per-key record versions, sent back as `expected_version` for compare-and-swap writes. */
  dashboard_state_versions?: Record<string, number>;
  overrides: Record<string, unknown>;
  adapters: AdapterRender[];
}

export interface DashboardPlacement {
  adapter_instance: string;
  area: string;
  query_params: Record<string, unknown>;
}

export interface DashboardDoc {
  id: string;
  title: string;
  layout: string;
  adapters: DashboardPlacement[];
  initial_state: Record<string, unknown>;
  /** Declared Dashboard State keys (schema version, type, size limit, migration policy). */
  state_schema?: Record<string, unknown>;
  overrides: Record<string, LayoutOverride>;
}

export interface InstanceConfig {
  id: string;
  category: string;
  definition: string;
  inputs: Record<string, string>;
  config: unknown;
  secrets: Record<string, string>;
  capabilities: string[];
  run_as?: string | null;
  enabled?: boolean | null;
}

export interface ValidationIssue {
  severity: string;
  code: string;
  target: string;
  message: string;
}

export interface ValidationReport {
  valid: boolean;
  issues: ValidationIssue[];
}

export type InputRefSpec =
  | { ref_kind: "connector"; definition?: string | null; required_capabilities?: string[] }
  | { ref_kind: "data"; definition?: string | null; expected_output?: unknown }
  | { ref_kind: "action"; definition?: string | null };

/** Contract summary of one Definition (`GET /api/definitions`, draft graph `definitions`). */
export interface DefinitionSummary {
  package: string;
  category: string;
  description: string;
  capabilities: string[];
  required_capabilities: string[];
  optional_capabilities: string[];
  inputs: Record<string, InputRefSpec>;
  accepts_requests: string[];
}

export interface GraphInspectResponse {
  revision_commit: string;
  generation_id: number;
  graph_digest: string;
  instances: Record<string, InstanceConfig>;
  dashboards: Record<string, DashboardDoc>;
  service_principals: Array<{
    id: string;
    description: string;
    requested_ceiling: Record<string, unknown>;
    granted_ceiling?: Record<string, unknown> | null;
    granted_by?: string | null;
    granted_at?: string | null;
  }>;
  validation: ValidationReport;
  /** Present on Draft graphs (`GET /api/drafts/{id}/graph`). */
  draft_id?: string;
  base_revision?: string;
  definitions?: Record<string, DefinitionSummary>;
}

export interface DryRunReport {
  action_instance: string;
  feasible: boolean;
  advisory: boolean;
  warnings: string[];
  effects: unknown;
}

/** `POST /api/widget-request/dry-run`: the Request resolved through its Adapter. */
export interface WidgetRequestDryRun {
  protocol: string;
  dashboard_id: string;
  adapter_instance: string;
  widget_id: string;
  request: string;
  resolution: "action" | "refresh" | "unresolved";
  action_instance?: string | null;
  args?: unknown;
  dry_run?: DryRunReport | null;
  dry_run_error?: string | null;
  requires_confirmation: boolean;
}

export interface DefinitionSources {
  definition_id: string;
  package: string;
  category: string;
  package_dir?: string | null;
  definition_dir?: string | null;
  editable: boolean;
  draft_id?: string | null;
  definition_luau: string;
  runtime_file: string;
  runtime_luau: string;
  tests_luau?: string | null;
  generated_luau_types: string;
}

export interface DraftDiffFile {
  path: string;
  status: "added" | "modified" | "deleted" | "renamed" | string;
  patch: string;
}

export interface DraftDiff {
  draft_id: string;
  base_revision: string;
  files: DraftDiffFile[];
  truncated: boolean;
}

export interface GitStatusReport {
  head_commit?: string | null;
  branch: string;
  dirty: boolean;
  changed_files: string[];
  remote_url?: string | null;
  sync_status: string;
  last_error?: string | null;
}

export interface GitCommitInfo {
  hash: string;
  author: string;
  timestamp?: string;
  subject: string;
}

export interface DefinitionTestReport {
  definition_id: string;
  passed: number;
  failed: number;
  cases: Array<{ name: string; passed: boolean; error?: string | null; duration_ms: number }>;
}

export interface PreviewTestReport {
  validation: ValidationReport;
  definition_tests: DefinitionTestReport[];
  scenario_results: Array<{ suite: string; scenario: string; passed: boolean; error?: string | null }>;
  preview_renders: Record<string, unknown>;
  all_passed: boolean;
}

let csrfToken = "";

export function setCsrfToken(token: string) {
  csrfToken = token;
}

export function getCsrfToken(): string {
  return csrfToken;
}

export async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const method = (init.method ?? "GET").toUpperCase();
  const headers = new Headers(init.headers ?? {});
  if (init.body && !headers.has("content-type")) {
    headers.set("content-type", "application/json");
  }
  if (["POST", "PUT", "PATCH", "DELETE"].includes(method) && csrfToken) {
    headers.set("x-csrf-token", csrfToken);
  }
  const resp = await fetch(path, {
    ...init,
    credentials: "include",
    headers,
  });
  const text = await resp.text();
  const data = text ? JSON.parse(text) : {};
  if (!resp.ok) {
    throw new Error(data.error || `HTTP ${resp.status}`);
  }
  return data as T;
}
