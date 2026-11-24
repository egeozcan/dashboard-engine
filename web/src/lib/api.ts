export interface MeResponse {
  authenticated: boolean;
  principal_id: string;
  principal_kind: string;
  csrf_token?: string;
}

export interface WidgetNode {
  key?: string;
  widget_id: string;
  widget_type: string;
  props?: Record<string, unknown>;
  children?: WidgetNode[];
  layout_override?: {
    x?: number | null;
    y?: number | null;
    w?: number | null;
    h?: number | null;
    visible?: boolean | null;
    props?: Record<string, unknown>;
  };
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
  overrides: Record<string, unknown>;
  adapters: AdapterRender[];
}

export interface DashboardDoc {
  id: string;
  title: string;
  layout: string;
  adapters: Array<{
    adapter_instance: string;
    area: string;
    query_params: Record<string, unknown>;
  }>;
  initial_state: Record<string, unknown>;
  overrides: Record<
    string,
    {
      x?: number | null;
      y?: number | null;
      w?: number | null;
      h?: number | null;
      visible?: boolean | null;
      props?: Record<string, unknown>;
    }
  >;
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
}

export interface ValidationIssue {
  severity: string;
  code: string;
  target: string;
  message: string;
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
  validation: {
    valid: boolean;
    issues: ValidationIssue[];
  };
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
