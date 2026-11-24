/**
 * Dashboard Designer model (§8, §10.2.1).
 *
 * Designer changes are recorded as a list of edits (patches) instead of a whole Dashboard
 * document. On save the edits are applied to the Dashboard *as it exists in the new Draft's
 * base Revision*, so concurrent changes made by other Administrators or Agents since the page
 * loaded are preserved, and activation's stale-base check still protects the Draft.
 *
 * Layout override semantics (12-column grid, rows of `ROW_UNIT_PX`):
 * - grid children and Adapter roots: `w` = column span, `h` = row span, `x`/`y` = 0-based
 *   column/row start;
 * - container children: `w` = width in twelfths, `h` = minimum height in rows;
 * - `visible: false` hides the node outside the Designer;
 * - `props` overrides only the widget type's configurable props (`CONFIGURABLE_PROPS`).
 *   Custom widget providers expose no configurable props: their props are Adapter data.
 */
import type React from "react";
import type { DashboardDoc, DashboardPlacement, LayoutOverride, WidgetNode } from "./api";

export const GRID_COLUMNS = 12;
export const ROW_UNIT_PX = 48;
export const MAX_ROW_SPAN = 24;

/** Patch for one override; `null` clears a field, `undefined` leaves it unchanged. */
export type OverridePatch = {
  x?: number | null;
  y?: number | null;
  w?: number | null;
  h?: number | null;
  visible?: boolean | null;
};

export type DesignerEdit =
  | { kind: "override"; widgetId: string; patch: OverridePatch }
  | { kind: "override_props"; widgetId: string; props: Record<string, unknown | null> }
  | { kind: "reset_override"; widgetId: string }
  | { kind: "placement_add"; placement: DashboardPlacement }
  | { kind: "placement_remove"; adapterInstance: string }
  | {
      kind: "placement_update";
      adapterInstance: string;
      area?: string;
      queryParams?: Record<string, unknown>;
    }
  | { kind: "placement_move"; adapterInstance: string; toIndex: number };

export interface PropSpec {
  key: string;
  label: string;
  type: "string" | "number" | "enum";
  options?: string[];
}

export const CONFIGURABLE_PROPS: Record<string, PropSpec[]> = {
  "dash.table": [{ key: "title", label: "Title", type: "string" }],
  "dash.chart": [{ key: "title", label: "Title", type: "string" }],
  "dash.map": [{ key: "title", label: "Title", type: "string" }],
  "dash.counter": [{ key: "label", label: "Label", type: "string" }],
  "dash.text": [
    { key: "title", label: "Title", type: "string" },
    { key: "text", label: "Text", type: "string" },
  ],
  "dash.container": [
    { key: "direction", label: "Direction", type: "enum", options: ["vertical", "horizontal"] },
    { key: "gap", label: "Gap (px)", type: "number" },
  ],
  "dash.grid": [
    { key: "columns", label: "Columns", type: "number" },
    { key: "gap", label: "Gap (px)", type: "number" },
  ],
  "dash.button": [{ key: "label", label: "Label", type: "string" }],
  "dash.form": [
    { key: "title", label: "Title", type: "string" },
    { key: "submit_label", label: "Submit label", type: "string" },
  ],
  "dash.filter": [{ key: "label", label: "Label", type: "string" }],
};

export function configurablePropsFor(widgetType: string): PropSpec[] {
  return CONFIGURABLE_PROPS[widgetType] ?? [];
}

/** Node props with the Administrator's configurable-prop overrides applied. */
export function effectiveProps(node: WidgetNode): Record<string, unknown> {
  const base = node.props ?? {};
  const ov = node.layout_override?.props;
  if (!ov) return base;
  const allowed = new Set(configurablePropsFor(node.widget_type).map((p) => p.key));
  const merged: Record<string, unknown> = { ...base };
  for (const [k, v] of Object.entries(ov)) {
    if (allowed.has(k) && v !== null && v !== undefined) merged[k] = v;
  }
  return merged;
}

function clampInt(v: number, lo: number, hi: number): number {
  return Math.min(hi, Math.max(lo, Math.round(v)));
}

function isEmptyOverride(ov: LayoutOverride): boolean {
  return (
    (ov.x ?? null) === null &&
    (ov.y ?? null) === null &&
    (ov.w ?? null) === null &&
    (ov.h ?? null) === null &&
    (ov.visible ?? null) === null &&
    Object.keys(ov.props ?? {}).length === 0
  );
}

function normalizePatch(patch: OverridePatch): OverridePatch {
  const out: OverridePatch = { ...patch };
  if (typeof out.w === "number") out.w = clampInt(out.w, 1, GRID_COLUMNS);
  if (typeof out.h === "number") out.h = clampInt(out.h, 1, MAX_ROW_SPAN);
  if (typeof out.x === "number") out.x = clampInt(out.x, 0, GRID_COLUMNS - 1);
  if (typeof out.y === "number") out.y = clampInt(out.y, 0, 999);
  return out;
}

/** Apply Designer edits to a Dashboard document without mutating it. */
export function applyDesignerEdits(doc: DashboardDoc, edits: DesignerEdit[]): DashboardDoc {
  const overrides: Record<string, LayoutOverride> = {};
  for (const [k, v] of Object.entries(doc.overrides ?? {})) {
    overrides[k] = { ...v, props: { ...(v.props ?? {}) } };
  }
  let adapters: DashboardPlacement[] = (doc.adapters ?? []).map((p) => ({
    ...p,
    query_params: { ...(p.query_params ?? {}) },
  }));

  for (const e of edits) {
    switch (e.kind) {
      case "override": {
        const prev = overrides[e.widgetId] ?? { props: {} };
        const next: LayoutOverride = { ...prev };
        for (const [k, v] of Object.entries(normalizePatch(e.patch))) {
          if (v !== undefined) (next as Record<string, unknown>)[k] = v;
        }
        overrides[e.widgetId] = next;
        if (isEmptyOverride(next)) delete overrides[e.widgetId];
        break;
      }
      case "override_props": {
        const prev = overrides[e.widgetId] ?? { props: {} };
        const props = { ...(prev.props ?? {}) };
        for (const [k, v] of Object.entries(e.props)) {
          if (v === null || v === undefined || v === "") delete props[k];
          else props[k] = v;
        }
        const next = { ...prev, props };
        overrides[e.widgetId] = next;
        if (isEmptyOverride(next)) delete overrides[e.widgetId];
        break;
      }
      case "reset_override":
        delete overrides[e.widgetId];
        break;
      case "placement_add":
        if (!adapters.some((p) => p.adapter_instance === e.placement.adapter_instance)) {
          adapters.push({ ...e.placement, query_params: { ...(e.placement.query_params ?? {}) } });
        }
        break;
      case "placement_remove":
        adapters = adapters.filter((p) => p.adapter_instance !== e.adapterInstance);
        break;
      case "placement_update":
        adapters = adapters.map((p) =>
          p.adapter_instance === e.adapterInstance
            ? {
                ...p,
                area: e.area ?? p.area,
                query_params: e.queryParams ? { ...e.queryParams } : p.query_params,
              }
            : p
        );
        break;
      case "placement_move": {
        const idx = adapters.findIndex((p) => p.adapter_instance === e.adapterInstance);
        if (idx < 0) break;
        const [item] = adapters.splice(idx, 1);
        const to = Math.max(0, Math.min(adapters.length, e.toIndex));
        adapters.splice(to, 0, item);
        break;
      }
    }
  }
  return { ...doc, adapters, overrides };
}

/** Re-attach `layout_override` on every node from an override map (keyed by `widget_id`). */
export function applyOverridesToTree(
  node: WidgetNode,
  overrides: Record<string, LayoutOverride>
): WidgetNode {
  const ov = overrides[node.widget_id];
  const next: WidgetNode = { ...node };
  if (ov) next.layout_override = ov;
  else delete next.layout_override;
  if (node.children) next.children = node.children.map((c) => applyOverridesToTree(c, overrides));
  return next;
}

/** Placement areas in order of first appearance. */
export function groupPlacementsByArea(
  placements: DashboardPlacement[]
): Array<{ area: string; placements: DashboardPlacement[] }> {
  const out: Array<{ area: string; placements: DashboardPlacement[] }> = [];
  for (const p of placements) {
    const area = p.area || "main";
    let group = out.find((g) => g.area === area);
    if (!group) {
      group = { area, placements: [] };
      out.push(group);
    }
    group.placements.push(p);
  }
  return out;
}

/** Style for a child of a 12-column grid (grid children and Adapter roots). */
export function gridItemStyle(
  ov: LayoutOverride | undefined,
  fallbackSpan: number
): React.CSSProperties {
  const w = typeof ov?.w === "number" ? clampInt(ov.w, 1, GRID_COLUMNS) : clampInt(fallbackSpan, 1, GRID_COLUMNS);
  const style: React.CSSProperties = {};
  if (typeof ov?.x === "number") {
    const x = clampInt(ov.x, 0, GRID_COLUMNS - 1);
    style.gridColumn = `${x + 1} / span ${Math.min(w, GRID_COLUMNS - x)}`;
  } else {
    style.gridColumn = `span ${w}`;
  }
  const h = typeof ov?.h === "number" ? clampInt(ov.h, 1, MAX_ROW_SPAN) : null;
  if (typeof ov?.y === "number") {
    style.gridRow = `${clampInt(ov.y, 0, 999) + 1} / span ${h ?? 1}`;
  } else if (h !== null) {
    style.gridRow = `span ${h}`;
  }
  if (h !== null) style.minHeight = h * ROW_UNIT_PX;
  return style;
}

/** Style for a child of a flex container. */
export function containerItemStyle(ov: LayoutOverride | undefined): React.CSSProperties | undefined {
  const style: React.CSSProperties = {};
  if (typeof ov?.w === "number" && ov.w < GRID_COLUMNS) {
    style.width = `${(clampInt(ov.w, 1, GRID_COLUMNS) / GRID_COLUMNS) * 100}%`;
  }
  if (typeof ov?.h === "number") style.minHeight = clampInt(ov.h, 1, MAX_ROW_SPAN) * ROW_UNIT_PX;
  return Object.keys(style).length ? style : undefined;
}

/** Grid geometry of a widget when a drag starts (0-based column/row start, spans). */
export interface DragStart {
  x: number;
  y: number;
  w: number;
  h: number;
}

/** New `w`/`h` after dragging the resize handle by (`dx`, `dy`) pixels. */
export function resizeFromDrag(
  start: DragStart,
  dx: number,
  dy: number,
  colWidth: number
): { w: number; h: number } {
  const dCols = colWidth > 0 ? Math.round(dx / colWidth) : 0;
  const dRows = Math.round(dy / ROW_UNIT_PX);
  return {
    w: clampInt(start.w + dCols, 1, GRID_COLUMNS - Math.min(start.x, GRID_COLUMNS - 1)),
    h: clampInt(start.h + dRows, 1, MAX_ROW_SPAN),
  };
}

/** New `x`/`y` after dragging the move handle by (`dx`, `dy`) pixels. */
export function moveFromDrag(
  start: DragStart,
  dx: number,
  dy: number,
  colWidth: number
): { x: number; y: number } {
  const dCols = colWidth > 0 ? Math.round(dx / colWidth) : 0;
  const dRows = Math.round(dy / ROW_UNIT_PX);
  return {
    x: clampInt(start.x + dCols, 0, Math.max(0, GRID_COLUMNS - start.w)),
    y: clampInt(start.y + dRows, 0, 999),
  };
}

/** Number of pending, distinct widget overrides / placement changes (for the save button). */
export function describeEdits(edits: DesignerEdit[]): string {
  const widgets = new Set<string>();
  let placements = 0;
  for (const e of edits) {
    if ("widgetId" in e) widgets.add(e.widgetId);
    else placements += 1;
  }
  const parts: string[] = [];
  if (widgets.size) parts.push(`${widgets.size} widget override${widgets.size === 1 ? "" : "s"}`);
  if (placements) parts.push(`${placements} placement change${placements === 1 ? "" : "s"}`);
  return parts.join(", ");
}
