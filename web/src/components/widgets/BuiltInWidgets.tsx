import React, { useEffect, useId, useMemo, useRef, useState } from "react";
import type { WidgetNode } from "../../lib/api";
import { scopedClientStateKey, useClientState } from "../../lib/clientState";
import {
  GRID_COLUMNS,
  ROW_UNIT_PX,
  containerItemStyle,
  effectiveProps,
  gridItemStyle,
  moveFromDrag,
  resizeFromDrag,
  type DragStart,
  type OverridePatch,
} from "../../lib/designer";

export interface WidgetRequestContext {
  adapterInstance: string;
  widgetId: string;
  generation: number;
  request: string;
  payload: Record<string, unknown>;
}

interface RendererProps {
  node: WidgetNode;
  adapterInstance: string;
  generation: number;
  onWidgetRequest: (ctx: WidgetRequestContext) => Promise<void>;
  onDashboardStateChange?: (key: string, value: unknown) => Promise<void>;
  designerMode?: boolean;
  onSelectWidget?: (widgetId: string) => void;
  selectedWidgetId?: string | null;
  layoutStyle?: React.CSSProperties;
  /** Dashboard the node is rendered on (namespaces Client State). */
  dashboardId?: string;
  /** Designer: drag/resize handles report new grid geometry for a widget. */
  onLayoutChange?: (widgetId: string, patch: OverridePatch) => void;
  /** Whether the node sits in a 12-column grid (drag/resize handles apply). */
  inGrid?: boolean;
}

type DragMode = "move" | "resize";

export const WidgetRenderer: React.FC<RendererProps> = (rp) => {
  const {
    node,
    adapterInstance,
    generation,
    onWidgetRequest,
    onDashboardStateChange,
    designerMode,
    onSelectWidget,
    selectedWidgetId,
    layoutStyle,
    dashboardId,
    onLayoutChange,
    inGrid,
  } = rp;
  const [drag, setDrag] = useState<{ mode: DragMode; dx: number; dy: number; w: number; h: number } | null>(
    null
  );
  const wrapperRef = useRef<HTMLDivElement>(null);

  if (node.layout_override?.visible === false && !designerMode) {
    return null;
  }

  const props = effectiveProps(node);
  const isSelected = designerMode && selectedWidgetId === node.widget_id;
  const designerStyle: React.CSSProperties | undefined = designerMode
    ? {
        outline: isSelected ? "2px solid var(--accent)" : "1px dashed var(--border)",
        outlineOffset: 2,
        opacity: node.layout_override?.visible === false ? 0.5 : 1,
        position: "relative",
      }
    : undefined;
  const dragStyle: React.CSSProperties | undefined = drag
    ? drag.mode === "move"
      ? { transform: `translate(${drag.dx}px, ${drag.dy}px)`, zIndex: 5 }
      : { width: Math.max(24, drag.w + drag.dx), height: Math.max(24, drag.h + drag.dy), zIndex: 5 }
    : undefined;
  const wrapperStyle: React.CSSProperties | undefined =
    layoutStyle || designerStyle || dragStyle
      ? { ...layoutStyle, ...designerStyle, ...dragStyle }
      : undefined;

  const childCommon = {
    adapterInstance,
    generation,
    onWidgetRequest,
    onDashboardStateChange,
    designerMode,
    onSelectWidget,
    selectedWidgetId,
    dashboardId,
    onLayoutChange,
  };

  const startDrag = (mode: DragMode) => (e: React.PointerEvent) => {
    if (!onLayoutChange) return;
    e.preventDefault();
    e.stopPropagation();
    const el = wrapperRef.current;
    const parent = el?.parentElement;
    const pr = parent?.getBoundingClientRect();
    const er = el?.getBoundingClientRect();
    const colW = pr && pr.width > 0 ? pr.width / GRID_COLUMNS : 0;
    const ov = node.layout_override ?? {};
    const start: DragStart = {
      x: typeof ov.x === "number" ? ov.x : colW && pr && er ? Math.round((er.left - pr.left) / colW) : 0,
      y: typeof ov.y === "number" ? ov.y : pr && er ? Math.round((er.top - pr.top) / ROW_UNIT_PX) : 0,
      w: typeof ov.w === "number" ? ov.w : colW && er ? Math.max(1, Math.round(er.width / colW)) : GRID_COLUMNS,
      h: typeof ov.h === "number" ? ov.h : er ? Math.max(1, Math.round(er.height / ROW_UNIT_PX)) : 1,
    };
    const sx = Number.isFinite(e.clientX) ? e.clientX : 0;
    const sy = Number.isFinite(e.clientY) ? e.clientY : 0;
    const baseW = er?.width ?? 0;
    const baseH = er?.height ?? 0;
    setDrag({ mode, dx: 0, dy: 0, w: baseW, h: baseH });
    const onMove = (ev: PointerEvent) => {
      setDrag({ mode, dx: ev.clientX - sx, dy: ev.clientY - sy, w: baseW, h: baseH });
    };
    const onUp = (ev: PointerEvent) => {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
      setDrag(null);
      const dx = ev.clientX - sx;
      const dy = ev.clientY - sy;
      if (Math.abs(dx) < 3 && Math.abs(dy) < 3) return;
      onLayoutChange(
        node.widget_id,
        mode === "resize" ? resizeFromDrag(start, dx, dy, colW) : moveFromDrag(start, dx, dy, colW)
      );
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
  };

  const showHandles = designerMode && inGrid && Boolean(onLayoutChange);

  const wrap = (inner: React.ReactNode) => (
    <div
      ref={wrapperRef}
      key={node.widget_id}
      data-widget-id={node.widget_id}
      style={wrapperStyle}
      onClick={
        designerMode && onSelectWidget
          ? (e) => {
              e.stopPropagation();
              onSelectWidget(node.widget_id);
            }
          : undefined
      }
    >
      {designerMode && (
        <div className="designer-node-label">
          {showHandles && (
            <span
              role="button"
              tabIndex={-1}
              aria-label={`Move ${node.widget_id}`}
              title="Drag to move (sets x/y)"
              className="designer-handle move"
              onPointerDown={startDrag("move")}
            >
              ⠿
            </span>
          )}
          {node.widget_id} ({node.widget_type})
        </div>
      )}
      {inner}
      {showHandles && (
        <span
          role="button"
          tabIndex={-1}
          aria-label={`Resize ${node.widget_id}`}
          title="Drag to resize (sets w/h)"
          className="designer-handle resize"
          onPointerDown={startDrag("resize")}
        />
      )}
    </div>
  );

  switch (node.widget_type) {
    case "dash.container": {
      const dir = props.direction === "horizontal" ? "row" : "column";
      const gap = typeof props.gap === "number" ? props.gap : 12;
      return wrap(
        <div style={{ display: "flex", flexDirection: dir, gap }}>
          {(node.children ?? []).map((child) => (
            <WidgetRenderer
              key={child.widget_id}
              node={child}
              {...childCommon}
              layoutStyle={containerItemStyle(child.layout_override)}
            />
          ))}
        </div>
      );
    }

    case "dash.grid": {
      const cols = Math.max(1, typeof props.columns === "number" ? props.columns : 2);
      const gap = typeof props.gap === "number" ? props.gap : 12;
      const children = node.children ?? [];
      const visibleChildren = children.filter(
        (c) => designerMode || c.layout_override?.visible !== false
      );
      let explicitSum = 0;
      let unsetCount = 0;
      for (const child of visibleChildren) {
        const w = child.layout_override?.w;
        if (typeof w === "number") {
          explicitSum += Math.min(GRID_COLUMNS, Math.max(1, w));
        } else {
          unsetCount += 1;
        }
      }
      const fallbackUnsetSpan =
        explicitSum < GRID_COLUMNS && unsetCount > 0 && visibleChildren.length <= cols
          ? Math.max(1, Math.floor((GRID_COLUMNS - explicitSum) / unsetCount))
          : Math.max(1, Math.floor(GRID_COLUMNS / cols));

      return wrap(
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(12, minmax(0, 1fr))",
            gridAutoRows: `minmax(${ROW_UNIT_PX}px, auto)`,
            gap,
          }}
        >
          {children.map((child) => (
            <WidgetRenderer
              key={child.widget_id}
              node={child}
              {...childCommon}
              inGrid
              layoutStyle={gridItemStyle(child.layout_override, fallbackUnsetSpan)}
            />
          ))}
        </div>
      );
    }

    case "dash.tabs":
      return wrap(
        <TabsWidget
          tabs={(node.children ?? []).map((c, idx) => ({
            id: c.widget_id,
            label: String(c.props?.title ?? c.key ?? `Tab ${idx + 1}`),
            node: c,
          }))}
          common={childCommon}
        />
      );

    case "dash.counter": {
      const status = String(props.status ?? "healthy");
      return wrap(
        <div className="card" style={{ marginBottom: 0 }}>
          <div style={{ fontSize: 12, color: "var(--text-muted)" }}>
            {String(props.label ?? "Metric")}
          </div>
          <div className="row" style={{ justifyContent: "space-between", marginTop: 6 }}>
            <strong style={{ fontSize: 28 }}>{String(props.value ?? 0)}</strong>
            <span className={`badge ${status === "warning" ? "warn" : "ok"}`}>{status}</span>
          </div>
        </div>
      );
    }

    case "dash.text":
      return wrap(
        <div className="card" style={{ marginBottom: 0 }}>
          {Boolean(props.title) && <h4 style={{ margin: "0 0 6px" }}>{String(props.title)}</h4>}
          <p style={{ margin: 0 }}>{String(props.text ?? "")}</p>
        </div>
      );

    case "dash.chart":
      return wrap(
        <ChartWidget
          title={String(props.title ?? "Trend")}
          series={(Array.isArray(props.series) ? props.series : []) as Array<Record<string, unknown>>}
          yKey={String(props.y_key ?? "value_num")}
          xKey={String(props.x_key ?? "sampled_at")}
        />
      );

    case "dash.map":
      return wrap(
        <MapWidget
          title={String(props.title ?? "Locations")}
          points={(Array.isArray(props.points) ? props.points : []) as Array<Record<string, unknown>>}
        />
      );

    case "dash.table": {
      const scope = { dashboardId, widgetId: node.widget_id };
      return wrap(
        <TableWidget
          title={String(props.title ?? "Table")}
          columns={(Array.isArray(props.columns) ? props.columns : []) as Array<{ key: string; label: string; type?: string }>}
          rows={(Array.isArray(props.rows) ? props.rows : []) as Array<Record<string, unknown>>}
          sortStateKey={scopedClientStateKey(scope, String(props.client_sort_state_key ?? "sort"))}
          filterStateKey={scopedClientStateKey(scope, String(props.client_filter_state_key ?? "filter"))}
          rowAction={props.row_action as { label: string; request: string } | undefined}
          onRowAction={(row) =>
            onWidgetRequest({
              adapterInstance,
              widgetId: node.widget_id,
              generation,
              request: String((props.row_action as { request?: string })?.request ?? "action"),
              payload: row,
            })
          }
        />
      );
    }

    case "dash.button":
      return wrap(
        <button
          type="button"
          className="btn primary"
          onClick={() =>
            onWidgetRequest({
              adapterInstance,
              widgetId: node.widget_id,
              generation,
              request: String(props.request ?? "clicked"),
              payload: (props.payload as Record<string, unknown>) ?? {},
            })
          }
        >
          {String(props.label ?? "Run")}
        </button>
      );

    case "dash.filter": {
      const stateKey = String(props.state_key ?? "filter");
      return wrap(
        <FilterWidget
          label={String(props.label ?? stateKey)}
          options={(Array.isArray(props.options) ? props.options : []) as string[]}
          initialValue={String(props.value ?? "")}
          onChange={(val) => onDashboardStateChange?.(stateKey, val)}
        />
      );
    }

    case "dash.form":
      return wrap(
        <FormWidget
          title={String(props.title ?? "Submit")}
          fields={(Array.isArray(props.fields) ? props.fields : []) as Array<{ name: string; label: string }>}
          submitLabel={String(props.submit_label ?? "Submit")}
          onSubmit={(values) =>
            onWidgetRequest({
              adapterInstance,
              widgetId: node.widget_id,
              generation,
              request: String(props.request ?? "submitted"),
              payload: values,
            })
          }
        />
      );

    default:
      // Custom widget_provider rendered strictly inside sandboxed iframe (`allow-scripts` WITHOUT `allow-same-origin`, §10.1)
      return wrap(
        <SandboxedWidgetFrame
          defId={node.widget_type}
          widgetId={node.widget_id}
          adapterInstance={adapterInstance}
          generation={generation}
          props={props}
          assetUrl={node.asset_url}
          onWidgetRequest={onWidgetRequest}
        />
      );
  }
};

// ---- Sortable + Responsive Data Table (responsive-table + sortable-table guidance) ----

const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: "base" });

const TableWidget: React.FC<{
  title: string;
  columns: Array<{ key: string; label: string; type?: string }>;
  rows: Array<Record<string, unknown>>;
  sortStateKey: string;
  filterStateKey: string;
  rowAction?: { label: string; request: string };
  onRowAction: (row: Record<string, unknown>) => Promise<void>;
}> = ({ title, columns, rows, sortStateKey, filterStateKey, rowAction, onRowAction }) => {
  const captionId = useId();
  const [sortState, setSortState] = useClientState<{ key: string; dir: "ascending" | "descending" } | null>(
    sortStateKey,
    null
  );
  const [filterText, setFilterText] = useClientState<string>(filterStateKey, "");
  const [statusMsg, setStatusMsg] = useState("");
  const [busyKey, setBusyKey] = useState<string | null>(null);

  const filtered = useMemo(() => {
    const q = filterText.trim().toLowerCase();
    const base = q
      ? rows.filter((r) => JSON.stringify(r).toLowerCase().includes(q))
      : [...rows];
    if (sortState) {
      const col = columns.find((c) => c.key === sortState.key);
      const factor = sortState.dir === "ascending" ? 1 : -1;
      base.sort((a, b) => {
        const av = a[sortState.key];
        const bv = b[sortState.key];
        if (col?.type === "number" || (typeof av === "number" && typeof bv === "number")) {
          return factor * (Number(av ?? 0) - Number(bv ?? 0));
        }
        return factor * collator.compare(String(av ?? ""), String(bv ?? ""));
      });
    }
    return base;
  }, [rows, filterText, sortState, columns]);

  const toggleSort = (key: string, label: string) => {
    const nextDir: "ascending" | "descending" =
      sortState?.key === key && sortState.dir === "ascending" ? "descending" : "ascending";
    setSortState({ key, dir: nextDir });
    setStatusMsg(`Sorted by ${label}, ${nextDir}, ${filtered.length} rows`);
  };

  return (
    <div className="card" style={{ marginBottom: 0 }}>
      <div className="row" style={{ justifyContent: "space-between", marginBottom: 10 }}>
        <strong id={captionId}>{title}</strong>
        <label className="row" style={{ fontSize: 12 }}>
          <span>Client filter:</span>
          <input
            type="search"
            aria-label={`Filter ${title}`}
            value={filterText}
            onChange={(e) => setFilterText(e.target.value)}
            placeholder="Search rows..."
          />
        </label>
      </div>
      <div role="status" className="sr-only">
        {statusMsg}
      </div>
      <div tabIndex={0} role="region" aria-labelledby={captionId} className="table-wrapper">
        <table>
          <caption className="sr-only">{title}</caption>
          <thead>
            <tr>
              {columns.map((c) => (
                <th
                  key={c.key}
                  scope="col"
                  aria-sort={sortState?.key === c.key ? sortState.dir : undefined}
                >
                  <button type="button" onClick={() => toggleSort(c.key, c.label)}>
                    {c.label}
                  </button>
                </th>
              ))}
              {rowAction && <th scope="col">Action</th>}
            </tr>
          </thead>
          <tbody>
            {filtered.map((row, idx) => {
              const rowId = String(row.id ?? row.number ?? idx);
              return (
                <tr key={rowId}>
                  {columns.map((c) => (
                    <td key={c.key} data-label={c.label}>
                      {String(row[c.key] ?? "—")}
                    </td>
                  ))}
                  {rowAction && (
                    <td data-label="Action">
                      <button
                        type="button"
                        className="btn"
                        disabled={busyKey === rowId}
                        onClick={async () => {
                          setBusyKey(rowId);
                          try {
                            await onRowAction(row);
                          } finally {
                            setBusyKey(null);
                          }
                        }}
                      >
                        {busyKey === rowId ? "Running..." : rowAction.label}
                      </button>
                    </td>
                  )}
                </tr>
              );
            })}
            {filtered.length === 0 && (
              <tr>
                <td colSpan={columns.length + (rowAction ? 1 : 0)}>No matching rows</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};

function formatAxisTime(raw: unknown): string {
  const s = String(raw ?? "");
  const iso = s.match(/^(\d{4})-(\d{2}-\d{2})T(\d{2}:\d{2})/);
  if (iso) {
    return `${iso[2]} ${iso[3]}`;
  }
  return s.length > 14 ? `${s.slice(0, 14)}...` : s;
}

const ChartWidget: React.FC<{
  title: string;
  series: Array<Record<string, unknown>>;
  yKey: string;
  xKey: string;
}> = ({ title, series, yKey, xKey }) => {
  const vals = series.map((s) => Number(s[yKey] ?? 0));
  const maxVal = Math.max(1, ...vals);
  const w = 380;
  const h = 130;
  const padLeft = 32;
  const padRight = 14;
  const padTop = 12;
  const padBottom = 22;
  const plotW = w - padLeft - padRight;
  const plotH = h - padTop - padBottom;
  const coords = vals.map((v, i) => {
    const x = vals.length <= 1 ? padLeft + plotW / 2 : padLeft + (i / (vals.length - 1)) * plotW;
    const y = padTop + plotH - (v / maxVal) * plotH;
    return { x, y, v, rawX: series[i]?.[xKey] ?? i };
  });
  const points = coords.map((c) => `${c.x.toFixed(1)},${c.y.toFixed(1)}`).join(" ");
  const yTicks = maxVal >= 2 ? [0, Math.round(maxVal / 2), maxVal] : [0, maxVal];

  return (
    <div className="card" style={{ marginBottom: 0 }}>
      <div className="row" style={{ justifyContent: "space-between", marginBottom: 6 }}>
        <strong>{title}</strong>
        <span className="badge">{series.length} samples</span>
      </div>
      <svg viewBox={`0 0 ${w} ${h}`} width="100%" height={h} role="img" aria-label={title}>
        {yTicks.map((tick) => {
          const y = padTop + plotH - (tick / maxVal) * plotH;
          return (
            <g key={tick}>
              <line
                x1={padLeft}
                y1={y}
                x2={w - padRight}
                y2={y}
                stroke="var(--border)"
                strokeDasharray={tick === 0 ? undefined : "3 3"}
              />
              <text
                x={padLeft - 6}
                y={y + 3}
                textAnchor="end"
                fontSize="10"
                fill="var(--text-muted)"
              >
                {tick}
              </text>
            </g>
          );
        })}
        {coords.length > 1 && (
          <polyline fill="none" stroke="var(--accent)" strokeWidth="2.5" points={points} />
        )}
        {coords.map((c, i) => (
          <circle key={String(c.rawX ?? i)} cx={c.x} cy={c.y} r="3.5" fill="var(--accent)">
            <title>{`${formatAxisTime(c.rawX)}: ${c.v}`}</title>
          </circle>
        ))}
        {coords.length === 1 && (
          <text
            x={coords[0].x}
            y={h - 5}
            textAnchor="middle"
            fontSize="10"
            fill="var(--text-muted)"
          >
            {formatAxisTime(coords[0].rawX)}
          </text>
        )}
        {coords.length > 1 && (
          <>
            <text x={padLeft} y={h - 5} textAnchor="start" fontSize="10" fill="var(--text-muted)">
              {formatAxisTime(coords[0].rawX)}
            </text>
            <text x={w - padRight} y={h - 5} textAnchor="end" fontSize="10" fill="var(--text-muted)">
              {formatAxisTime(coords[coords.length - 1].rawX)}
            </text>
          </>
        )}
      </svg>
    </div>
  );
};

const MapWidget: React.FC<{ title: string; points: Array<Record<string, unknown>> }> = ({
  title,
  points,
}) => (
  <div className="card" style={{ marginBottom: 0 }}>
    <strong>{title}</strong>
    <svg viewBox="0 0 360 140" width="100%" height={140} role="img" aria-label={title}>
      <rect x="0" y="0" width="360" height="140" fill="var(--surface-elevated)" rx="6" />
      {points.map((p, idx) => {
        const lon = Number(p.lon ?? 0);
        const lat = Number(p.lat ?? 0);
        const x = ((lon + 180) / 360) * 340 + 10;
        const y = ((90 - lat) / 180) * 120 + 10;
        return <circle key={idx} cx={x} cy={y} r="4.5" fill="var(--accent)" />;
      })}
    </svg>
  </div>
);

const TabsWidget: React.FC<{
  tabs: Array<{ id: string; label: string; node: WidgetNode }>;
  common: Omit<RendererProps, "node" | "layoutStyle" | "inGrid">;
}> = ({ tabs, common }) => {
  const [selectedIdx, setSelectedIdx] = useState(0);
  return (
    <div className="widget-tabs card" style={{ marginBottom: 0 }}>
      <div role="tablist" aria-label="Widget tabs">
        {tabs.map((t, i) => (
          <button
            key={t.id}
            id={`tab-${t.id}`}
            type="button"
            role="tab"
            aria-selected={i === selectedIdx}
            onClick={() => setSelectedIdx(i)}
          >
            {t.label}
          </button>
        ))}
      </div>
      {tabs.map((t, i) => (
        <div
          key={t.id}
          role="tabpanel"
          aria-labelledby={`tab-${t.id}`}
          hidden={i !== selectedIdx}
          tabIndex={i === selectedIdx ? 0 : undefined}
        >
          <div style={{ paddingTop: 12 }}>
            <WidgetRenderer node={t.node} {...common} />
          </div>
        </div>
      ))}
    </div>
  );
};

const FilterWidget: React.FC<{
  label: string;
  options: string[];
  initialValue: string;
  onChange: (val: string) => void;
}> = ({ label, options, initialValue, onChange }) => (
  <label className="row">
    <span>{label}:</span>
    <select value={initialValue} onChange={(e) => onChange(e.target.value)}>
      {options.map((o) => (
        <option key={o} value={o}>
          {o}
        </option>
      ))}
    </select>
  </label>
);

const FormWidget: React.FC<{
  title: string;
  fields: Array<{ name: string; label: string }>;
  submitLabel: string;
  onSubmit: (values: Record<string, unknown>) => Promise<void>;
}> = ({ title, fields, submitLabel, onSubmit }) => {
  const [vals, setVals] = useState<Record<string, string>>({});
  return (
    <form
      className="card col"
      onSubmit={(e) => {
        e.preventDefault();
        void onSubmit(vals);
      }}
    >
      <strong>{title}</strong>
      {fields.map((f) => (
        <label key={f.name} className="col">
          <span>{f.label}</span>
          <input
            type="text"
            value={vals[f.name] ?? ""}
            onChange={(e) => setVals({ ...vals, [f.name]: e.target.value })}
          />
        </label>
      ))}
      <button type="submit" className="btn primary">
        {submitLabel}
      </button>
    </form>
  );
};

/** Default entry point when the server did not supply the provider's declared asset. */
export function defaultWidgetAssetUrl(defId: string): string {
  return `/widgets/${encodeURIComponent(defId)}/assets/index.html`;
}

/** Only same-origin asset URLs of this widget provider are loaded into the frame. */
export function resolveWidgetAssetUrl(defId: string, assetUrl?: string): string {
  const prefix = `/widgets/${encodeURIComponent(defId)}/assets/`;
  if (assetUrl && assetUrl.startsWith(prefix) && !assetUrl.includes("..")) return assetUrl;
  return defaultWidgetAssetUrl(defId);
}

/**
 * Sandboxed custom widget iframe (`sandbox="allow-scripts"` WITHOUT `allow-same-origin`, §10.1).
 *
 * Requests from the frame are tied to the host-rendered `widget_id` and Generation: the
 * Generation the frame reports is never trusted, and messages claiming another Generation are
 * dropped (the frame is showing an outdated render).
 */
export const SandboxedWidgetFrame: React.FC<{
  defId: string;
  widgetId: string;
  adapterInstance: string;
  generation: number;
  props: Record<string, unknown>;
  assetUrl?: string;
  onWidgetRequest: (ctx: WidgetRequestContext) => Promise<void>;
}> = ({ defId, widgetId, adapterInstance, generation, props, assetUrl, onWidgetRequest }) => {
  const frameRef = useRef<HTMLIFrameElement>(null);
  const src = resolveWidgetAssetUrl(defId, assetUrl);

  const pushProps = () => {
    frameRef.current?.contentWindow?.postMessage(
      {
        protocol: "dash.widget/1",
        kind: "props",
        widget_id: widgetId,
        generation,
        props,
      },
      "*"
    );
  };

  useEffect(() => {
    pushProps();
  }, [widgetId, generation, props]);

  useEffect(() => {
    const onMessage = (ev: MessageEvent) => {
      if (ev.source !== frameRef.current?.contentWindow) return;
      const d = ev.data;
      if (
        !d ||
        typeof d !== "object" ||
        d.protocol !== "dash.widget/1" ||
        d.kind !== "request" ||
        d.widget_id !== widgetId ||
        typeof d.request !== "string" ||
        d.request.length === 0 ||
        JSON.stringify(d).length > 16384
      ) {
        return;
      }
      if (d.generation !== undefined && Number(d.generation) !== generation) {
        console.warn(
          `dropping widget Request from ${widgetId}: frame reported Generation ${String(d.generation)}, host rendered ${generation}`
        );
        return;
      }
      const payload =
        d.payload && typeof d.payload === "object" && !Array.isArray(d.payload)
          ? (d.payload as Record<string, unknown>)
          : {};
      void onWidgetRequest({
        adapterInstance,
        widgetId,
        generation,
        request: d.request,
        payload,
      });
    };
    window.addEventListener("message", onMessage);
    return () => window.removeEventListener("message", onMessage);
  }, [widgetId, adapterInstance, generation, onWidgetRequest]);

  return (
    <iframe
      ref={frameRef}
      title={`Custom widget ${widgetId}`}
      src={src}
      sandbox="allow-scripts"
      onLoad={pushProps}
      style={{
        width: "100%",
        height: 54,
        border: "1px solid var(--border)",
        borderRadius: 6,
        background: "transparent",
      }}
    />
  );
};
