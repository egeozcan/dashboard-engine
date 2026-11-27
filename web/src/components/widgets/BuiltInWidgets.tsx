import React, { useEffect, useId, useRef, useState } from "react";
import type { WidgetNode } from "../../lib/api";
import { scopedClientStateKey, useClientState } from "../../lib/clientState";
import { asRecords, formatOptionsFrom } from "../../lib/widgetFormat";
import {
  ChartWidget,
  normalizeSeriesSpecs,
  normalizeThresholds as normalizeChartThresholds,
} from "./ChartWidget";
import {
  CounterWidget,
  TextWidget,
  counterDelta,
  counterStatus,
  normalizeCounterThresholds,
} from "./DisplayWidgets";
import { ButtonWidget, FilterWidget, FormWidget, type FilterControl } from "./InputWidgets";
import { MapWidget, normalizePoints } from "./MapWidget";
import { TableWidget, normalizeColumns, normalizeRowActions, type SortState } from "./TableWidget";
import {
  GRID_COLUMNS,
  ROW_UNIT_PX,
  containerItemStyle,
  customFrameHeight,
  DEFAULT_FRAME_HEIGHT_PX,
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
  /** Current Dashboard State (drives `dash.filter` values). */
  dashboardState?: Record<string, unknown>;
}

type DragMode = "move" | "resize";

/**
 * Renders one Widget node (and its children). Each node renders inside its own error boundary,
 * so a widget that cannot handle its data never blanks its siblings or the Dashboard.
 */
export const WidgetRenderer: React.FC<RendererProps> = (rp) => (
  <WidgetErrorBoundary node={rp.node} layoutStyle={rp.layoutStyle}>
    <WidgetRendererImpl {...rp} />
  </WidgetErrorBoundary>
);

const WidgetRendererImpl: React.FC<RendererProps> = (rp) => {
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
    dashboardState,
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
    dashboardState,
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

  const request = (name: string, payload: Record<string, unknown>) =>
    onWidgetRequest({ adapterInstance, widgetId: node.widget_id, generation, request: name, payload });
  const scope = { dashboardId, widgetId: node.widget_id };

  switch (node.widget_type) {
    case "dash.container": {
      const dir = props.direction === "horizontal" ? "row" : "column";
      const gap = num(props.gap) ?? 12;
      const body = (
        <div
          style={{
            display: "flex",
            flexDirection: dir,
            gap,
            flexWrap: props.wrap === true ? "wrap" : undefined,
            alignItems: FLEX_ALIGN[String(props.align)],
            justifyContent: FLEX_JUSTIFY[String(props.justify)],
          }}
        >
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
      return wrap(
        typeof props.title === "string" && props.title ? (
          <section className="dash-section" aria-label={props.title}>
            <h3 className="dash-section-title">{props.title}</h3>
            {body}
          </section>
        ) : (
          body
        )
      );
    }

    case "dash.grid": {
      const cols = Math.max(1, Math.min(GRID_COLUMNS, Math.round(num(props.columns) ?? 2)));
      const gap = num(props.gap) ?? 12;
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

      const grid = (
        // The host is a size container: on narrow screens every cell spans the full width.
        <div className="dash-grid-host">
          <div
            className="dash-grid"
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
        </div>
      );
      return wrap(
        typeof props.title === "string" && props.title ? (
          <section className="dash-section" aria-label={props.title}>
            <h3 className="dash-section-title">{props.title}</h3>
            {grid}
          </section>
        ) : (
          grid
        )
      );
    }

    case "dash.tabs":
      return wrap(
        <TabsWidget
          label={str(props.title, "Widget tabs")}
          tabs={(node.children ?? []).map((c, idx) => ({
            id: c.widget_id,
            key: c.key,
            label: String(c.props?.tab_label ?? c.props?.title ?? c.key ?? `Tab ${idx + 1}`),
            node: c,
          }))}
          stateKey={scopedClientStateKey(scope, str(props.client_state_key, "tab"))}
          defaultTab={props.default_tab}
          common={childCommon}
        />
      );

    case "dash.counter": {
      const fmt = formatOptionsFrom(props);
      return wrap(
        <CounterWidget
          label={str(props.label, "Metric")}
          value={props.value}
          fmt={fmt}
          status={counterStatus(props.value, props.status, normalizeCounterThresholds(props.thresholds))}
          delta={counterDelta({
            value: props.value,
            previous: props.previous,
            delta: props.delta,
            deltaFormat: typeof props.delta_format === "string" ? props.delta_format : undefined,
            higherIsBetter: props.higher_is_better !== false,
            fmt,
          })}
          deltaLabel={typeof props.delta_label === "string" ? props.delta_label : undefined}
          description={typeof props.description === "string" ? props.description : undefined}
          sparkline={Array.isArray(props.sparkline) ? props.sparkline : undefined}
        />
      );
    }

    case "dash.text":
      return wrap(
        <TextWidget
          title={props.title ? String(props.title) : undefined}
          text={str(props.text, "")}
          markdown={props.markdown === true || props.format === "markdown"}
          variant={typeof props.variant === "string" ? props.variant : undefined}
          renderError={Boolean(node.render_error)}
        />
      );

    case "dash.chart": {
      const yKey = str(props.y_key, "value_num");
      const kind = props.kind === "bar" || props.kind === "area" ? props.kind : "line";
      return wrap(
        <ChartWidget
          title={str(props.title, "Trend")}
          rows={asRecords(props.series)}
          xKey={str(props.x_key, "sampled_at")}
          series={normalizeSeriesSpecs(
            props.y_keys,
            yKey,
            typeof props.series_label === "string" ? props.series_label : undefined
          )}
          kind={kind}
          height={num(props.height)}
          yMin={num(props.y_min)}
          yMax={num(props.y_max)}
          zeroBaseline={props.zero_baseline !== false}
          fmt={formatOptionsFrom(props)}
          xFormat={typeof props.x_format === "string" ? props.x_format : undefined}
          thresholds={normalizeChartThresholds(props.thresholds)}
          showLegend={typeof props.show_legend === "boolean" ? props.show_legend : undefined}
          emptyMessage={typeof props.empty_message === "string" ? props.empty_message : undefined}
        />
      );
    }

    case "dash.map": {
      const { points, dropped } = normalizePoints(props.points);
      const pa = isRecord(props.point_action) ? props.point_action : null;
      const pointAction =
        pa && typeof pa.request === "string" && pa.request
          ? { label: str(pa.label, "Open"), request: pa.request }
          : undefined;
      return wrap(
        <MapWidget
          title={str(props.title, "Locations")}
          points={points}
          dropped={dropped}
          fit={props.fit !== false}
          height={num(props.height)}
          fmt={formatOptionsFrom(props)}
          pointAction={pointAction}
          onPointAction={pointAction ? (p) => request(pointAction.request, p) : undefined}
        />
      );
    }

    case "dash.table": {
      const ds = isRecord(props.default_sort) ? props.default_sort : null;
      const defaultSort: SortState =
        ds && typeof ds.key === "string"
          ? { key: ds.key, dir: ds.dir === "desc" || ds.dir === "descending" ? "descending" : "ascending" }
          : null;
      const pageSize = num(props.page_size);
      return wrap(
        <TableWidget
          title={str(props.title, "Table")}
          columns={normalizeColumns(props.columns)}
          rows={asRecords(props.rows)}
          sortStateKey={scopedClientStateKey(scope, str(props.client_sort_state_key, "sort"))}
          filterStateKey={scopedClientStateKey(scope, str(props.client_filter_state_key, "filter"))}
          defaultSort={defaultSort}
          searchable={props.searchable !== false}
          pageSize={pageSize === undefined ? undefined : Math.max(0, Math.floor(pageSize))}
          emptyMessage={typeof props.empty_message === "string" ? props.empty_message : undefined}
          exportable={props.exportable === true}
          rowKey={typeof props.row_key === "string" ? props.row_key : undefined}
          rowActions={normalizeRowActions(props.row_actions, props.row_action)}
          onRowAction={(action, row) => request(action.request, row)}
        />
      );
    }

    case "dash.button":
      return wrap(
        <ButtonWidget
          label={str(props.label, "Run")}
          variant={typeof props.variant === "string" ? props.variant : undefined}
          disabled={props.disabled === true}
          description={typeof props.description === "string" ? props.description : undefined}
          onClick={() => request(str(props.request, "clicked"), isRecord(props.payload) ? props.payload : {})}
        />
      );

    case "dash.filter": {
      const stateKey = str(props.state_key, "filter");
      const fromState = dashboardState && Object.prototype.hasOwnProperty.call(dashboardState, stateKey);
      const control = FILTER_CONTROLS.has(String(props.control)) ? (props.control as FilterControl) : "select";
      return wrap(
        <FilterWidget
          label={str(props.label, stateKey)}
          control={control}
          options={props.options}
          value={fromState ? dashboardState![stateKey] : props.value}
          placeholder={typeof props.placeholder === "string" ? props.placeholder : undefined}
          allLabel={typeof props.all_label === "string" ? props.all_label : undefined}
          debounceMs={num(props.debounce_ms)}
          onChange={(val) => void onDashboardStateChange?.(stateKey, val)}
        />
      );
    }

    case "dash.form":
      return wrap(
        <FormWidget
          title={str(props.title, "Submit")}
          fields={props.fields}
          submitLabel={str(props.submit_label, "Submit")}
          variant={typeof props.variant === "string" ? props.variant : undefined}
          disabled={props.disabled === true}
          resetOnSubmit={props.reset_on_submit === true}
          staticPayload={isRecord(props.payload) ? props.payload : undefined}
          onSubmit={(values) => request(str(props.request, "submitted"), values)}
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
          height={customFrameHeight(node.layout_override, node.frame_height)}
          onWidgetRequest={onWidgetRequest}
        />
      );
  }
};

const FLEX_ALIGN: Record<string, React.CSSProperties["alignItems"]> = {
  start: "flex-start",
  center: "center",
  end: "flex-end",
  stretch: "stretch",
  baseline: "baseline",
};

const FLEX_JUSTIFY: Record<string, React.CSSProperties["justifyContent"]> = {
  start: "flex-start",
  center: "center",
  end: "flex-end",
  between: "space-between",
  around: "space-around",
  evenly: "space-evenly",
};

const FILTER_CONTROLS: ReadonlySet<string> = new Set(["select", "multiselect", "search", "toggle", "segmented"]);

function str(v: unknown, fallback: string): string {
  return typeof v === "string" ? v : v === undefined || v === null ? fallback : String(v);
}

function num(v: unknown): number | undefined {
  return typeof v === "number" && Number.isFinite(v) ? v : undefined;
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === "object" && !Array.isArray(v);
}

/**
 * Keeps one widget's render failure (e.g. data the renderer cannot handle) from blanking the
 * whole Dashboard; the next render of that node retries.
 */
class WidgetErrorBoundary extends React.Component<
  { node: WidgetNode; layoutStyle?: React.CSSProperties; children: React.ReactNode },
  { error: Error | null }
> {
  state: { error: Error | null } = { error: null };

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  componentDidCatch(error: Error) {
    const { widget_id, widget_type } = this.props.node;
    console.error(`widget '${widget_id}' (${widget_type}) failed to render`, error);
  }

  componentDidUpdate(prev: { node: WidgetNode }) {
    if (this.state.error && prev.node !== this.props.node) this.setState({ error: null });
  }

  render() {
    if (this.state.error) {
      const { widget_id, widget_type } = this.props.node;
      return (
        <div data-widget-id={widget_id} style={this.props.layoutStyle}>
          <div className="card dash-text callout danger" role="alert" style={{ marginBottom: 0 }}>
            <h4 style={{ margin: "0 0 6px" }}>Widget failed to render</h4>
            <p style={{ margin: 0 }}>
              {widget_id} ({widget_type}): {this.state.error.message}
            </p>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

// ---- Tabs (WAI-ARIA tabs pattern: roving tabindex, arrow/Home/End keys, persisted choice) ----

const TabsWidget: React.FC<{
  label: string;
  tabs: Array<{ id: string; key?: string; label: string; node: WidgetNode }>;
  stateKey: string;
  defaultTab?: unknown;
  common: Omit<RendererProps, "node" | "layoutStyle" | "inGrid">;
}> = ({ label, tabs, stateKey, defaultTab, common }) => {
  const [stored, setStored] = useClientState<string | null>(stateKey, null);
  const tabRefs = useRef<Array<HTMLButtonElement | null>>([]);
  const baseId = useId();
  let selectedIdx = tabs.findIndex((t) => t.id === stored);
  if (selectedIdx < 0) {
    selectedIdx =
      typeof defaultTab === "number"
        ? Math.min(tabs.length - 1, Math.max(0, Math.floor(defaultTab)))
        : tabs.findIndex((t) => typeof defaultTab === "string" && (t.key === defaultTab || t.id === defaultTab));
    if (selectedIdx < 0) selectedIdx = 0;
  }
  const select = (i: number, focus = false) => {
    const t = tabs[i];
    if (!t) return;
    setStored(t.id);
    if (focus) tabRefs.current[i]?.focus();
  };
  const onKeyDown = (e: React.KeyboardEvent) => {
    const last = tabs.length - 1;
    let next: number | null = null;
    if (e.key === "ArrowRight") next = selectedIdx >= last ? 0 : selectedIdx + 1;
    else if (e.key === "ArrowLeft") next = selectedIdx <= 0 ? last : selectedIdx - 1;
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = last;
    if (next !== null) {
      e.preventDefault();
      select(next, true);
    }
  };
  return (
    <div className="widget-tabs card" style={{ marginBottom: 0 }}>
      <div role="tablist" aria-label={label} onKeyDown={onKeyDown}>
        {tabs.map((t, i) => (
          <button
            key={t.id}
            ref={(el) => {
              tabRefs.current[i] = el;
            }}
            id={`${baseId}-tab-${i}`}
            type="button"
            role="tab"
            aria-selected={i === selectedIdx}
            aria-controls={`${baseId}-panel-${i}`}
            tabIndex={i === selectedIdx ? 0 : -1}
            onClick={() => select(i)}
          >
            {t.label}
          </button>
        ))}
      </div>
      {tabs.map((t, i) => (
        <div
          key={t.id}
          id={`${baseId}-panel-${i}`}
          role="tabpanel"
          aria-labelledby={`${baseId}-tab-${i}`}
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
  /** Frame height in px (Dashboard override, then the Definition's `frame_height`). */
  height?: number;
  onWidgetRequest: (ctx: WidgetRequestContext) => Promise<void>;
}> = ({ defId, widgetId, adapterInstance, generation, props, assetUrl, height = DEFAULT_FRAME_HEIGHT_PX, onWidgetRequest }) => {
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
        height,
        border: "1px solid var(--border)",
        borderRadius: 6,
        background: "transparent",
      }}
    />
  );
};
