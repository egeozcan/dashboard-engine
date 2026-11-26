// Built-in `dash.chart`: line, area and bar charts over one or more series of rows, with a time,
// numeric or categorical x axis, nice y ticks, thresholds, a legend and a hover/keyboard tooltip.
import React, { useId, useMemo, useRef, useState } from "react";
import {
  SERIES_PALETTE,
  formatAxisTime,
  formatValue,
  niceTicks,
  parseTime,
  safeColor,
  statusTone,
  toNumber,
  toneColor,
  type FormatOptions,
} from "../../lib/widgetFormat";

export type ChartKind = "line" | "area" | "bar";

export interface ChartSeriesSpec {
  key: string;
  label: string;
  color: string;
}

export interface ChartThreshold {
  value: number;
  label?: string;
  color: string;
}

/** Series to plot: `y_keys` (strings or `{ key, label, color }`), else the single `y_key`. */
export function normalizeSeriesSpecs(yKeys: unknown, yKey: string, label?: string): ChartSeriesSpec[] {
  const raw = Array.isArray(yKeys) && yKeys.length > 0 ? yKeys : [{ key: yKey, label: label ?? yKey }];
  const out: ChartSeriesSpec[] = [];
  raw.forEach((s) => {
    const rec = typeof s === "string" ? { key: s } : s && typeof s === "object" ? (s as Record<string, unknown>) : null;
    if (!rec || typeof rec.key !== "string" || !rec.key) return;
    out.push({
      key: rec.key,
      label: typeof rec.label === "string" ? rec.label : rec.key,
      color: safeColor(rec.color) ?? SERIES_PALETTE[out.length % SERIES_PALETTE.length],
    });
  });
  return out;
}

export function normalizeThresholds(raw: unknown): ChartThreshold[] {
  if (!Array.isArray(raw)) return [];
  const out: ChartThreshold[] = [];
  for (const t of raw) {
    if (!t || typeof t !== "object") continue;
    const r = t as Record<string, unknown>;
    const value = toNumber(r.value);
    if (value === null) continue;
    out.push({
      value,
      label: typeof r.label === "string" ? r.label : undefined,
      color: safeColor(r.color) ?? toneColor(statusTone(r.status ?? "warning")),
    });
  }
  return out;
}

type XMode = "time" | "number" | "category";

interface Prepared {
  mode: XMode;
  xs: unknown[];
  xNum: number[];
  values: Array<Array<number | null>>;
}

/** Sort rows along a time/numeric x axis and extract finite y values (non-finite → gap). */
export function prepareChartData(
  rows: Array<Record<string, unknown>>,
  xKey: string,
  series: ChartSeriesSpec[],
  xFormat?: string
): Prepared {
  const raw = rows.map((r, i) => (r[xKey] === undefined ? i : r[xKey]));
  const allTimes = raw.length > 0 && raw.every((x) => parseTime(x) !== null);
  const allNumbers = raw.length > 0 && raw.every((x) => typeof x === "number" && Number.isFinite(x));
  let mode: XMode = "category";
  if (xFormat === "time") mode = allTimes ? "time" : "category";
  else if (xFormat === "number") mode = allNumbers ? "number" : "category";
  else if (xFormat !== "category") {
    // Auto: ISO timestamp strings form a time axis, plain numbers a linear one (epoch numbers
    // need `x_format = "time"`).
    if (allTimes && raw.every((x) => typeof x === "string")) mode = "time";
    else if (allNumbers) mode = "number";
  }
  let order = rows.map((_, i) => i);
  const xNumOf = (x: unknown, i: number) =>
    mode === "time" ? (parseTime(x) as number) : mode === "number" ? (x as number) : i;
  if (mode !== "category") order = order.sort((a, b) => xNumOf(raw[a], a) - xNumOf(raw[b], b));
  return {
    mode,
    xs: order.map((i) => raw[i]),
    xNum: order.map((i, pos) => (mode === "category" ? pos : xNumOf(raw[i], i))),
    values: series.map((s) => order.map((i) => toNumber(rows[i][s.key]))),
  };
}

function xLabel(mode: XMode, x: unknown, spanMs: number): string {
  if (mode === "time") {
    const t = parseTime(x);
    return t === null ? String(x) : formatAxisTime(t, spanMs > 3 * 86400_000);
  }
  const s = String(x ?? "");
  return s.length > 14 ? `${s.slice(0, 13)}…` : s;
}

const W = 600;

export const ChartWidget: React.FC<{
  title: string;
  rows: Array<Record<string, unknown>>;
  xKey: string;
  series: ChartSeriesSpec[];
  kind?: ChartKind;
  height?: number;
  yMin?: number;
  yMax?: number;
  zeroBaseline?: boolean;
  fmt?: FormatOptions;
  xFormat?: string;
  thresholds?: ChartThreshold[];
  showLegend?: boolean;
  emptyMessage?: string;
}> = ({
  title,
  rows,
  xKey,
  series,
  kind = "line",
  height = 160,
  yMin,
  yMax,
  zeroBaseline = true,
  fmt = {},
  xFormat,
  thresholds = [],
  showLegend,
  emptyMessage = "No data",
}) => {
  const titleId = useId();
  const svgRef = useRef<SVGSVGElement>(null);
  const [active, setActive] = useState<number | null>(null);
  const H = Math.min(600, Math.max(80, height));
  const seriesKey = series.map((s) => s.key).join("\u0000");
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const data = useMemo(() => prepareChartData(rows, xKey, series, xFormat), [rows, xKey, seriesKey, xFormat]);
  const n = data.xs.length;

  const finite = data.values.flat().filter((v): v is number => v !== null);
  for (const t of thresholds) finite.push(t.value);
  let lo = finite.length ? Math.min(...finite) : 0;
  let hi = finite.length ? Math.max(...finite) : 1;
  if (zeroBaseline || kind !== "line") {
    lo = Math.min(lo, 0);
    hi = Math.max(hi, 0);
  }
  if (yMin !== undefined) lo = yMin;
  if (yMax !== undefined) hi = yMax;
  const ticks = niceTicks(lo, hi, 4);
  const dLo = yMin ?? ticks[0];
  const dHi = yMax ?? ticks[ticks.length - 1];
  const tickLabels = ticks.filter((t) => t >= dLo - 1e-9 && t <= dHi + 1e-9).map((t) => ({
    t,
    label: formatValue(t, { ...fmt, format: fmt.format === "number" || !fmt.format ? "compact" : fmt.format, unit: undefined, prefix: undefined }),
  }));

  const padLeft = Math.max(28, 10 + 6.5 * Math.max(1, ...tickLabels.map((t) => t.label.length)));
  const padRight = 14;
  const padTop = 10;
  const padBottom = 22;
  const plotW = W - padLeft - padRight;
  const plotH = H - padTop - padBottom;
  const yScale = (v: number) => padTop + plotH - ((v - dLo) / (dHi - dLo || 1)) * plotH;

  const banded = kind === "bar" || data.mode === "category";
  const xMin = n ? data.xNum[0] : 0;
  const xMax = n ? data.xNum[n - 1] : 1;
  const xPos = (i: number) => {
    if (banded) return padLeft + ((i + 0.5) * plotW) / Math.max(1, n);
    if (n <= 1 || xMax === xMin) return padLeft + plotW / 2;
    return padLeft + ((data.xNum[i] - xMin) / (xMax - xMin)) * plotW;
  };
  const spanMs = data.mode === "time" ? xMax - xMin : 0;

  // At most ~6 x labels, evenly spread over the points.
  const labelIdx: number[] = [];
  if (n === 1) labelIdx.push(0);
  else if (n > 1) {
    const want = Math.min(n, 6);
    for (let k = 0; k < want; k++) labelIdx.push(Math.round((k * (n - 1)) / (want - 1)));
  }

  const clampY = (v: number) => Math.min(padTop + plotH, Math.max(padTop, yScale(v)));
  const baseY = clampY(Math.max(dLo, Math.min(dHi, 0)));

  const linePath = (vals: Array<number | null>) => {
    let d = "";
    let pen = false;
    vals.forEach((v, i) => {
      if (v === null) {
        pen = false;
        return;
      }
      d += `${pen ? "L" : "M"}${xPos(i).toFixed(1)},${clampY(v).toFixed(1)}`;
      pen = true;
    });
    return d;
  };
  const areaPath = (vals: Array<number | null>) => {
    const segs: string[] = [];
    let seg: Array<[number, number]> = [];
    const close = () => {
      if (seg.length) {
        segs.push(
          `M${seg[0][0].toFixed(1)},${baseY.toFixed(1)}` +
            seg.map(([x, y]) => `L${x.toFixed(1)},${y.toFixed(1)}`).join("") +
            `L${seg[seg.length - 1][0].toFixed(1)},${baseY.toFixed(1)}Z`
        );
      }
      seg = [];
    };
    vals.forEach((v, i) => (v === null ? close() : seg.push([xPos(i), clampY(v)])));
    close();
    return segs.join("");
  };

  const pointerToIndex = (clientX: number): number | null => {
    const svg = svgRef.current;
    if (!svg || n === 0) return null;
    const r = svg.getBoundingClientRect();
    if (r.width <= 0) return null;
    const vx = ((clientX - r.left) / r.width) * W;
    let best = 0;
    let bestD = Infinity;
    for (let i = 0; i < n; i++) {
      const d = Math.abs(xPos(i) - vx);
      if (d < bestD) {
        bestD = d;
        best = i;
      }
    }
    return best;
  };

  const legend = showLegend ?? series.length > 1;
  const last = n ? data.values[0]?.[n - 1] : null;
  const summary =
    n === 0
      ? `${title}: no data`
      : `${title}: ${n} points` +
        series
          .map((s, si) => {
            const vals = data.values[si].filter((v): v is number => v !== null);
            if (!vals.length) return "";
            return `; ${s.label} latest ${formatValue(data.values[si][n - 1], fmt)}, min ${formatValue(Math.min(...vals), fmt)}, max ${formatValue(Math.max(...vals), fmt)}`;
          })
          .join("");

  const barGroup = (plotW / Math.max(1, n)) * 0.8;
  const barW = Math.max(1, barGroup / Math.max(1, series.length));
  const showDots = kind !== "bar" && n <= 60;

  return (
    <div className="card dash-chart" style={{ marginBottom: 0, position: "relative" }}>
      <div className="row" style={{ justifyContent: "space-between", marginBottom: 6 }}>
        <strong id={titleId}>{title}</strong>
        <div className="row" style={{ gap: 6 }}>
          {last !== null && last !== undefined && series.length === 1 && (
            <span className="badge" title="Latest value">
              {formatValue(last, fmt)}
            </span>
          )}
          <span className="badge">{n} samples</span>
        </div>
      </div>
      {legend && (
        <ul className="chart-legend" aria-label="Series">
          {series.map((s) => (
            <li key={s.key}>
              <span className="swatch" style={{ background: s.color }} />
              {s.label}
            </li>
          ))}
        </ul>
      )}
      <svg
        ref={svgRef}
        viewBox={`0 0 ${W} ${H}`}
        width="100%"
        role="img"
        aria-labelledby={titleId}
        aria-describedby={`${titleId}-summary`}
        tabIndex={n > 0 ? 0 : undefined}
        style={{ display: "block", overflow: "visible", touchAction: "pan-y" }}
        onPointerMove={(e) => setActive(pointerToIndex(e.clientX))}
        onPointerLeave={() => setActive(null)}
        onBlur={() => setActive(null)}
        onKeyDown={(e) => {
          if (n === 0) return;
          if (e.key === "ArrowRight" || e.key === "ArrowLeft") {
            e.preventDefault();
            const cur = active ?? (e.key === "ArrowRight" ? -1 : n);
            setActive(Math.min(n - 1, Math.max(0, cur + (e.key === "ArrowRight" ? 1 : -1))));
          } else if (e.key === "Home") setActive(0);
          else if (e.key === "End") setActive(n - 1);
          else if (e.key === "Escape") setActive(null);
        }}
      >
        <desc id={`${titleId}-summary`}>{summary}</desc>
        {tickLabels.map(({ t, label }) => {
          const y = yScale(t);
          return (
            <g key={t}>
              <line
                x1={padLeft}
                y1={y}
                x2={W - padRight}
                y2={y}
                stroke="var(--border)"
                strokeDasharray={t === 0 ? undefined : "3 3"}
              />
              <text x={padLeft - 6} y={y + 3} textAnchor="end" fontSize="10" fill="var(--text-muted)">
                {label}
              </text>
            </g>
          );
        })}
        {n === 0 && (
          <text x={padLeft + plotW / 2} y={padTop + plotH / 2} textAnchor="middle" fontSize="12" fill="var(--text-muted)">
            {emptyMessage}
          </text>
        )}
        {kind === "bar" &&
          series.map((s, si) =>
            data.values[si].map((v, i) => {
              if (v === null) return null;
              const x = xPos(i) - barGroup / 2 + si * barW;
              const y = clampY(v);
              return (
                <rect
                  key={`${s.key}-${i}`}
                  x={x}
                  y={Math.min(y, baseY)}
                  width={Math.max(1, barW - 1)}
                  height={Math.max(0.5, Math.abs(baseY - y))}
                  fill={s.color}
                  opacity={active === null || active === i ? 1 : 0.55}
                >
                  <title>{`${xLabel(data.mode, data.xs[i], spanMs)} · ${s.label}: ${formatValue(v, fmt)}`}</title>
                </rect>
              );
            })
          )}
        {kind !== "bar" &&
          series.map((s, si) => (
            <g key={s.key}>
              {kind === "area" && <path d={areaPath(data.values[si])} fill={s.color} opacity={0.15} stroke="none" />}
              {n > 1 && <path d={linePath(data.values[si])} fill="none" stroke={s.color} strokeWidth="2.5" strokeLinejoin="round" />}
              {(showDots || n === 1) &&
                data.values[si].map((v, i) =>
                  v === null ? null : (
                    <circle key={i} cx={xPos(i)} cy={clampY(v)} r={active === i ? 5 : 3.5} fill={s.color}>
                      <title>{`${xLabel(data.mode, data.xs[i], spanMs)}: ${formatValue(v, fmt)}`}</title>
                    </circle>
                  )
                )}
            </g>
          ))}
        {thresholds.map((t, i) => {
          if (t.value < dLo || t.value > dHi) return null;
          const y = yScale(t.value);
          return (
            <g key={`th-${i}`}>
              <line x1={padLeft} y1={y} x2={W - padRight} y2={y} stroke={t.color} strokeWidth="1.5" strokeDasharray="6 4" />
              {t.label && (
                <text x={W - padRight} y={y - 4} textAnchor="end" fontSize="10" fill={t.color}>
                  {t.label}
                </text>
              )}
            </g>
          );
        })}
        {active !== null && active < n && kind !== "bar" && (
          <line x1={xPos(active)} y1={padTop} x2={xPos(active)} y2={padTop + plotH} stroke="var(--text-muted)" strokeDasharray="2 3" />
        )}
        {labelIdx.map((i, k) => (
          <text
            key={`x-${i}-${k}`}
            x={xPos(i)}
            y={H - 5}
            textAnchor={n === 1 || banded ? "middle" : k === 0 ? "start" : k === labelIdx.length - 1 ? "end" : "middle"}
            fontSize="10"
            fill="var(--text-muted)"
          >
            {xLabel(data.mode, data.xs[i], spanMs)}
          </text>
        ))}
      </svg>
      {active !== null && active < n && (
        <div
          className="chart-tooltip"
          role="status"
          style={{
            left: `${(xPos(active) / W) * 100}%`,
            transform: xPos(active) > W * 0.6 ? "translateX(-100%)" : undefined,
          }}
        >
          <div style={{ fontWeight: 600 }}>{xLabel(data.mode, data.xs[active], spanMs)}</div>
          {series.map((s, si) => (
            <div key={s.key} className="row" style={{ gap: 6, flexWrap: "nowrap" }}>
              <span className="swatch" style={{ background: s.color }} />
              <span>{s.label}:</span>
              <strong>{formatValue(data.values[si][active], fmt)}</strong>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};
