// Built-in display widgets: `dash.counter` (KPI with status, delta, thresholds and sparkline)
// and `dash.text` (plain or safe-Markdown text with callout variants).
import React from "react";
import { Markdown } from "../../lib/markdown";
import {
  formatValue,
  statusTone,
  toNumber,
  toneClass,
  type FormatOptions,
  type Tone,
} from "../../lib/widgetFormat";

export interface CounterThresholds {
  warning?: number;
  critical?: number;
  /** `above` (default): high values are bad; `below`: low values are bad. */
  direction: "above" | "below";
}

export function normalizeCounterThresholds(raw: unknown): CounterThresholds | undefined {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return undefined;
  const r = raw as Record<string, unknown>;
  const warning = toNumber(r.warning) ?? undefined;
  const critical = toNumber(r.critical) ?? undefined;
  if (warning === undefined && critical === undefined) return undefined;
  return { warning, critical, direction: r.direction === "below" ? "below" : "above" };
}

/** Status of a counter: the Adapter's explicit `status`, else derived from `thresholds`. */
export function counterStatus(value: unknown, status: unknown, thresholds?: CounterThresholds): string | undefined {
  if (typeof status === "string" && status.trim()) return status;
  const v = toNumber(value);
  if (!thresholds || v === null) return undefined;
  const bad = (limit?: number) =>
    limit !== undefined && (thresholds.direction === "below" ? v <= limit : v >= limit);
  if (bad(thresholds.critical)) return "critical";
  if (bad(thresholds.warning)) return "warning";
  return "healthy";
}

export interface CounterDelta {
  text: string;
  tone: Tone;
  direction: "up" | "down" | "flat";
}

/** Change vs `previous` (percent by default) or the Adapter-supplied `delta`. */
export function counterDelta(opts: {
  value: unknown;
  previous?: unknown;
  delta?: unknown;
  deltaFormat?: string;
  higherIsBetter?: boolean;
  fmt: FormatOptions;
}): CounterDelta | null {
  const v = toNumber(opts.value);
  const prev = toNumber(opts.previous);
  let change = toNumber(opts.delta);
  let format = opts.deltaFormat;
  if (change === null && v !== null && prev !== null) {
    if ((format ?? "percent") === "percent") {
      if (prev === 0) {
        change = v - prev;
        format = "absolute";
      } else {
        change = (v - prev) / Math.abs(prev);
        format = "percent";
      }
    } else {
      change = v - prev;
    }
  }
  if (change === null) return null;
  const direction = change > 0 ? "up" : change < 0 ? "down" : "flat";
  const better = opts.higherIsBetter !== false ? change > 0 : change < 0;
  const tone: Tone = direction === "flat" ? "neutral" : better ? "ok" : "danger";
  const magnitude =
    format === "percent"
      ? formatValue(Math.abs(change), { format: "percent", decimals: 1 })
      : formatValue(Math.abs(change), { ...opts.fmt, prefix: undefined });
  const arrow = direction === "up" ? "▲" : direction === "down" ? "▼" : "■";
  return { text: `${arrow} ${magnitude}`, tone, direction };
}

export const Sparkline: React.FC<{ values: unknown[]; label: string; color?: string }> = ({
  values,
  label,
  color = "var(--accent)",
}) => {
  const nums = values.map(toNumber).filter((v): v is number => v !== null);
  if (nums.length < 2) return null;
  const w = 120;
  const h = 32;
  const lo = Math.min(...nums);
  const hi = Math.max(...nums);
  const pts = nums
    .map((v, i) => {
      const x = (i / (nums.length - 1)) * (w - 4) + 2;
      const y = h - 2 - ((v - lo) / (hi - lo || 1)) * (h - 4);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  return (
    <svg viewBox={`0 0 ${w} ${h}`} width={w} height={h} role="img" aria-label={label} className="sparkline">
      <polyline points={pts} fill="none" stroke={color} strokeWidth="2" strokeLinejoin="round" />
    </svg>
  );
};

export const CounterWidget: React.FC<{
  label: string;
  value: unknown;
  fmt: FormatOptions;
  status?: string;
  delta: CounterDelta | null;
  deltaLabel?: string;
  description?: string;
  sparkline?: unknown[];
}> = ({ label, value, fmt, status, delta, deltaLabel, description, sparkline }) => {
  const tone = statusTone(status);
  return (
    <div className={`card dash-counter tone-${tone}`} style={{ marginBottom: 0 }}>
      <div className="row" style={{ justifyContent: "space-between" }}>
        <div style={{ fontSize: 12, color: "var(--text-muted)" }}>{label}</div>
        {status && <span className={`badge ${toneClass(tone)}`}>{status}</span>}
      </div>
      <div className="row" style={{ justifyContent: "space-between", marginTop: 6, alignItems: "flex-end" }}>
        <div className="col" style={{ gap: 2 }}>
          <strong style={{ fontSize: 28, lineHeight: 1.1 }}>{formatValue(value ?? 0, fmt)}</strong>
          {delta && (
            <span className={`counter-delta ${toneClass(delta.tone)}`}>
              {delta.text}
              {deltaLabel ? <span style={{ color: "var(--text-muted)" }}> {deltaLabel}</span> : null}
            </span>
          )}
        </div>
        {sparkline && <Sparkline values={sparkline} label={`${label} trend`} />}
      </div>
      {description && (
        <div style={{ fontSize: 12, color: "var(--text-muted)", marginTop: 6 }}>{description}</div>
      )}
    </div>
  );
};

const VARIANTS = new Set(["info", "success", "warning", "danger"]);

export const TextWidget: React.FC<{
  title?: string;
  text: string;
  markdown?: boolean;
  variant?: string;
  renderError?: boolean;
}> = ({ title, text, markdown, variant, renderError }) => {
  const v = renderError ? "danger" : variant && VARIANTS.has(variant) ? variant : undefined;
  return (
    <div
      className={`card dash-text${v ? ` callout ${v}` : ""}`}
      style={{ marginBottom: 0 }}
      role={renderError || v === "danger" ? "alert" : undefined}
    >
      {Boolean(title) && <h4 style={{ margin: "0 0 6px" }}>{title}</h4>}
      {markdown ? <Markdown text={text} /> : <p style={{ margin: 0, whiteSpace: "pre-line" }}>{text}</p>}
    </div>
  );
};
