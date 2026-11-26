// Built-in `dash.map`: geographic markers on an equirectangular grid, fitted to the points,
// colored by status, sized by value, with tooltips and an optional per-marker Request.
//
// The host renders no basemap tiles (the shell's CSP allows no third-party network); street-level
// maps belong in a custom widget provider.
import React, { useId, useState } from "react";
import {
  formatValue,
  niceTicks,
  safeColor,
  statusTone,
  toNumber,
  toneColor,
  type FormatOptions,
} from "../../lib/widgetFormat";

export interface MapPoint {
  lat: number;
  lon: number;
  label: string;
  status?: string;
  color: string;
  value: number | null;
  raw: Record<string, unknown>;
}

/** Valid points from the untrusted `points` prop (`lat` + `lon`/`lng`; out-of-range dropped). */
export function normalizePoints(raw: unknown): { points: MapPoint[]; dropped: number } {
  if (!Array.isArray(raw)) return { points: [], dropped: 0 };
  const points: MapPoint[] = [];
  let dropped = 0;
  raw.forEach((p, idx) => {
    if (!p || typeof p !== "object") {
      dropped += 1;
      return;
    }
    const r = p as Record<string, unknown>;
    const lat = toNumber(r.lat ?? r.latitude);
    const lon = toNumber(r.lon ?? r.lng ?? r.longitude);
    if (lat === null || lon === null || Math.abs(lat) > 90 || Math.abs(lon) > 180) {
      dropped += 1;
      return;
    }
    const status = typeof r.status === "string" ? r.status : undefined;
    points.push({
      lat,
      lon,
      label: String(r.label ?? r.name ?? r.id ?? `Point ${idx + 1}`),
      status,
      color: safeColor(r.color) ?? (status ? toneColor(statusTone(status)) : "var(--accent)"),
      value: toNumber(r.value),
      raw: r,
    });
  });
  return { points, dropped };
}

const W = 600;

export const MapWidget: React.FC<{
  title: string;
  points: MapPoint[];
  dropped?: number;
  fit?: boolean;
  height?: number;
  fmt?: FormatOptions;
  pointAction?: { label: string; request: string };
  onPointAction?: (point: Record<string, unknown>) => Promise<void>;
}> = ({ title, points, dropped = 0, fit = true, height = 240, fmt = {}, pointAction, onPointAction }) => {
  const titleId = useId();
  const [busy, setBusy] = useState<number | null>(null);
  const H = Math.min(800, Math.max(120, height));
  const pad = 14;

  // Bounds: the points' extent plus a margin (at least 4°), else the whole world.
  let lonMin = -180;
  let lonMax = 180;
  let latMin = -90;
  let latMax = 90;
  if (fit && points.length > 0) {
    lonMin = Math.min(...points.map((p) => p.lon));
    lonMax = Math.max(...points.map((p) => p.lon));
    latMin = Math.min(...points.map((p) => p.lat));
    latMax = Math.max(...points.map((p) => p.lat));
    const lonPad = Math.max(2, (lonMax - lonMin) * 0.15);
    const latPad = Math.max(2, (latMax - latMin) * 0.15);
    lonMin = Math.max(-180, lonMin - lonPad);
    lonMax = Math.min(180, lonMax + lonPad);
    latMin = Math.max(-90, latMin - latPad);
    latMax = Math.min(90, latMax + latPad);
  }
  const plotW = W - 2 * pad;
  const plotH = H - 2 * pad;
  const scale = Math.min(plotW / (lonMax - lonMin), plotH / (latMax - latMin));
  const offX = pad + (plotW - (lonMax - lonMin) * scale) / 2;
  const offY = pad + (plotH - (latMax - latMin) * scale) / 2;
  const px = (lon: number) => offX + (lon - lonMin) * scale;
  const py = (lat: number) => offY + (latMax - lat) * scale;

  const values = points.map((p) => p.value).filter((v): v is number => v !== null && v >= 0);
  const vMax = values.length ? Math.max(...values) : 0;
  const radius = (v: number | null) => (v === null || vMax <= 0 ? 5 : 4 + 10 * Math.sqrt(Math.max(0, v) / vMax));

  const lonTicks = niceTicks(lonMin, lonMax, 6).filter((t) => t > lonMin && t < lonMax);
  const latTicks = niceTicks(latMin, latMax, 5).filter((t) => t > latMin && t < latMax);

  const statusCounts = new Map<string, { n: number; color: string }>();
  for (const p of points) {
    if (!p.status) continue;
    const e = statusCounts.get(p.status) ?? { n: 0, color: p.color };
    e.n += 1;
    statusCounts.set(p.status, e);
  }

  const describe = (p: MapPoint) =>
    `${p.label}${p.status ? ` (${p.status})` : ""}${p.value !== null ? `: ${formatValue(p.value, fmt)}` : ""} — ${p.lat.toFixed(3)}, ${p.lon.toFixed(3)}`;

  const activate = async (idx: number) => {
    if (!onPointAction || busy !== null) return;
    setBusy(idx);
    try {
      await onPointAction(points[idx].raw);
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="card dash-map" style={{ marginBottom: 0 }}>
      <div className="row" style={{ justifyContent: "space-between", marginBottom: 6 }}>
        <strong id={titleId}>{title}</strong>
        <div className="row" style={{ gap: 6 }}>
          {[...statusCounts.entries()].map(([s, e]) => (
            <span key={s} className={`badge`} style={{ color: e.color, borderColor: e.color }}>
              {s}: {e.n}
            </span>
          ))}
          <span className="badge">{points.length} locations</span>
          {dropped > 0 && (
            <span className="badge warn" title="Points without valid lat/lon were not drawn">
              {dropped} invalid
            </span>
          )}
        </div>
      </div>
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" role="group" aria-labelledby={titleId} style={{ display: "block" }}>
        <rect x="0" y="0" width={W} height={H} fill="var(--surface-elevated)" rx="6" />
        <rect
          x={px(lonMin)}
          y={py(latMax)}
          width={(lonMax - lonMin) * scale}
          height={(latMax - latMin) * scale}
          fill="none"
          stroke="var(--border)"
        />
        {lonTicks.map((t) => (
          <g key={`lon${t}`}>
            <line x1={px(t)} y1={py(latMax)} x2={px(t)} y2={py(latMin)} stroke="var(--border)" strokeDasharray="2 4" />
            <text x={px(t)} y={py(latMin) - 3} textAnchor="middle" fontSize="9" fill="var(--text-muted)">
              {`${Math.abs(t)}°${t < 0 ? "W" : t > 0 ? "E" : ""}`}
            </text>
          </g>
        ))}
        {latTicks.map((t) => (
          <g key={`lat${t}`}>
            <line x1={px(lonMin)} y1={py(t)} x2={px(lonMax)} y2={py(t)} stroke="var(--border)" strokeDasharray="2 4" />
            <text x={px(lonMin) + 3} y={py(t) - 3} fontSize="9" fill="var(--text-muted)">
              {`${Math.abs(t)}°${t < 0 ? "S" : t > 0 ? "N" : ""}`}
            </text>
          </g>
        ))}
        {points.length === 0 && (
          <text x={W / 2} y={H / 2} textAnchor="middle" fontSize="12" fill="var(--text-muted)">
            No locations
          </text>
        )}
        {points.map((p, idx) => {
          const interactive = Boolean(pointAction && onPointAction);
          const label = describe(p);
          return (
            <g
              key={idx}
              className={interactive ? "map-marker interactive" : "map-marker"}
              {...(interactive
                ? {
                    role: "button",
                    tabIndex: 0,
                    "aria-label": `${pointAction!.label}: ${label}`,
                    "aria-disabled": busy !== null ? true : undefined,
                    onClick: () => void activate(idx),
                    onKeyDown: (e: React.KeyboardEvent) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        void activate(idx);
                      }
                    },
                  }
                : { role: "img", "aria-label": label })}
            >
              <circle
                cx={px(p.lon)}
                cy={py(p.lat)}
                r={radius(p.value)}
                fill={p.color}
                fillOpacity={0.8}
                stroke="var(--surface)"
                strokeWidth="1.5"
                opacity={busy === idx ? 0.4 : 1}
              >
                <title>{label}</title>
              </circle>
            </g>
          );
        })}
      </svg>
    </div>
  );
};
