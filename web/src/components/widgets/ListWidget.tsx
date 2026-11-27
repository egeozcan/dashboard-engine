// Built-in `dash.list`: a titled list of linked items (activity feeds, open PRs, stale repos).
// Each row is one link: title, subtitle (optionally followed by a timestamp), a marker, and on
// the right a meta word and/or a live relative time ("14h ago").
import React, { useEffect, useState } from "react";
import { parseTime, safeHref, statusTone, toneColor } from "../../lib/widgetFormat";

export type ListMarker = "none" | "dot" | "pull_request";

export interface ListItem {
  title: string;
  url?: string;
  subtitle?: string;
  subtitleTime?: number;
  marker: ListMarker;
  color?: string;
  meta?: string;
  time?: number;
}

/** Items from an untrusted `items` prop; entries without a title are dropped. */
export function normalizeListItems(raw: unknown): ListItem[] {
  if (!Array.isArray(raw)) return [];
  const out: ListItem[] = [];
  for (const it of raw) {
    if (!it || typeof it !== "object") continue;
    const r = it as Record<string, unknown>;
    if (typeof r.title !== "string" || !r.title) continue;
    const marker = r.marker === "dot" || r.marker === "pull_request" ? r.marker : "none";
    out.push({
      title: r.title,
      url: safeHref(r.url),
      subtitle: typeof r.subtitle === "string" && r.subtitle ? r.subtitle : undefined,
      subtitleTime: parseTime(r.subtitle_time) ?? undefined,
      marker,
      color: typeof r.tone === "string" ? toneColor(statusTone(r.tone)) : undefined,
      meta: typeof r.meta === "string" && r.meta ? r.meta : undefined,
      time: parseTime(r.time) ?? undefined,
    });
  }
  return out;
}

const STEPS: Array<[string, number]> = [
  ["y", 365.25 * 86400],
  ["mo", 30.44 * 86400],
  ["d", 86400],
  ["h", 3600],
  ["m", 60],
];

/** Compact relative time: "just now", "5m ago", "14h ago", "8mo ago", "14y ago", "in 3d". */
export function shortRelative(ms: number, now: number): string {
  const diff = (now - ms) / 1000;
  const abs = Math.abs(diff);
  if (abs < 45) return "just now";
  for (const [unit, size] of STEPS) {
    if (abs >= size) {
      const n = Math.floor(abs / size);
      return diff >= 0 ? `${n}${unit} ago` : `in ${n}${unit}`;
    }
  }
  return diff >= 0 ? "1m ago" : "in 1m";
}

const fullDate = (ms: number) => new Date(ms).toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
const dateTime = (ms: number) =>
  new Date(ms).toLocaleString(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });

/** Re-render every minute so relative times stay current between renders. */
function useMinuteClock(): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), 60_000);
    return () => window.clearInterval(id);
  }, []);
  return now;
}

const PullRequestIcon = () => (
  <svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
    <circle cx="4" cy="3.5" r="1.75" />
    <circle cx="4" cy="12.5" r="1.75" />
    <circle cx="12" cy="12.5" r="1.75" />
    <path d="M4 5.25v5.5M12 10.75V7a2.5 2.5 0 0 0-2.5-2.5H7.5M9 3 7.5 4.5 9 6" />
  </svg>
);

export const ListWidget: React.FC<{
  title?: string;
  subtitle?: string;
  items: ListItem[];
  showDate?: boolean;
  emptyMessage?: string;
  footer?: string;
}> = ({ title, subtitle, items, showDate, emptyMessage = "Nothing to show", footer }) => {
  const now = useMinuteClock();
  return (
    <div className="card dash-list" style={{ marginBottom: 0 }}>
      {(title || subtitle) && (
        <div className="dash-list-header">
          <span className="dash-list-mark" aria-hidden="true" />
          <div>
            {title && <div className="dash-list-title">{title}</div>}
            {subtitle && <div className="dash-list-subtitle">{subtitle}</div>}
          </div>
        </div>
      )}
      {items.length === 0 ? (
        <p className="dash-list-empty">{emptyMessage}</p>
      ) : (
        <ul aria-label={title}>
          {items.map((it, i) => {
            const right1 = it.meta ?? (it.time !== undefined ? shortRelative(it.time, now) : undefined);
            const right2 = it.meta
              ? it.time !== undefined
                ? shortRelative(it.time, now)
                : undefined
              : showDate && it.time !== undefined
                ? fullDate(it.time)
                : undefined;
            const body = (
              <>
                {it.marker !== "none" && (
                  <span className={`dash-list-marker ${it.marker}`} aria-hidden="true">
                    {it.marker === "pull_request" ? <PullRequestIcon /> : <span className="dot" />}
                  </span>
                )}
                <span className="dash-list-main">
                  <span className="dash-list-item-title">{it.title}</span>
                  {(it.subtitle || it.subtitleTime !== undefined) && (
                    <span className="dash-list-item-subtitle">
                      {[it.subtitle, it.subtitleTime !== undefined ? dateTime(it.subtitleTime) : undefined].filter(Boolean).join(" ")}
                    </span>
                  )}
                </span>
                {(right1 || right2) && (
                  <span className="dash-list-side">
                    {right1 && (
                      <span
                        className={`dash-list-meta${it.meta || showDate ? " strong" : ""}`}
                        style={it.color ? { color: it.color } : undefined}
                        title={it.time !== undefined ? new Date(it.time).toLocaleString() : undefined}
                      >
                        {right1}
                      </span>
                    )}
                    {right2 && <span className="dash-list-meta-detail">{right2}</span>}
                  </span>
                )}
                {it.url && (
                  <span className="dash-list-link" aria-hidden="true">
                    ↗
                  </span>
                )}
              </>
            );
            return (
              <li key={`${i}-${it.title}`}>
                {it.url ? (
                  <a className="dash-list-row" href={it.url} target="_blank" rel="noopener noreferrer">
                    {body}
                  </a>
                ) : (
                  <div className="dash-list-row">{body}</div>
                )}
              </li>
            );
          })}
        </ul>
      )}
      {footer && (
        <div className="dash-list-footer">
          <span className="dot" aria-hidden="true" />
          {footer}
        </div>
      )}
    </div>
  );
};
