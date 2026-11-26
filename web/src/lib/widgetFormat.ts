/**
 * Shared value formatting, status tones, axis ticks and URL/color sanitizing for the built-in
 * widget providers. Everything here is pure and deterministic given its inputs (formatting of
 * dates uses the browser's locale and time zone).
 */

export const EMPTY_CELL = "—";

/** Value formats understood by `dash.table` columns, `dash.counter`, `dash.chart` and `dash.map`. */
export type FormatName =
  | "text"
  | "number"
  | "integer"
  | "compact"
  | "percent"
  | "currency"
  | "bytes"
  | "duration"
  | "duration_ms"
  | "date"
  | "datetime"
  | "time"
  | "relative"
  | "boolean";

export interface FormatOptions {
  format?: string;
  /** Fraction digits (maximum; currency/percent use it as both minimum and maximum). */
  decimals?: number;
  /** ISO 4217 code for `currency` (default USD). */
  currency?: string;
  /** Suffix appended after the formatted value (`%` and `/x` attach without a space). */
  unit?: string;
  /** Prefix placed before the formatted value. */
  prefix?: string;
}

/** Read the formatting options a widget or column declares in its props. */
export function formatOptionsFrom(src: Record<string, unknown> | undefined | null): FormatOptions {
  if (!src) return {};
  const out: FormatOptions = {};
  if (typeof src.format === "string") out.format = src.format;
  if (typeof src.decimals === "number" && Number.isFinite(src.decimals)) {
    out.decimals = Math.min(10, Math.max(0, Math.round(src.decimals)));
  }
  if (typeof src.currency === "string") out.currency = src.currency;
  if (typeof src.unit === "string") out.unit = src.unit;
  if (typeof src.prefix === "string") out.prefix = src.prefix;
  return out;
}

/** Finite number from a number or numeric string; `null` otherwise. */
export function toNumber(v: unknown): number | null {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

/**
 * Epoch milliseconds of a timestamp: an RFC 3339/ISO string, or an epoch number (seconds when
 * below 1e11, else milliseconds). `null` when it is not a time.
 */
export function parseTime(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) {
    return Math.abs(v) < 1e11 ? v * 1000 : v;
  }
  if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}/.test(v.trim())) {
    const t = Date.parse(v.trim());
    return Number.isNaN(t) ? null : t;
  }
  return null;
}

function numberFormat(opts: Intl.NumberFormatOptions): Intl.NumberFormat {
  try {
    return new Intl.NumberFormat(undefined, opts);
  } catch {
    // Invalid currency code or option combination: plain numbers instead of a crash.
    return new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });
  }
}

function withAffixes(s: string, o: FormatOptions): string {
  const unit = o.unit ? (/^[%/°]/.test(o.unit) ? o.unit : ` ${o.unit}`) : "";
  return `${o.prefix ?? ""}${s}${unit}`;
}

const BYTE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];

export function formatBytes(n: number, decimals = 1): string {
  const sign = n < 0 ? "-" : "";
  let v = Math.abs(n);
  let i = 0;
  while (v >= 1024 && i < BYTE_UNITS.length - 1) {
    v /= 1024;
    i += 1;
  }
  const digits = i === 0 ? 0 : decimals;
  return `${sign}${numberFormat({ maximumFractionDigits: digits }).format(v)} ${BYTE_UNITS[i]}`;
}

/** Human duration from seconds: the two most significant units (`1d 4h`, `3m 20s`, `450 ms`). */
export function formatDuration(seconds: number): string {
  const sign = seconds < 0 ? "-" : "";
  const s = Math.abs(seconds);
  if (s < 1) return `${sign}${Math.round(s * 1000)} ms`;
  const units: Array<[string, number]> = [
    ["d", 86400],
    ["h", 3600],
    ["m", 60],
    ["s", 1],
  ];
  const parts: string[] = [];
  let rest = Math.round(s);
  for (const [label, size] of units) {
    const q = Math.floor(rest / size);
    if (q > 0 || parts.length > 0) {
      parts.push(`${q}${label}`);
      rest -= q * size;
    }
    if (parts.length === 2) break;
  }
  // `1h 0m` reads better as `1h`.
  if (parts.length === 2 && parts[1].startsWith("0")) parts.pop();
  return `${sign}${parts.join(" ") || "0s"}`;
}

const RELATIVE_STEPS: Array<[Intl.RelativeTimeFormatUnit, number]> = [
  ["year", 365 * 86400],
  ["month", 30 * 86400],
  ["week", 7 * 86400],
  ["day", 86400],
  ["hour", 3600],
  ["minute", 60],
  ["second", 1],
];

export function formatRelative(ms: number, now: number = Date.now()): string {
  const diffSec = (ms - now) / 1000;
  const abs = Math.abs(diffSec);
  const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  if (abs < 45) return rtf.format(0, "second");
  for (const [unit, size] of RELATIVE_STEPS) {
    if (abs >= size) return rtf.format(Math.round(diffSec / size), unit);
  }
  return rtf.format(Math.round(diffSec), "second");
}

function formatTimeValue(v: unknown, format: string, now: number): string | null {
  const t = parseTime(v);
  if (t === null) return null;
  const d = new Date(t);
  switch (format) {
    case "date":
      return d.toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" });
    case "time":
      return d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
    case "relative":
      return formatRelative(t, now);
    default:
      return d.toLocaleString(undefined, {
        year: "numeric",
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      });
  }
}

/**
 * Format a widget value for display. `null`/`undefined`/`""` render as an em dash; values that do
 * not fit the requested format (e.g. a non-numeric string under `number`) render unchanged.
 */
export function formatValue(v: unknown, o: FormatOptions = {}, now: number = Date.now()): string {
  if (v === null || v === undefined || v === "") return EMPTY_CELL;
  const format = o.format ?? "";
  if (format === "boolean" || typeof v === "boolean") {
    const truthy = v === true || v === "true" || v === 1 || v === "1";
    const falsy = v === false || v === "false" || v === 0 || v === "0";
    if (truthy || falsy) return truthy ? "Yes" : "No";
  }
  if (format === "date" || format === "datetime" || format === "time" || format === "relative") {
    return formatTimeValue(v, format, now) ?? String(v);
  }
  if (format === "text") return withAffixes(String(v), o);
  const n = toNumber(v);
  if (n === null) {
    if (typeof v === "object") {
      const s = JSON.stringify(v);
      return s.length > 80 ? `${s.slice(0, 77)}...` : s;
    }
    return withAffixes(String(v), o);
  }
  if (format === "" && typeof v === "string") return withAffixes(v, o);
  switch (format) {
    case "integer":
      return withAffixes(numberFormat({ maximumFractionDigits: 0 }).format(n), o);
    case "compact":
      return withAffixes(
        numberFormat({ notation: "compact", maximumFractionDigits: o.decimals ?? 1 }).format(n),
        o
      );
    case "percent": {
      const d = o.decimals ?? 1;
      return withAffixes(
        numberFormat({ style: "percent", minimumFractionDigits: 0, maximumFractionDigits: d }).format(n),
        o
      );
    }
    case "currency": {
      const d = o.decimals;
      return withAffixes(
        numberFormat({
          style: "currency",
          currency: (o.currency ?? "USD").toUpperCase(),
          ...(d !== undefined ? { minimumFractionDigits: d, maximumFractionDigits: d } : {}),
        }).format(n),
        o
      );
    }
    case "bytes":
      return withAffixes(formatBytes(n, o.decimals ?? 1), o);
    case "duration":
      return withAffixes(formatDuration(n), o);
    case "duration_ms":
      return withAffixes(formatDuration(n / 1000), o);
    default:
      return withAffixes(numberFormat({ maximumFractionDigits: o.decimals ?? 2 }).format(n), o);
  }
}

/** Visual tone of a status word used by counters, badges and map markers. */
export type Tone = "ok" | "warn" | "danger" | "info" | "neutral";

const TONES: Record<string, Tone> = {
  ok: "ok",
  healthy: "ok",
  success: "ok",
  succeeded: "ok",
  passing: "ok",
  up: "ok",
  online: "ok",
  active: "ok",
  green: "ok",
  warning: "warn",
  warn: "warn",
  degraded: "warn",
  pending: "warn",
  slow: "warn",
  yellow: "warn",
  amber: "warn",
  critical: "danger",
  error: "danger",
  danger: "danger",
  failed: "danger",
  failing: "danger",
  down: "danger",
  offline: "danger",
  red: "danger",
  info: "info",
  running: "info",
  blue: "info",
};

export function statusTone(status: unknown): Tone {
  if (typeof status !== "string") return "neutral";
  return TONES[status.trim().toLowerCase()] ?? "neutral";
}

/** CSS class suffix of the shared `.badge` style for a tone. */
export function toneClass(tone: Tone): string {
  return tone === "neutral" ? "" : tone;
}

/** CSS color of a tone (SVG fills/strokes). */
export function toneColor(tone: Tone): string {
  switch (tone) {
    case "ok":
      return "var(--ok)";
    case "warn":
      return "var(--warn)";
    case "danger":
      return "var(--danger)";
    case "info":
      return "var(--accent)";
    default:
      return "var(--text-muted)";
  }
}

/**
 * Adapter-supplied link targets: only `http(s)`, `mailto` and same-origin absolute paths are
 * rendered as links (`javascript:`, `data:` and protocol-relative URLs are dropped).
 */
export function safeHref(raw: unknown): string | undefined {
  if (typeof raw !== "string") return undefined;
  const s = raw.trim();
  if (s.startsWith("/") && !s.startsWith("//") && !s.startsWith("/\\")) return s;
  try {
    const u = new URL(s);
    if (u.protocol === "http:" || u.protocol === "https:" || u.protocol === "mailto:") return u.href;
  } catch {
    // not an absolute URL
  }
  return undefined;
}

const COLOR_RE =
  /^(#[0-9a-fA-F]{3,8}|[a-zA-Z]{3,20}|(?:rgb|rgba|hsl|hsla)\(\s*[\d.]+%?(?:\s*[,\s/]\s*[\d.]+%?){2,3}\s*\)|var\(--[\w-]+\))$/;

/** Adapter-supplied colors: hex, named, rgb()/hsl() or a CSS variable; anything else is dropped. */
export function safeColor(raw: unknown): string | undefined {
  return typeof raw === "string" && COLOR_RE.test(raw.trim()) ? raw.trim() : undefined;
}

export const SERIES_PALETTE = [
  "var(--accent)",
  "#8250df",
  "#bf8700",
  "#1a7f37",
  "#cf222e",
  "#0598bc",
  "#e16f24",
  "#6e7781",
];

/** Nearest 1/2/5 × 10^k step to `raw`. */
function niceStep(raw: number): number {
  const exp = Math.floor(Math.log10(raw));
  const f = raw / 10 ** exp;
  const nf = f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10;
  return nf * 10 ** exp;
}

/** "Nice" axis ticks (1/2/5 steps) covering `[min, max]`. */
export function niceTicks(min: number, max: number, count = 4): number[] {
  if (!Number.isFinite(min) || !Number.isFinite(max)) return [0, 1];
  if (min === max) {
    const pad = min === 0 ? 1 : Math.abs(min) * 0.1;
    min -= min === 0 ? 0 : pad;
    max += pad;
  }
  if (min > max) [min, max] = [max, min];
  const step = niceStep((max - min) / Math.max(1, count - 1));
  const lo = Math.floor(min / step) * step;
  const hi = Math.ceil(max / step) * step;
  const ticks: number[] = [];
  for (let v = lo; v <= hi + step / 2 && ticks.length < 50; v += step) {
    ticks.push(Number(v.toPrecision(12)));
  }
  return ticks;
}

function pad2(n: number): string {
  return String(n).padStart(2, "0");
}

/** Compact local-time axis label: `MM-DD HH:MM`, or `MM-DD` when `dateOnly`. */
export function formatAxisTime(ms: number, dateOnly = false): string {
  const d = new Date(ms);
  const day = `${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
  return dateOnly ? day : `${day} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
}

/** Normalize `options` props: strings, numbers, or `{ value, label }` records. */
export function normalizeOptions(raw: unknown): Array<{ value: string; label: string }> {
  if (!Array.isArray(raw)) return [];
  const out: Array<{ value: string; label: string }> = [];
  for (const o of raw) {
    if (typeof o === "string" || typeof o === "number" || typeof o === "boolean") {
      out.push({ value: String(o), label: String(o) });
    } else if (o && typeof o === "object" && "value" in o) {
      const rec = o as { value: unknown; label?: unknown };
      out.push({ value: String(rec.value), label: String(rec.label ?? rec.value) });
    }
  }
  return out;
}

/** Plain record rows from an untrusted `rows`/`series`/`points` prop. */
export function asRecords(raw: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(raw)) return [];
  return raw.filter((r): r is Record<string, unknown> => !!r && typeof r === "object" && !Array.isArray(r));
}
