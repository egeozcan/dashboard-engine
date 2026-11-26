/**
 * Field model of the built-in `dash.form` widget: normalizing Adapter-supplied field specs,
 * client-side validation and typed coercion of the submitted payload.
 *
 * Client-side validation is a convenience only: the Adapter's declared Request schema and the
 * action's argument schema remain authoritative on the server.
 */

export type FieldType =
  | "text"
  | "textarea"
  | "number"
  | "integer"
  | "boolean"
  | "select"
  | "date"
  | "datetime"
  | "email"
  | "url";

const FIELD_TYPES: ReadonlySet<string> = new Set([
  "text",
  "textarea",
  "number",
  "integer",
  "boolean",
  "select",
  "date",
  "datetime",
  "email",
  "url",
]);

export interface FieldOption {
  value: string;
  label: string;
  /** Original option value (sent in the payload, so numeric options stay numbers). */
  raw: unknown;
}

export interface FieldSpec {
  name: string;
  label: string;
  type: FieldType;
  required: boolean;
  placeholder?: string;
  help?: string;
  default?: unknown;
  options: FieldOption[];
  min?: number;
  max?: number;
  step?: number;
  pattern?: string;
  minLength?: number;
  maxLength?: number;
}

export type FieldValue = string | boolean;

function num(v: unknown): number | undefined {
  return typeof v === "number" && Number.isFinite(v) ? v : undefined;
}

function options(raw: unknown): FieldOption[] {
  if (!Array.isArray(raw)) return [];
  const out: FieldOption[] = [];
  for (const o of raw) {
    if (typeof o === "string" || typeof o === "number" || typeof o === "boolean") {
      out.push({ value: String(o), label: String(o), raw: o });
    } else if (o && typeof o === "object" && "value" in o) {
      const rec = o as { value: unknown; label?: unknown };
      out.push({ value: String(rec.value), label: String(rec.label ?? rec.value), raw: rec.value });
    }
  }
  return out;
}

/** Field specs from the untrusted `fields` prop; entries without a usable `name` are dropped. */
export function normalizeFields(raw: unknown): FieldSpec[] {
  if (!Array.isArray(raw)) return [];
  const seen = new Set<string>();
  const out: FieldSpec[] = [];
  for (const f of raw) {
    if (!f || typeof f !== "object") continue;
    const r = f as Record<string, unknown>;
    const name = typeof r.name === "string" ? r.name.trim() : "";
    if (!name || seen.has(name)) continue;
    seen.add(name);
    const type = (typeof r.type === "string" && FIELD_TYPES.has(r.type) ? r.type : "text") as FieldType;
    const opts = options(r.options);
    out.push({
      name,
      label: typeof r.label === "string" ? r.label : name,
      type: type === "text" && opts.length > 0 ? "select" : type,
      required: r.required === true,
      placeholder: typeof r.placeholder === "string" ? r.placeholder : undefined,
      help: typeof r.help === "string" ? r.help : undefined,
      default: r.default,
      options: opts,
      min: num(r.min),
      max: num(r.max),
      step: num(r.step),
      pattern: typeof r.pattern === "string" ? r.pattern : undefined,
      minLength: num(r.min_length),
      maxLength: num(r.max_length),
    });
  }
  return out;
}

function pad2(n: number): string {
  return String(n).padStart(2, "0");
}

/** `datetime-local` input value (local time) of an ISO timestamp. */
export function toLocalDateTimeInput(v: unknown): string {
  if (typeof v !== "string" || !v) return "";
  const t = Date.parse(v);
  if (Number.isNaN(t)) return "";
  const d = new Date(t);
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}T${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
}

/** Initial input values from each field's `default`. */
export function initialValues(fields: FieldSpec[]): Record<string, FieldValue> {
  const out: Record<string, FieldValue> = {};
  for (const f of fields) {
    const d = f.default;
    if (f.type === "boolean") {
      out[f.name] = d === true;
    } else if (f.type === "datetime") {
      out[f.name] = toLocalDateTimeInput(d);
    } else {
      out[f.name] = d === undefined || d === null ? "" : String(d);
    }
  }
  return out;
}

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

/** Validation error message of one field, or `null` when valid. */
export function validateField(f: FieldSpec, value: FieldValue | undefined): string | null {
  if (f.type === "boolean") {
    return f.required && value !== true ? `${f.label} must be checked` : null;
  }
  const s = typeof value === "string" ? value.trim() : "";
  if (s === "") return f.required ? `${f.label} is required` : null;
  switch (f.type) {
    case "number":
    case "integer": {
      const n = Number(s);
      if (!Number.isFinite(n)) return `${f.label} must be a number`;
      if (f.type === "integer" && !Number.isInteger(n)) return `${f.label} must be a whole number`;
      if (f.min !== undefined && n < f.min) return `${f.label} must be at least ${f.min}`;
      if (f.max !== undefined && n > f.max) return `${f.label} must be at most ${f.max}`;
      return null;
    }
    case "email":
      if (!EMAIL_RE.test(s)) return `${f.label} must be an email address`;
      break;
    case "url":
      try {
        const u = new URL(s);
        if (u.protocol !== "http:" && u.protocol !== "https:") throw new Error("protocol");
      } catch {
        return `${f.label} must be an http(s) URL`;
      }
      break;
    case "select":
      if (f.options.length > 0 && !f.options.some((o) => o.value === s)) {
        return `${f.label} must be one of the listed options`;
      }
      break;
    case "date":
      if (!/^\d{4}-\d{2}-\d{2}$/.test(s) || Number.isNaN(Date.parse(s))) {
        return `${f.label} must be a date (YYYY-MM-DD)`;
      }
      break;
    case "datetime":
      if (Number.isNaN(Date.parse(s))) return `${f.label} must be a date and time`;
      break;
    default:
      break;
  }
  if (f.minLength !== undefined && s.length < f.minLength) {
    return `${f.label} must be at least ${f.minLength} characters`;
  }
  if (f.maxLength !== undefined && s.length > f.maxLength) {
    return `${f.label} must be at most ${f.maxLength} characters`;
  }
  if (f.pattern) {
    let re: RegExp | null = null;
    try {
      re = new RegExp(`^(?:${f.pattern})$`);
    } catch {
      re = null;
    }
    if (re && !re.test(s)) return `${f.label} has an invalid format`;
  }
  return null;
}

/**
 * Validate every field and build the typed Request payload: numbers as numbers, booleans as
 * booleans, `datetime` as an ISO 8601 UTC timestamp, select values as their original option
 * values. Empty optional fields are omitted.
 */
export function buildPayload(
  fields: FieldSpec[],
  values: Record<string, FieldValue>
): { payload: Record<string, unknown>; errors: Record<string, string> } {
  const payload: Record<string, unknown> = {};
  const errors: Record<string, string> = {};
  for (const f of fields) {
    const v = values[f.name];
    const err = validateField(f, v);
    if (err) {
      errors[f.name] = err;
      continue;
    }
    if (f.type === "boolean") {
      payload[f.name] = v === true;
      continue;
    }
    const s = typeof v === "string" ? v.trim() : "";
    if (s === "") continue;
    switch (f.type) {
      case "number":
      case "integer":
        payload[f.name] = Number(s);
        break;
      case "datetime":
        payload[f.name] = new Date(Date.parse(s)).toISOString();
        break;
      case "select":
        payload[f.name] = f.options.find((o) => o.value === s)?.raw ?? s;
        break;
      case "textarea":
        payload[f.name] = typeof v === "string" ? v : s;
        break;
      default:
        payload[f.name] = s;
    }
  }
  return { payload, errors };
}
