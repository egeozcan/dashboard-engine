// Built-in input widgets: `dash.button` (Request trigger), `dash.form` (typed, validated
// Request form) and `dash.filter` (Dashboard State control).
import React, { useEffect, useId, useRef, useState } from "react";
import {
  buildPayload,
  initialValues,
  normalizeFields,
  type FieldSpec,
  type FieldValue,
} from "../../lib/formFields";
import { normalizeOptions } from "../../lib/widgetFormat";

function variantClass(variant: unknown, fallback = ""): string {
  return variant === "primary" || variant === "danger" ? variant : variant === "default" ? "" : fallback;
}

export const ButtonWidget: React.FC<{
  label: string;
  variant?: string;
  disabled?: boolean;
  description?: string;
  onClick: () => Promise<void>;
}> = ({ label, variant, disabled, description, onClick }) => {
  const [busy, setBusy] = useState(false);
  const descId = useId();
  return (
    <span className="dash-button">
      <button
        type="button"
        className={`btn ${variantClass(variant, "primary")}`}
        disabled={disabled || busy}
        aria-busy={busy || undefined}
        aria-describedby={description ? descId : undefined}
        title={description}
        onClick={async () => {
          setBusy(true);
          try {
            await onClick();
          } finally {
            setBusy(false);
          }
        }}
      >
        {busy ? "Running..." : label}
      </button>
      {description && (
        <span id={descId} className="sr-only">
          {description}
        </span>
      )}
    </span>
  );
};

const INPUT_TYPES: Record<string, string> = {
  text: "text",
  number: "number",
  integer: "number",
  date: "date",
  datetime: "datetime-local",
  email: "email",
  url: "url",
};

const FieldInput: React.FC<{
  field: FieldSpec;
  value: FieldValue;
  error?: string;
  id: string;
  onChange: (v: FieldValue) => void;
}> = ({ field: f, value, error, id, onChange }) => {
  const describedBy = [f.help ? `${id}-help` : "", error ? `${id}-err` : ""].filter(Boolean).join(" ") || undefined;
  const common = {
    id,
    name: f.name,
    "aria-invalid": error ? true : undefined,
    "aria-describedby": describedBy,
    "aria-required": f.required || undefined,
  };
  if (f.type === "boolean") {
    return (
      <input
        {...common}
        type="checkbox"
        checked={value === true}
        onChange={(e) => onChange(e.target.checked)}
      />
    );
  }
  if (f.type === "textarea") {
    return (
      <textarea
        {...common}
        rows={4}
        value={String(value)}
        placeholder={f.placeholder}
        maxLength={f.maxLength}
        onChange={(e) => onChange(e.target.value)}
      />
    );
  }
  if (f.type === "select") {
    return (
      <select {...common} value={String(value)} onChange={(e) => onChange(e.target.value)}>
        <option value="">{f.placeholder ?? (f.required ? "Select…" : "—")}</option>
        {f.options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    );
  }
  return (
    <input
      {...common}
      type={INPUT_TYPES[f.type] ?? "text"}
      value={String(value)}
      placeholder={f.placeholder}
      min={f.min}
      max={f.max}
      step={f.step ?? (f.type === "integer" ? 1 : f.type === "number" ? "any" : undefined)}
      maxLength={f.maxLength}
      inputMode={f.type === "integer" ? "numeric" : f.type === "number" ? "decimal" : undefined}
      onChange={(e) => onChange(e.target.value)}
    />
  );
};

export const FormWidget: React.FC<{
  title: string;
  fields: unknown;
  submitLabel: string;
  variant?: string;
  disabled?: boolean;
  resetOnSubmit?: boolean;
  staticPayload?: Record<string, unknown>;
  onSubmit: (values: Record<string, unknown>) => Promise<void>;
}> = ({ title, fields: rawFields, submitLabel, variant, disabled, resetOnSubmit, staticPayload, onSubmit }) => {
  const baseId = useId();
  const fieldsKey = JSON.stringify(rawFields ?? null);
  const [fields, setFields] = useState<FieldSpec[]>(() => normalizeFields(rawFields));
  const [vals, setVals] = useState<Record<string, FieldValue>>(() => initialValues(fields));
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState(false);
  const lastKey = useRef(fieldsKey);

  // A re-render with different fields keeps what the user typed into fields that still exist.
  useEffect(() => {
    if (lastKey.current === fieldsKey) return;
    lastKey.current = fieldsKey;
    const next = normalizeFields(rawFields);
    setFields(next);
    setVals((prev) => {
      const init = initialValues(next);
      for (const f of next) if (f.name in prev) init[f.name] = prev[f.name];
      return init;
    });
    setErrors({});
  }, [fieldsKey, rawFields]);

  const set = (name: string, v: FieldValue) => {
    setVals((prev) => ({ ...prev, [name]: v }));
    if (errors[name]) setErrors(({ [name]: _drop, ...rest }) => rest);
  };

  return (
    <form
      className="card col dash-form"
      style={{ marginBottom: 0 }}
      noValidate
      aria-labelledby={`${baseId}-title`}
      onSubmit={async (e) => {
        e.preventDefault();
        if (busy || disabled) return;
        const { payload, errors: errs } = buildPayload(fields, vals);
        setErrors(errs);
        const firstBad = fields.find((f) => errs[f.name]);
        if (firstBad) {
          document.getElementById(`${baseId}-${firstBad.name}`)?.focus();
          return;
        }
        setBusy(true);
        try {
          await onSubmit({ ...(staticPayload ?? {}), ...payload });
          if (resetOnSubmit) setVals(initialValues(fields));
        } finally {
          setBusy(false);
        }
      }}
    >
      <strong id={`${baseId}-title`}>{title}</strong>
      {fields.map((f) => {
        const id = `${baseId}-${f.name}`;
        const err = errors[f.name];
        const input = <FieldInput field={f} value={vals[f.name] ?? ""} error={err} id={id} onChange={(v) => set(f.name, v)} />;
        return (
          <div key={f.name} className={`col form-field${err ? " invalid" : ""}`} style={{ gap: 4 }}>
            {f.type === "boolean" ? (
              <label htmlFor={id} className="row" style={{ gap: 6 }}>
                {input}
                <span>
                  {f.label}
                  {f.required && <span aria-hidden="true"> *</span>}
                </span>
              </label>
            ) : (
              <>
                <label htmlFor={id}>
                  {f.label}
                  {f.required && <span aria-hidden="true"> *</span>}
                </label>
                {input}
              </>
            )}
            {f.help && (
              <span id={`${id}-help`} className="field-help">
                {f.help}
              </span>
            )}
            {err && (
              <span id={`${id}-err`} className="field-error" role="alert">
                {err}
              </span>
            )}
          </div>
        );
      })}
      <div>
        <button type="submit" className={`btn ${variantClass(variant, "primary")}`} disabled={busy || disabled}>
          {busy ? "Submitting..." : submitLabel}
        </button>
      </div>
    </form>
  );
};

export type FilterControl = "select" | "multiselect" | "search" | "toggle" | "segmented";

function sameValue(a: unknown, b: unknown): boolean {
  return JSON.stringify(a ?? null) === JSON.stringify(b ?? null);
}

/**
 * Dashboard State control. The shown value follows Dashboard State (`state_key`) as pushed by
 * the server, falling back to the Adapter's `value` prop; changes are written back as State.
 */
export const FilterWidget: React.FC<{
  label: string;
  control: FilterControl;
  options: unknown;
  value: unknown;
  placeholder?: string;
  allLabel?: string;
  debounceMs?: number;
  onChange: (val: unknown) => void;
}> = ({ label, control, options: rawOptions, value, placeholder, allLabel, debounceMs = 300, onChange }) => {
  const id = useId();
  const options = normalizeOptions(rawOptions);
  const [local, setLocal] = useState<unknown>(value);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const valueKey = JSON.stringify(value ?? null);

  // Follow the authoritative value (another Client, an automation or a reload changed it).
  useEffect(() => {
    setLocal(value);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [valueKey]);
  useEffect(() => () => {
    if (timer.current) clearTimeout(timer.current);
  }, []);

  const commit = (next: unknown) => {
    setLocal(next);
    if (!sameValue(next, value)) onChange(next);
  };

  switch (control) {
    case "search":
      return (
        <label className="row dash-filter" htmlFor={id}>
          <span>{label}:</span>
          <input
            id={id}
            type="search"
            value={typeof local === "string" ? local : local == null ? "" : String(local)}
            placeholder={placeholder ?? "Search…"}
            onChange={(e) => {
              const next = e.target.value;
              setLocal(next);
              if (timer.current) clearTimeout(timer.current);
              timer.current = setTimeout(() => {
                if (!sameValue(next, value)) onChange(next);
              }, Math.max(0, debounceMs));
            }}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                if (timer.current) clearTimeout(timer.current);
                commit((e.target as HTMLInputElement).value);
              }
            }}
          />
        </label>
      );
    case "toggle":
      return (
        <label className="row dash-filter" htmlFor={id}>
          <input id={id} type="checkbox" checked={local === true || local === "true"} onChange={(e) => commit(e.target.checked)} />
          <span>{label}</span>
        </label>
      );
    case "multiselect": {
      const selected = new Set(Array.isArray(local) ? local.map(String) : []);
      return (
        <fieldset className="row dash-filter" style={{ border: 0, padding: 0, margin: 0 }}>
          <legend style={{ float: "left", marginRight: 6 }}>{label}:</legend>
          {options.map((o) => (
            <label key={o.value} className="row" style={{ gap: 4 }}>
              <input
                type="checkbox"
                checked={selected.has(o.value)}
                onChange={(e) => {
                  const next = new Set(selected);
                  if (e.target.checked) next.add(o.value);
                  else next.delete(o.value);
                  commit(options.map((x) => x.value).filter((v) => next.has(v)));
                }}
              />
              <span>{o.label}</span>
            </label>
          ))}
        </fieldset>
      );
    }
    case "segmented": {
      const all = allLabel !== undefined ? [{ value: "", label: allLabel }, ...options] : options;
      return (
        <div className="row dash-filter" role="group" aria-label={label}>
          <span>{label}:</span>
          <div className="segmented">
            {all.map((o) => (
              <button
                key={o.value}
                type="button"
                className={`btn ${String(local ?? "") === o.value ? "primary" : ""}`}
                aria-pressed={String(local ?? "") === o.value}
                onClick={() => commit(o.value)}
              >
                {o.label}
              </button>
            ))}
          </div>
        </div>
      );
    }
    default: {
      const current = local == null ? "" : String(local);
      const known = options.some((o) => o.value === current);
      return (
        <label className="row dash-filter" htmlFor={id}>
          <span>{label}:</span>
          <select id={id} value={current} onChange={(e) => commit(e.target.value)}>
            {(allLabel !== undefined || placeholder !== undefined || (!known && current === "")) && (
              <option value="">{allLabel ?? placeholder ?? "—"}</option>
            )}
            {!known && current !== "" && <option value={current}>{current}</option>}
            {options.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </label>
      );
    }
  }
};
