import { describe, expect, it } from "vitest";
import { buildPayload, initialValues, normalizeFields } from "./formFields";
import {
  formatBytes,
  formatDuration,
  formatValue,
  niceTicks,
  parseTime,
  safeColor,
  safeHref,
  statusTone,
} from "./widgetFormat";

describe("formatValue", () => {
  it("formats numbers, units, bytes, durations and booleans", () => {
    expect(formatValue(null)).toBe("—");
    expect(formatValue("")).toBe("—");
    expect(formatValue(1234.5678, { format: "number", decimals: 1 })).toMatch(/1.?234\.6/);
    expect(formatValue(0.256, { format: "percent" })).toMatch(/25\.6\s?%/);
    expect(formatValue(42, { unit: "%" })).toBe("42%");
    expect(formatValue(42, { unit: "ms" })).toBe("42 ms");
    expect(formatValue(5, { prefix: "~" })).toBe("~5");
    expect(formatValue(1536, { format: "bytes" })).toBe("1.5 KB");
    expect(formatBytes(512)).toBe("512 B");
    expect(formatValue(3725, { format: "duration" })).toBe("1h 2m");
    expect(formatValue(1500, { format: "duration_ms" })).toBe("2s");
    expect(formatDuration(0.25)).toBe("250 ms");
    expect(formatDuration(3600)).toBe("1h");
    expect(formatDuration(90061)).toBe("1d 1h");
    expect(formatValue(true)).toBe("Yes");
    expect(formatValue("false", { format: "boolean" })).toBe("No");
    expect(formatValue(12.5, { format: "currency", currency: "EUR", decimals: 2 })).toMatch(/12\.50/);
    // Invalid currency codes never throw.
    expect(formatValue(3, { format: "currency", currency: "nope!" })).toBe("3");
  });

  it("leaves values that do not fit the format readable", () => {
    expect(formatValue("n/a", { format: "number" })).toBe("n/a");
    expect(formatValue("DEV-1")).toBe("DEV-1");
    expect(formatValue({ a: 1 })).toBe('{"a":1}');
    expect(formatValue("not a date", { format: "datetime" })).toBe("not a date");
  });

  it("formats relative times against a reference clock", () => {
    const now = Date.parse("2026-01-01T12:00:00Z");
    expect(formatValue("2026-01-01T11:57:00Z", { format: "relative" }, now)).toMatch(/3 minutes ago/);
    expect(formatValue("2026-01-01T14:00:00Z", { format: "relative" }, now)).toMatch(/in 2 hours/);
  });
});

describe("helpers", () => {
  it("parses ISO strings and epoch seconds/milliseconds", () => {
    expect(parseTime("2026-01-01T00:00:00Z")).toBe(Date.UTC(2026, 0, 1));
    expect(parseTime(1_767_225_600)).toBe(1_767_225_600_000);
    expect(parseTime(1_767_225_600_000)).toBe(1_767_225_600_000);
    expect(parseTime("DEV-123")).toBeNull();
  });

  it("maps status words to tones", () => {
    expect(statusTone("Healthy")).toBe("ok");
    expect(statusTone("degraded")).toBe("warn");
    expect(statusTone("FAILED")).toBe("danger");
    expect(statusTone("whatever")).toBe("neutral");
    expect(statusTone(undefined)).toBe("neutral");
  });

  it("only allows safe link targets and colors", () => {
    expect(safeHref("https://example.com/a?b=1")).toBe("https://example.com/a?b=1");
    expect(safeHref("mailto:ops@example.com")).toBe("mailto:ops@example.com");
    expect(safeHref("/dashboards/eng")).toBe("/dashboards/eng");
    expect(safeHref("javascript:alert(1)")).toBeUndefined();
    expect(safeHref("data:text/html,<b>x</b>")).toBeUndefined();
    expect(safeHref("//evil.example/x")).toBeUndefined();
    expect(safeColor("#ff0000")).toBe("#ff0000");
    expect(safeColor("rgb(1, 2, 3)")).toBe("rgb(1, 2, 3)");
    expect(safeColor("var(--ok)")).toBe("var(--ok)");
    expect(safeColor("url(https://evil.example/x.svg#a)")).toBeUndefined();
    expect(safeColor("red;background:url(x)")).toBeUndefined();
  });

  it("computes nice axis ticks", () => {
    expect(niceTicks(0, 3)).toEqual([0, 1, 2, 3]);
    expect(niceTicks(0, 100)).toEqual([0, 50, 100]);
    expect(niceTicks(-12, 37)).toEqual([-20, 0, 20, 40]);
    expect(niceTicks(5, 5).length).toBeGreaterThan(1);
  });
});

describe("form fields", () => {
  const fields = normalizeFields([
    { name: "title", label: "Title", required: true, max_length: 10 },
    { name: "count", label: "Count", type: "integer", min: 1, max: 5, default: 2 },
    { name: "ratio", label: "Ratio", type: "number" },
    { name: "notify", label: "Notify", type: "boolean", default: true },
    { name: "env", label: "Env", options: ["staging", { value: 3, label: "Prod" }] },
    { name: "contact", label: "Contact", type: "email" },
    { name: "when", label: "When", type: "datetime" },
    { name: "", label: "dropped" },
    { name: "title", label: "duplicate dropped" },
  ]);

  it("normalizes fields and defaults", () => {
    expect(fields.map((f) => f.name)).toEqual(["title", "count", "ratio", "notify", "env", "contact", "when"]);
    expect(fields.find((f) => f.name === "env")?.type).toBe("select");
    expect(initialValues(fields)).toMatchObject({ title: "", count: "2", notify: true, env: "" });
  });

  it("validates and coerces the payload", () => {
    const bad = buildPayload(fields, { ...initialValues(fields), count: "9", contact: "nope", title: "" });
    expect(Object.keys(bad.errors).sort()).toEqual(["contact", "count", "title"]);

    const ok = buildPayload(fields, {
      ...initialValues(fields),
      title: "  Deploy ",
      count: "3",
      ratio: "0.5",
      env: "3",
      when: "2026-01-02T03:04",
    });
    expect(ok.errors).toEqual({});
    expect(ok.payload).toMatchObject({ title: "Deploy", count: 3, ratio: 0.5, notify: true, env: 3 });
    expect(ok.payload.when).toBe(new Date(2026, 0, 2, 3, 4).toISOString());
    expect("contact" in ok.payload).toBe(false);

    const integer = buildPayload(fields, { ...initialValues(fields), title: "x", count: "2.5" });
    expect(integer.errors.count).toMatch(/whole number/);
  });
});
