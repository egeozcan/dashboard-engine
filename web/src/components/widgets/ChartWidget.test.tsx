import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ChartWidget, hourStep, hourTicks, localMidnights, normalizeSeriesSpecs } from "./ChartWidget";

// Local wall-clock times, so the tests hold in any time zone.
const local = (day: number, hour: number) => new Date(2026, 10, day, hour).getTime();

function hourlyRows(fromDay: number, fromHour: number, count: number) {
  return Array.from({ length: count }, (_, i) => ({
    time: new Date(local(fromDay, fromHour) + i * 3600_000).toISOString(),
    temp: i % 5,
  }));
}

function renderChart(kind: "line" | "bar", rows: Array<Record<string, unknown>>) {
  const { container } = render(
    <ChartWidget title="Next 24 hours" rows={rows} xKey="time" series={normalizeSeriesSpecs(["temp"], "temp")} kind={kind} />
  );
  const texts = Array.from(container.querySelectorAll("svg > text")).map((t) => t.textContent ?? "");
  const boundaries = Array.from(container.querySelectorAll(".chart-day-boundary"));
  return { texts, boundaries };
}

describe("ChartWidget hour axis", () => {
  it("picks whole-hour ticks and local midnights", () => {
    expect(hourStep(23 * 3600_000)).toBe(3);
    expect(hourStep(5 * 3600_000)).toBe(1);
    expect(hourStep(70 * 3600_000)).toBe(12);
    expect(hourTicks(local(27, 13) + 60_000, local(27, 19), 3)).toEqual([local(27, 15), local(27, 18)]);
    expect(localMidnights(local(27, 14), local(29, 0))).toEqual([local(28, 0), local(29, 0)]);
    expect(localMidnights(local(27, 1), local(27, 23))).toEqual([]);
  });

  it("labels a 24-hour line chart with hours only and marks the day boundary", () => {
    const { texts, boundaries } = renderChart("line", hourlyRows(27, 14, 24));
    const hours = texts.filter((t) => /^\d\d:\d\d$/.test(t));
    expect(hours).toEqual(["15:00", "18:00", "21:00", "00:00", "03:00", "06:00", "09:00", "12:00"]);
    expect(texts.some((t) => t.includes("11-27 "))).toBe(false);
    expect(boundaries).toHaveLength(1);
    expect(Array.from(boundaries[0].querySelectorAll("text")).map((t) => t.textContent)).toEqual(["11-27", "11-28"]);
  });

  it("puts a bar chart's boundary between the last bar of one day and the first of the next", () => {
    const { boundaries } = renderChart("bar", hourlyRows(27, 22, 4));
    expect(boundaries).toHaveLength(1);
    const x = Number(boundaries[0].querySelector("line")?.getAttribute("x1"));
    const rects = Array.from(document.querySelectorAll("rect")).map((r) => Number(r.getAttribute("x")));
    // Bars at 22:00, 23:00 | 00:00, 01:00.
    expect(x).toBeGreaterThan(rects[1]);
    expect(x).toBeLessThan(rects[2]);
  });

  it("keeps date labels and no boundaries on long spans", () => {
    const rows = Array.from({ length: 8 }, (_, i) => ({ time: new Date(local(20 + i, 12)).toISOString(), temp: i }));
    const { texts, boundaries } = renderChart("line", rows);
    expect(boundaries).toHaveLength(0);
    expect(texts).toContain("11-20");
  });
});
