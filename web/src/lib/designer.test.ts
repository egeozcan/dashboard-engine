import { describe, expect, it } from "vitest";
import type { DashboardDoc } from "./api";
import {
  applyDesignerEdits,
  applyOverridesToTree,
  effectiveProps,
  gridItemStyle,
  containerItemStyle,
  moveFromDrag,
  resizeFromDrag,
  ROW_UNIT_PX,
  type DesignerEdit,
} from "./designer";

const baseDoc = (): DashboardDoc => ({
  id: "engineering",
  title: "Engineering",
  layout: "grid",
  initial_state: {},
  adapters: [
    { adapter_instance: "pr_trend_adapter", area: "top", query_params: {} },
    { adapter_instance: "pr_table_adapter", area: "main", query_params: {} },
  ],
  overrides: {
    pr_table_adapter_pr_table: { w: 12, h: 6, visible: true, props: {} },
  },
});

describe("applyDesignerEdits", () => {
  it("applies every override field as a patch and never mutates the base", () => {
    const doc = baseDoc();
    const edits: DesignerEdit[] = [
      { kind: "override", widgetId: "pr_table_adapter_pr_table", patch: { x: 2, y: 1, w: 8 } },
      { kind: "override", widgetId: "pr_table_adapter_pr_table", patch: { h: null } },
      { kind: "override_props", widgetId: "pr_table_adapter_pr_table", props: { title: "PRs" } },
      { kind: "override", widgetId: "pr_trend_adapter_trend_chart", patch: { visible: false, w: 40 } },
    ];
    const next = applyDesignerEdits(doc, edits);
    expect(next.overrides.pr_table_adapter_pr_table).toEqual({
      x: 2,
      y: 1,
      w: 8,
      h: null,
      visible: true,
      props: { title: "PRs" },
    });
    // Clamped to the 12-column grid.
    expect(next.overrides.pr_trend_adapter_trend_chart).toMatchObject({ visible: false, w: 12 });
    expect(doc.overrides.pr_table_adapter_pr_table.w).toBe(12);
  });

  it("replays edits on a newer base so concurrent changes are preserved", () => {
    const edits: DesignerEdit[] = [
      { kind: "override", widgetId: "pr_table_adapter_pr_table", patch: { w: 6 } },
    ];
    const newer = baseDoc();
    newer.title = "Renamed by an Agent";
    newer.overrides.other_widget = { w: 3, props: {} };
    const next = applyDesignerEdits(newer, edits);
    expect(next.title).toBe("Renamed by an Agent");
    expect(next.overrides.other_widget).toEqual({ w: 3, props: {} });
    expect(next.overrides.pr_table_adapter_pr_table.w).toBe(6);
    expect(next.overrides.pr_table_adapter_pr_table.h).toBe(6);
  });

  it("resets overrides and edits placements", () => {
    const next = applyDesignerEdits(baseDoc(), [
      { kind: "reset_override", widgetId: "pr_table_adapter_pr_table" },
      { kind: "placement_add", placement: { adapter_instance: "ops_adapter", area: "side", query_params: {} } },
      { kind: "placement_update", adapterInstance: "pr_table_adapter", area: "side", queryParams: { repository: "acme/payments" } },
      { kind: "placement_move", adapterInstance: "ops_adapter", toIndex: 0 },
      { kind: "placement_remove", adapterInstance: "pr_trend_adapter" },
    ]);
    expect(next.overrides).toEqual({});
    expect(next.adapters).toEqual([
      { adapter_instance: "ops_adapter", area: "side", query_params: {} },
      { adapter_instance: "pr_table_adapter", area: "side", query_params: { repository: "acme/payments" } },
    ]);
  });
});

describe("layout override rendering", () => {
  it("maps x/y/w/h onto the 12-column grid", () => {
    expect(gridItemStyle({ x: 2, y: 1, w: 6, h: 3 }, 4)).toEqual({
      gridColumn: "3 / span 6",
      gridRow: "2 / span 3",
      minHeight: 3 * ROW_UNIT_PX,
    });
    expect(gridItemStyle({ h: 4 }, 4)).toEqual({ gridColumn: "span 4", gridRow: "span 4", minHeight: 4 * ROW_UNIT_PX });
    expect(gridItemStyle(undefined, 6)).toEqual({ gridColumn: "span 6" });
    // x near the right edge never overflows the grid.
    expect(gridItemStyle({ x: 10, w: 6 }, 4).gridColumn).toBe("11 / span 2");
    expect(containerItemStyle({ w: 6, h: 2 })).toEqual({ width: "50%", minHeight: 2 * ROW_UNIT_PX });
  });

  it("merges only configurable prop overrides", () => {
    const node = {
      widget_id: "t",
      widget_type: "dash.table",
      props: { title: "Adapter title", rows: [1] },
      layout_override: { props: { title: "Admin title", rows: [] } },
    };
    expect(effectiveProps(node)).toEqual({ title: "Admin title", rows: [1] });
    const custom = { widget_id: "c", widget_type: "engineering.pr_badge_widget", props: { open_count: 3 }, layout_override: { props: { open_count: 99 } } };
    expect(effectiveProps(custom)).toEqual({ open_count: 3 });
  });

  it("re-attaches overrides by widget_id across a nested tree", () => {
    const tree = {
      widget_id: "root",
      widget_type: "dash.container",
      layout_override: { w: 3 },
      children: [{ widget_id: "child", widget_type: "dash.text" }],
    };
    const out = applyOverridesToTree(tree, { child: { h: 2 } });
    expect(out.layout_override).toBeUndefined();
    expect(out.children?.[0].layout_override).toEqual({ h: 2 });
  });

  it("converts drag deltas into grid geometry", () => {
    const start = { x: 0, y: 0, w: 6, h: 2 };
    expect(resizeFromDrag(start, 100, ROW_UNIT_PX * 2, 50)).toEqual({ w: 8, h: 4 });
    expect(resizeFromDrag(start, -1000, -1000, 50)).toEqual({ w: 1, h: 1 });
    expect(moveFromDrag(start, 150, ROW_UNIT_PX, 50)).toEqual({ x: 3, y: 1 });
    expect(moveFromDrag(start, 5000, 0, 50)).toEqual({ x: 6, y: 0 });
  });
});
