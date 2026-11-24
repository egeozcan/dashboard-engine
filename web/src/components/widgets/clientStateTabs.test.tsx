// §17 AC9: Client State is shared by every tab of one browser profile (one Client), persists
// across a reload, and is namespaced per Dashboard and widget_id.
//
// Each "tab" is its own React root rendering the real table widget. jsdom does not deliver
// `storage` events between documents, so this test plays the browser's part: every real write
// a tab makes to localStorage is announced to the other tabs as the StorageEvent a browser would
// fire (same key, old and new value). Nothing in the test constructs storage keys or values.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, waitFor, within } from "@testing-library/react";
import { WidgetRenderer } from "./BuiltInWidgets";

type Tab = ReturnType<typeof render>;

const node = {
  widget_id: "pr_table_adapter_pr_table",
  widget_type: "dash.table",
  props: {
    title: "Open Pull Requests",
    columns: [
      { key: "number", label: "#", type: "number" },
      { key: "title", label: "Title" },
    ],
    rows: [
      { id: "p2", number: 43, title: "DEV-124 Zeta" },
      { id: "p1", number: 42, title: "DEV-123 Alpha" },
    ],
    client_sort_state_key: "pr_sort",
    client_filter_state_key: "pr_filter",
  },
};

function openTab(dashboardId = "engineering"): Tab {
  return render(
    <WidgetRenderer
      adapterInstance="pr_table_adapter"
      generation={1}
      dashboardId={dashboardId}
      onWidgetRequest={vi.fn().mockResolvedValue(undefined)}
      node={node}
    />
  );
}

function titles(tab: Tab): string[] {
  return within(tab.container)
    .getAllByRole("row")
    .slice(1)
    .map((r) => within(r).getAllByRole("cell")[1]?.textContent ?? "");
}

describe("Client State across tabs", () => {
  let writes = 0;

  beforeEach(() => {
    localStorage.clear();
    writes = 0;
    const storage = window.localStorage;
    const setItem = storage.setItem.bind(storage);
    const removeItem = storage.removeItem.bind(storage);
    vi.spyOn(storage, "setItem").mockImplementation((key: string, value: string) => {
      const oldValue = storage.getItem(key);
      setItem(key, value);
      writes += 1;
      window.dispatchEvent(new StorageEvent("storage", { key, oldValue, newValue: value }));
    });
    vi.spyOn(storage, "removeItem").mockImplementation((key: string) => {
      const oldValue = storage.getItem(key);
      removeItem(key);
      window.dispatchEvent(new StorageEvent("storage", { key, oldValue, newValue: null }));
    });
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("filter and sort chosen in one tab apply in the other, and survive a reload", async () => {
    const a = openTab();
    const b = openTab();
    expect(titles(b)).toEqual(["DEV-124 Zeta", "DEV-123 Alpha"]);

    fireEvent.change(within(a.container).getByRole("searchbox"), { target: { value: "alpha" } });
    expect(writes).toBeGreaterThan(0);
    await waitFor(() => expect(titles(b)).toEqual(["DEV-123 Alpha"]));
    expect(within(b.container).getByRole("searchbox")).toHaveValue("alpha");

    // Clearing the filter in B and sorting there flows back to A.
    fireEvent.change(within(b.container).getByRole("searchbox"), { target: { value: "" } });
    fireEvent.click(within(b.container).getByRole("button", { name: "#" }));
    await waitFor(() => {
      expect(titles(a)).toEqual(["DEV-123 Alpha", "DEV-124 Zeta"]);
      expect(within(a.container).getByRole("button", { name: "#" }).closest("th")).toHaveAttribute(
        "aria-sort",
        "ascending"
      );
    });

    // Reload: every tab closed, a new one opens with the profile's Client State.
    a.unmount();
    b.unmount();
    const c = openTab();
    expect(titles(c)).toEqual(["DEV-123 Alpha", "DEV-124 Zeta"]);
    expect(within(c.container).getByRole("button", { name: "#" }).closest("th")).toHaveAttribute(
      "aria-sort",
      "ascending"
    );
  });

  it("is namespaced per Dashboard: the same widget on another Dashboard is unaffected", async () => {
    const eng = openTab("engineering");
    const other = openTab("payments");
    fireEvent.change(within(eng.container).getByRole("searchbox"), { target: { value: "zeta" } });
    await waitFor(() => expect(titles(eng)).toEqual(["DEV-124 Zeta"]));
    expect(titles(other)).toEqual(["DEV-124 Zeta", "DEV-123 Alpha"]);
    expect(within(other.container).getByRole("searchbox")).toHaveValue("");
  });
});
