import { describe, expect, it, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { App } from "./App";
import { WidgetRenderer } from "./components/widgets/BuiltInWidgets";
import { getOrCreateClientId, clientStateStorageKey } from "./lib/clientState";

describe("dash Web UI", () => {
  beforeEach(() => {
    localStorage.clear();
    vi.restoreAllMocks();
  });

  it("renders accessible Sign In form when unauthenticated and logs in", async () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const url = String(input);
      if (url.endsWith("/api/auth/me")) {
        return new Response(JSON.stringify({ error: "unauth" }), { status: 401 });
      }
      if (url.endsWith("/api/auth/login")) {
        return new Response(
          JSON.stringify({
            username: "admin",
            principal_kind: "administrator",
            csrf_token: "csrf_tok_1",
          }),
          { status: 200 }
        );
      }
      if (url.endsWith("/api/dashboards")) {
        return new Response(JSON.stringify([]), { status: 200 });
      }
      return new Response(JSON.stringify({}), { status: 200 });
    });

    render(<App />);
    const userField = await screen.findByLabelText("Username");
    const pwField = screen.getByLabelText("Password");
    expect(userField).toHaveAttribute("autocomplete", "username");
    expect(pwField).toHaveAttribute("autocomplete", "current-password");

    fireEvent.change(pwField, { target: { value: "secret" } });
    fireEvent.click(screen.getByRole("button", { name: "Sign In" }));

    await waitFor(() => {
      expect(screen.getByRole("navigation", { name: "Primary sections" })).toBeInTheDocument();
    });
  });

  it("syncs Client State across tabs via storage event and sorts TableWidget accessibly", async () => {
    const clientId = getOrCreateClientId();
    const onReq = vi.fn().mockResolvedValue(undefined);

    render(
      <WidgetRenderer
        adapterInstance="pr_table_adapter"
        generation={42}
        onWidgetRequest={onReq}
        node={{
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
            row_action: { label: "Merge PR", request: "merge_requested" },
          },
        }}
      />
    );

    // Sort by "#" column -> sets aria-sort on <th>
    const sortBtn = screen.getByRole("button", { name: "#" });
    fireEvent.click(sortBtn);
    expect(sortBtn.closest("th")).toHaveAttribute("aria-sort", "ascending");

    // Simulate another tab updating Client filter in localStorage
    const storageKey = clientStateStorageKey(clientId, "pr_filter");
    fireEvent(
      window,
      new StorageEvent("storage", {
        key: storageKey,
        newValue: JSON.stringify("Alpha"),
      })
    );
    await waitFor(() => {
      expect(screen.getByText("DEV-123 Alpha")).toBeInTheDocument();
      expect(screen.queryByText("DEV-124 Zeta")).not.toBeInTheDocument();
    });

    // Clicking row action emits typed Widget Request
    fireEvent.click(screen.getByRole("button", { name: "Merge PR" }));
    expect(onReq).toHaveBeenCalledWith(
      expect.objectContaining({
        widgetId: "pr_table_adapter_pr_table",
        generation: 42,
        request: "merge_requested",
      })
    );
  });

  it("renders custom widgets in sandboxed iframe without allow-same-origin", () => {
    render(
      <WidgetRenderer
        adapterInstance="pr_table_adapter"
        generation={1}
        onWidgetRequest={vi.fn()}
        node={{
          widget_id: "pr_table_adapter_pr_summary",
          widget_type: "engineering.pr_badge_widget",
          props: { repository: "acme/platform", open_count: 3 },
        }}
      />
    );
    const frame = screen.getByTitle("Custom widget pr_table_adapter_pr_summary");
    expect(frame.getAttribute("sandbox")).toBe("allow-scripts");
    expect(frame.getAttribute("sandbox")).not.toContain("allow-same-origin");
  });
});
