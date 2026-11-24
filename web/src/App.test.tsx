import { describe, expect, it, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { App } from "./App";
import { WidgetRenderer } from "./components/widgets/BuiltInWidgets";
import { GraphEditorView } from "./views/GraphEditorView";
import { getOrCreateClientId, clientStateStorageKey, scopedClientStateKey } from "./lib/clientState";

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

    fireEvent.change(userField, { target: { value: "admin" } });
    fireEvent.change(pwField, { target: { value: "secret" } });
    fireEvent.click(screen.getByRole("button", { name: "Sign In" }));

    await waitFor(() => {
      expect(screen.getByRole("navigation", { name: "Primary sections" })).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: "Sign Out" }));
    await waitFor(() => {
      expect(screen.getByLabelText("Username")).toHaveValue("");
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
    // Client State keys are namespaced by Dashboard and widget_id (§8)
    const storageKey = clientStateStorageKey(
      clientId,
      scopedClientStateKey({ widgetId: "pr_table_adapter_pr_table" }, "pr_filter")
    );
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

  it("keeps GraphEditorView drag slot separate from create-instance slot and supports Discard Draft", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = String(input);
      if (url.endsWith("/api/graph")) {
        return new Response(
          JSON.stringify({
            generation: 1,
            revision: "abc123",
            instances: {},
            validation: { valid: true, issues: [] },
          }),
          { status: 200 }
        );
      }
      if (url.endsWith("/api/drafts") && init?.method === "POST") {
        return new Response(JSON.stringify({ draft_id: "dft_ui_1" }), { status: 200 });
      }
      if (url.endsWith("/api/drafts/dft_ui_1/instances")) {
        return new Response(JSON.stringify({ draft_id: "dft_ui_1" }), { status: 200 });
      }
      if (url.endsWith("/api/drafts/dft_ui_1/validate")) {
        return new Response(JSON.stringify({ valid: false, issues: [{ severity: "error", message: "bad def" }] }), {
          status: 200,
        });
      }
      if (url.endsWith("/api/drafts/dft_ui_1") && init?.method === "DELETE") {
        return new Response(JSON.stringify({ aborted: true }), { status: 200 });
      }
      return new Response(JSON.stringify({}), { status: 200 });
    });

    render(<GraphEditorView />);
    await screen.findByText("Configured Dependency Graph");

    const dragInput = screen.getByLabelText("Target input slot for drag-connect:") as HTMLInputElement;
    const formSlotInput = screen.getByPlaceholderText("input slot (e.g. prs)") as HTMLInputElement;
    // Drag-connect slot defaults to automatic selection from the target's declared inputs.
    expect(dragInput.value).toBe("");
    expect(formSlotInput.value).toBe("prs");

    fireEvent.change(dragInput, { target: { value: "issues" } });
    fireEvent.change(formSlotInput, { target: { value: "linked" } });
    expect(formSlotInput.value).toBe("linked");
    expect(dragInput.value).toBe("issues");

    fireEvent.change(screen.getByPlaceholderText("instance_id"), { target: { value: "bad_inst" } });
    fireEvent.change(screen.getByPlaceholderText("definition (pkg.def)"), {
      target: { value: "engineering.linked_prs" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Add to Draft" }));

    await screen.findByText("Active Draft: dft_ui_1");
    const discardBtn = screen.getByRole("button", { name: "Discard Draft" });
    fireEvent.click(discardBtn);

    await waitFor(() => {
      expect(screen.queryByText("Active Draft: dft_ui_1")).not.toBeInTheDocument();
    });
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/drafts/dft_ui_1",
      expect.objectContaining({ method: "DELETE" })
    );
  });

  it("preserves grid and container layout overrides consistently across normal and Designer modes", () => {
    const gridNode = {
      widget_id: "trend_root",
      widget_type: "dash.grid",
      props: { columns: 2, gap: 16 },
      children: [
        {
          widget_id: "open_count",
          widget_type: "dash.counter",
          props: { label: "Open PRs", value: 3 },
        },
        {
          widget_id: "trend_chart",
          widget_type: "dash.text",
          props: { text: "Chart placeholder" },
          layout_override: { w: 8 },
        },
      ],
    };

    for (const designerMode of [false, true]) {
      const { unmount, container } = render(
        <WidgetRenderer
          adapterInstance="pr_trend_adapter"
          generation={1}
          designerMode={designerMode}
          onWidgetRequest={vi.fn()}
          node={gridNode}
        />
      );
      const rootEl = container.querySelector('[data-widget-id="trend_root"]')!;
      const gridEl = Array.from(rootEl.querySelectorAll("div")).find(
        (d) => d.style.display === "grid"
      );
      expect(gridEl?.style.gridTemplateColumns).toBe("repeat(12, minmax(0, 1fr))");

      const countEl = container.querySelector('[data-widget-id="open_count"]') as HTMLElement;
      const chartEl = container.querySelector('[data-widget-id="trend_chart"]') as HTMLElement;
      expect(countEl.style.gridColumn).toBe("span 4");
      expect(chartEl.style.gridColumn).toBe("span 8");
      unmount();
    }

    const containerNode = {
      widget_id: "box",
      widget_type: "dash.container",
      props: { direction: "vertical" },
      children: [
        {
          widget_id: "half_child",
          widget_type: "dash.text",
          props: { text: "Half" },
          layout_override: { w: 6 },
        },
      ],
    };
    const { container } = render(
      <WidgetRenderer
        adapterInstance="pr_table_adapter"
        generation={1}
        designerMode={false}
        onWidgetRequest={vi.fn()}
        node={containerNode}
      />
    );
    const halfEl = container.querySelector('[data-widget-id="half_child"]') as HTMLElement;
    expect(halfEl.style.width).toBe("50%");
  });

  it("renders Y-axis scale and X-axis timestamp labels on ChartWidget even with 1 sample", () => {
    const { container } = render(
      <WidgetRenderer
        adapterInstance="pr_trend_adapter"
        generation={1}
        onWidgetRequest={vi.fn()}
        node={{
          widget_id: "trend_chart",
          widget_type: "dash.chart",
          props: {
            title: "30-Day Open PR Count",
            series: [{ sampled_at: "2026-11-24T12:00:00Z", value_num: 3 }],
            x_key: "sampled_at",
            y_key: "value_num",
          },
        }}
      />
    );
    const texts = Array.from(container.querySelectorAll("svg text")).map((t) => t.textContent);
    expect(texts).toContain("0");
    expect(texts).toContain("3");
    expect(texts.some((t) => t && t.includes("11-24"))).toBe(true);
  });
});
