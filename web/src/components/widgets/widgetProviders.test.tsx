// Behavior of the built-in widget providers (dash.table, dash.chart, dash.map, dash.counter,
// dash.text, dash.form, dash.button, dash.filter, dash.tabs) as rendered by WidgetRenderer.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import type { WidgetNode } from "../../lib/api";
import { WidgetRenderer } from "./BuiltInWidgets";
import { rowsToCsv, normalizeColumns } from "./TableWidget";

function renderNode(
  node: WidgetNode,
  extra: Partial<React.ComponentProps<typeof WidgetRenderer>> = {}
) {
  const onWidgetRequest = vi.fn().mockResolvedValue(undefined);
  const utils = render(
    <WidgetRenderer
      adapterInstance="ad"
      generation={7}
      dashboardId="dash1"
      onWidgetRequest={onWidgetRequest}
      {...extra}
      node={node}
    />
  );
  return { ...utils, onWidgetRequest };
}

beforeEach(() => localStorage.clear());
afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe("dash.table", () => {
  const rows = Array.from({ length: 30 }, (_, i) => ({
    id: `r${i}`,
    name: `Service ${String(i).padStart(2, "0")}`,
    status: i % 3 === 0 ? "down" : "up",
    latency_ms: i * 10,
    url: i === 1 ? "javascript:alert(1)" : `https://status.example/${i}`,
    secret_note: "hidden-needle",
    owner: i === 2 ? null : "ops",
  }));
  const node: WidgetNode = {
    widget_id: "ad_services",
    widget_type: "dash.table",
    props: {
      title: "Services",
      columns: [
        { key: "name", label: "Name", type: "link", href_key: "url" },
        { key: "status", label: "Status", type: "badge" },
        { key: "latency_ms", label: "Latency", format: "duration_ms" },
        { key: "owner", label: "Owner" },
        { key: "secret_note", label: "Note", hidden: true },
      ],
      rows,
      page_size: 10,
      default_sort: { key: "latency_ms", dir: "desc" },
      row_actions: [
        { label: "Restart", request: "restart", variant: "danger" },
        { label: "Ack", request: "ack" },
      ],
    },
  };

  it("formats cells, renders safe links and status badges, and hides hidden columns", () => {
    renderNode(node);
    expect(screen.queryByRole("columnheader", { name: "Note" })).toBeNull();
    const firstRow = screen.getAllByRole("row")[1];
    // Default sort: latency descending → Service 29 first.
    expect(within(firstRow).getByRole("link", { name: "Service 29" })).toHaveAttribute(
      "href",
      "https://status.example/29"
    );
    expect(within(firstRow).getByRole("link", { name: "Service 29" })).toHaveAttribute("rel", "noopener noreferrer");
    expect(within(firstRow).getByText("up")).toHaveClass("badge", "ok");
    expect(within(firstRow).getByText("290 ms")).toBeInTheDocument();
    expect(screen.getByLabelText("Row count")).toHaveTextContent("30 rows");
  });

  it("paginates, searches only visible columns and keeps empty cells last", () => {
    renderNode(node);
    expect(screen.getAllByRole("row")).toHaveLength(11);
    expect(screen.getByText(/Page 1 of 3/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    expect(screen.getByText(/Page 2 of 3/)).toBeInTheDocument();

    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "hidden-needle" } });
    expect(screen.getByText("No matching rows")).toBeInTheDocument();
    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "service 0" } });
    expect(screen.getByLabelText("Row count")).toHaveTextContent("10 of 30 rows");
    expect(screen.queryByText(/Page \d of/)).toBeNull();

    fireEvent.change(screen.getByRole("searchbox"), { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: "Owner" }));
    fireEvent.click(screen.getByRole("button", { name: "Owner" }));
    // Descending: the row without an owner is still last (on the last page).
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    fireEvent.click(screen.getByRole("button", { name: "Next" }));
    const bodyRows = screen.getAllByRole("row").slice(1);
    expect(within(bodyRows[bodyRows.length - 1]).getByText("Service 02")).toBeInTheDocument();
  });

  it("does not link unsafe URLs and sends the row with the chosen row action", async () => {
    const { onWidgetRequest } = renderNode({
      ...node,
      props: { ...node.props, default_sort: { key: "latency_ms", dir: "asc" } },
    });
    const row1 = screen.getAllByRole("row")[2];
    expect(within(row1).queryByRole("link")).toBeNull();
    expect(within(row1).getByText("Service 01")).toBeInTheDocument();
    fireEvent.click(within(row1).getByRole("button", { name: "Ack" }));
    await waitFor(() => expect(onWidgetRequest).toHaveBeenCalledTimes(1));
    expect(onWidgetRequest.mock.calls[0][0]).toMatchObject({
      widgetId: "ad_services",
      generation: 7,
      request: "ack",
      payload: { id: "r1" },
    });
  });

  it("exports CSV with formula injection neutralized", () => {
    const cols = normalizeColumns([{ key: "a", label: "A" }, { key: "b", label: "B, quoted" }]);
    expect(rowsToCsv([{ a: "=HYPERLINK(1)", b: 'say "hi"' }, { a: -5, b: null }], cols)).toBe(
      'A,"B, quoted"\r\n\'=HYPERLINK(1),"say ""hi"""\r\n-5,'
    );
  });
});

describe("dash.chart", () => {
  it("plots several series with a legend, thresholds and gaps for non-numeric values", () => {
    const { container } = renderNode({
      widget_id: "ad_chart",
      widget_type: "dash.chart",
      props: {
        title: "Latency",
        kind: "area",
        x_key: "t",
        y_keys: [{ key: "p50", label: "p50" }, { key: "p99", label: "p99", color: "url(evil)" }],
        series: [
          { t: "2026-01-01T00:00:00Z", p50: 10, p99: 40 },
          { t: "2026-01-01T00:05:00Z", p50: "n/a", p99: 55 },
          { t: "2026-01-01T00:10:00Z", p50: 12, p99: 48 },
        ],
        thresholds: [{ value: 50, label: "SLO", status: "critical" }],
        unit: "ms",
      },
    });
    expect(within(screen.getByRole("list", { name: "Series" })).getAllByRole("listitem")).toHaveLength(2);
    expect(screen.getByText("SLO")).toBeInTheDocument();
    // p50's gap splits its line into two single-point segments: no "L" between them.
    const paths = Array.from(container.querySelectorAll("path[fill='none']")).map((p) => p.getAttribute("d"));
    expect(paths[0]).not.toMatch(/L/);
    expect(paths[1]).toMatch(/L.*L/);
    // Unsafe colors fall back to the palette.
    expect(container.innerHTML).not.toContain("url(evil)");
    expect(container.querySelector("desc")?.textContent).toMatch(/p99 latest 48 ms, min 40 ms, max 55 ms/);
  });

  it("draws bars for categories, handles negative values and shows an empty state", () => {
    const { container, unmount } = renderNode({
      widget_id: "ad_bars",
      widget_type: "dash.chart",
      props: {
        title: "Net",
        kind: "bar",
        x_key: "team",
        y_key: "delta",
        series: [
          { team: "api", delta: 5 },
          { team: "web", delta: -3 },
        ],
      },
    });
    expect(container.querySelectorAll("rect[fill]").length).toBeGreaterThanOrEqual(2);
    const texts = Array.from(container.querySelectorAll("svg text")).map((t) => t.textContent);
    expect(texts).toEqual(expect.arrayContaining(["api", "web", "0"]));
    expect(texts.some((t) => t?.startsWith("-"))).toBe(true);
    unmount();

    renderNode({ widget_id: "ad_empty", widget_type: "dash.chart", props: { title: "Empty", series: [] } });
    expect(screen.getByText("No data")).toBeInTheDocument();
  });

  it("shows a keyboard-driven tooltip", () => {
    renderNode({
      widget_id: "ad_kb",
      widget_type: "dash.chart",
      props: { title: "Q", x_key: "x", y_key: "y", series: [{ x: 1, y: 2 }, { x: 2, y: 4 }] },
    });
    const svg = screen.getByRole("img", { name: "Q" });
    fireEvent.keyDown(svg, { key: "End" });
    expect(screen.getByRole("status")).toHaveTextContent("4");
  });
});

describe("dash.map", () => {
  it("drops invalid points, colors by status and sends marker Requests", async () => {
    const { onWidgetRequest } = renderNode({
      widget_id: "ad_map",
      widget_type: "dash.map",
      props: {
        title: "Sites",
        points: [
          { id: "fra", label: "Frankfurt", lat: 50.1, lon: 8.7, status: "down" },
          { id: "iad", label: "Virginia", lat: 38.9, lng: -77.0, status: "up", value: 10 },
          { id: "bad", lat: 123, lon: 0 },
          { id: "nan", lat: "x", lon: 1 },
        ],
        point_action: { label: "Inspect", request: "inspect_site" },
      },
    });
    expect(screen.getByText("2 locations")).toBeInTheDocument();
    expect(screen.getByText("2 invalid")).toBeInTheDocument();
    expect(screen.getByText("down: 1")).toBeInTheDocument();
    const marker = screen.getByRole("button", { name: /Inspect: Frankfurt \(down\)/ });
    fireEvent.keyDown(marker, { key: "Enter" });
    await waitFor(() => expect(onWidgetRequest).toHaveBeenCalledTimes(1));
    expect(onWidgetRequest.mock.calls[0][0]).toMatchObject({ request: "inspect_site", payload: { id: "fra" } });
  });
});

describe("dash.counter", () => {
  it("formats the value, derives status from thresholds and shows the change", () => {
    renderNode({
      widget_id: "ad_err",
      widget_type: "dash.counter",
      props: {
        label: "Error rate",
        value: 0.052,
        format: "percent",
        previous: 0.04,
        higher_is_better: false,
        thresholds: { warning: 0.01, critical: 0.05 },
        sparkline: [1, 3, 2, 5],
        description: "5xx / all requests",
      },
    });
    expect(screen.getByText("5.2%")).toBeInTheDocument();
    expect(screen.getByText("critical")).toHaveClass("badge", "danger");
    // +30% while lower is better → shown as a regression.
    expect(screen.getByText(/▲ 30%/)).toHaveClass("counter-delta", "danger");
    expect(screen.getByRole("img", { name: "Error rate trend" })).toBeInTheDocument();
    expect(screen.getByText("5xx / all requests")).toBeInTheDocument();
  });

  it("shows no status badge unless the Adapter gives one", () => {
    renderNode({ widget_id: "ad_c", widget_type: "dash.counter", props: { label: "Open", value: 3 } });
    expect(screen.queryByText("healthy")).toBeNull();
    expect(screen.getByText("3")).toBeInTheDocument();
  });
});

describe("dash.text", () => {
  it("renders the Markdown subset without injecting markup or unsafe links", () => {
    const { container } = renderNode({
      widget_id: "ad_md",
      widget_type: "dash.text",
      props: {
        title: "Runbook",
        markdown: true,
        variant: "warning",
        text: "## Steps\n- **Drain** the node\n- run `kubectl cordon`\n\n[Docs](https://docs.example/run) and [bad](javascript:alert(1)) <img src=x onerror=alert(1)>",
      },
    });
    expect(screen.getByRole("heading", { name: "Steps" })).toBeInTheDocument();
    expect(screen.getByText("Drain").tagName).toBe("STRONG");
    expect(screen.getByText("kubectl cordon").tagName).toBe("CODE");
    expect(screen.getByRole("link", { name: "Docs" })).toHaveAttribute("href", "https://docs.example/run");
    expect(screen.queryByRole("link", { name: "bad" })).toBeNull();
    expect(container.querySelector("img")).toBeNull();
    expect(container.querySelector(".callout.warning")).not.toBeNull();
  });

  it("marks server-side contract violation nodes as alerts", () => {
    renderNode({
      widget_id: "ad_x",
      widget_type: "dash.text",
      render_error: "WIDGET_CONTRACT_VIOLATION: ...",
      props: { title: "Widget unavailable", text: "WIDGET_CONTRACT_VIOLATION: ..." },
    });
    expect(screen.getByRole("alert")).toHaveClass("callout", "danger");
  });
});

describe("dash.form and dash.button", () => {
  it("validates, then submits typed values merged with the static payload", async () => {
    const { onWidgetRequest } = renderNode({
      widget_id: "ad_form",
      widget_type: "dash.form",
      props: {
        title: "Scale",
        request: "scale",
        payload: { service: "api" },
        reset_on_submit: true,
        fields: [
          { name: "replicas", label: "Replicas", type: "integer", required: true, min: 1, max: 20 },
          { name: "reason", label: "Reason", type: "textarea", help: "Shown in the audit log" },
          { name: "dry", label: "Canary first", type: "boolean", default: true },
        ],
      },
    });
    fireEvent.click(screen.getByRole("button", { name: "Submit" }));
    expect(await screen.findByText("Replicas is required")).toBeInTheDocument();
    expect(screen.getByLabelText(/Replicas/)).toHaveAttribute("aria-invalid", "true");
    expect(onWidgetRequest).not.toHaveBeenCalled();

    fireEvent.change(screen.getByLabelText(/Replicas/), { target: { value: "50" } });
    fireEvent.click(screen.getByRole("button", { name: "Submit" }));
    expect(await screen.findByText("Replicas must be at most 20")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText(/Replicas/), { target: { value: "4" } });
    fireEvent.click(screen.getByRole("button", { name: "Submit" }));
    await waitFor(() => expect(onWidgetRequest).toHaveBeenCalledTimes(1));
    expect(onWidgetRequest.mock.calls[0][0]).toMatchObject({
      request: "scale",
      payload: { service: "api", replicas: 4, dry: true },
    });
    await waitFor(() => expect(screen.getByLabelText(/Replicas/)).toHaveValue(null));
  });

  it("button: variant, disabled and busy state while the Request is in flight", async () => {
    let release: () => void = () => undefined;
    const onWidgetRequest = vi.fn(() => new Promise<void>((r) => (release = r)));
    render(
      <WidgetRenderer
        adapterInstance="ad"
        generation={1}
        onWidgetRequest={onWidgetRequest}
        node={{
          widget_id: "ad_btn",
          widget_type: "dash.button",
          props: { label: "Rollback", request: "rollback", variant: "danger", payload: { to: "v1" } },
        }}
      />
    );
    const btn = screen.getByRole("button", { name: "Rollback" });
    expect(btn).toHaveClass("btn", "danger");
    fireEvent.click(btn);
    expect(await screen.findByRole("button", { name: "Running..." })).toBeDisabled();
    expect(onWidgetRequest).toHaveBeenCalledWith(expect.objectContaining({ request: "rollback", payload: { to: "v1" } }));
    await act(async () => release());
    expect(screen.getByRole("button", { name: "Rollback" })).toBeEnabled();
    cleanup();

    renderNode({ widget_id: "ad_b2", widget_type: "dash.button", props: { label: "Off", disabled: true } });
    expect(screen.getByRole("button", { name: "Off" })).toBeDisabled();
  });
});

describe("dash.filter", () => {
  it("follows Dashboard State and writes changes back", () => {
    const onDashboardStateChange = vi.fn().mockResolvedValue(undefined);
    const node: WidgetNode = {
      widget_id: "ad_repo",
      widget_type: "dash.filter",
      props: {
        label: "Repository",
        state_key: "selected_repository",
        options: ["acme/platform", { value: "acme/payments", label: "Payments" }],
        all_label: "All repositories",
      },
    };
    const { rerender } = render(
      <WidgetRenderer
        adapterInstance="ad"
        generation={1}
        onWidgetRequest={vi.fn()}
        onDashboardStateChange={onDashboardStateChange}
        dashboardState={{ selected_repository: "acme/platform" }}
        node={node}
      />
    );
    const select = screen.getByLabelText("Repository:") as HTMLSelectElement;
    expect(select.value).toBe("acme/platform");
    fireEvent.change(select, { target: { value: "acme/payments" } });
    expect(onDashboardStateChange).toHaveBeenCalledWith("selected_repository", "acme/payments");

    // Another Client changed the State: the control follows.
    rerender(
      <WidgetRenderer
        adapterInstance="ad"
        generation={2}
        onWidgetRequest={vi.fn()}
        onDashboardStateChange={onDashboardStateChange}
        dashboardState={{ selected_repository: "" }}
        node={node}
      />
    );
    expect((screen.getByLabelText("Repository:") as HTMLSelectElement).value).toBe("");
    expect(screen.getByRole("option", { name: "All repositories" })).toBeInTheDocument();
  });

  it("debounces search controls and supports multiselect and toggles", () => {
    vi.useFakeTimers();
    const onDashboardStateChange = vi.fn().mockResolvedValue(undefined);
    const common = {
      adapterInstance: "ad",
      generation: 1,
      onWidgetRequest: vi.fn(),
      onDashboardStateChange,
      dashboardState: {},
    };
    render(
      <>
        <WidgetRenderer {...common} node={{ widget_id: "f1", widget_type: "dash.filter", props: { label: "Search", state_key: "q", control: "search", debounce_ms: 200 } }} />
        <WidgetRenderer {...common} node={{ widget_id: "f2", widget_type: "dash.filter", props: { label: "Envs", state_key: "envs", control: "multiselect", options: ["dev", "prod"] } }} />
        <WidgetRenderer {...common} node={{ widget_id: "f3", widget_type: "dash.filter", props: { label: "Only mine", state_key: "mine", control: "toggle" } }} />
      </>
    );
    const search = screen.getByRole("searchbox");
    fireEvent.change(search, { target: { value: "a" } });
    fireEvent.change(search, { target: { value: "ab" } });
    expect(onDashboardStateChange).not.toHaveBeenCalled();
    act(() => vi.advanceTimersByTime(250));
    expect(onDashboardStateChange).toHaveBeenCalledTimes(1);
    expect(onDashboardStateChange).toHaveBeenLastCalledWith("q", "ab");

    fireEvent.click(screen.getByRole("checkbox", { name: "prod" }));
    expect(onDashboardStateChange).toHaveBeenLastCalledWith("envs", ["prod"]);
    fireEvent.click(screen.getByRole("checkbox", { name: "Only mine" }));
    expect(onDashboardStateChange).toHaveBeenLastCalledWith("mine", true);
  });
});

describe("dash.tabs", () => {
  const node: WidgetNode = {
    widget_id: "ad_tabs",
    widget_type: "dash.tabs",
    props: { title: "Views" },
    children: [
      { widget_id: "ad_a", key: "a", widget_type: "dash.text", props: { title: "Alpha", text: "A body" } },
      { widget_id: "ad_b", key: "b", widget_type: "dash.text", props: { title: "Beta", text: "B body" } },
      { widget_id: "ad_c", key: "c", widget_type: "dash.text", props: { title: "Gamma", text: "C body" } },
    ],
  };

  it("supports arrow/Home/End keys with a roving tabindex and remembers the tab", () => {
    const { unmount } = renderNode(node);
    const tabs = screen.getAllByRole("tab");
    expect(screen.getByRole("tablist", { name: "Views" })).toBeInTheDocument();
    expect(tabs.map((t) => t.tabIndex)).toEqual([0, -1, -1]);
    fireEvent.keyDown(tabs[0], { key: "ArrowLeft" });
    expect(screen.getByRole("tab", { name: "Gamma" })).toHaveAttribute("aria-selected", "true");
    expect(document.activeElement).toBe(screen.getByRole("tab", { name: "Gamma" }));
    fireEvent.keyDown(document.activeElement!, { key: "Home" });
    fireEvent.keyDown(document.activeElement!, { key: "ArrowRight" });
    expect(screen.getByRole("tab", { name: "Beta" })).toHaveAttribute("aria-selected", "true");
    expect(screen.getByText("B body")).toBeVisible();
    unmount();

    renderNode(node);
    expect(screen.getByRole("tab", { name: "Beta" })).toHaveAttribute("aria-selected", "true");
  });

  it("honors default_tab by key", () => {
    renderNode({ ...node, widget_id: "ad_tabs2", props: { default_tab: "c" } });
    expect(screen.getByRole("tab", { name: "Gamma" })).toHaveAttribute("aria-selected", "true");
  });
});

describe("error isolation", () => {
  it("contains a widget that throws while rendering", () => {
    const spy = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const evil = {
      get label() {
        throw new Error("boom");
      },
    };
    renderNode({
      widget_id: "ad_root",
      widget_type: "dash.container",
      children: [
        { widget_id: "ad_bad", widget_type: "dash.counter", props: evil as unknown as Record<string, unknown> },
        { widget_id: "ad_ok", widget_type: "dash.text", props: { text: "still here" } },
      ],
    });
    expect(screen.getByRole("alert")).toHaveTextContent("ad_bad (dash.counter): boom");
    expect(screen.getByText("still here")).toBeInTheDocument();
    spy.mockRestore();
  });
});
