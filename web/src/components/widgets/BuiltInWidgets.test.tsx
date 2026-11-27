import { describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { WidgetRenderer, resolveWidgetAssetUrl } from "./BuiltInWidgets";

describe("WidgetRenderer layout overrides and sandbox bridge", () => {
  it("applies x/y/h overrides in grids, h in containers and configurable prop overrides", () => {
    const { container } = render(
      <WidgetRenderer
        adapterInstance="a"
        generation={1}
        onWidgetRequest={vi.fn()}
        node={{
          widget_id: "a_root",
          widget_type: "dash.grid",
          props: { columns: 2 },
          children: [
            {
              widget_id: "a_counter",
              widget_type: "dash.counter",
              props: { label: "Adapter label", value: 3 },
              layout_override: { x: 6, y: 2, w: 6, h: 3, props: { label: "Admin label", value: 99 } },
            },
            {
              widget_id: "a_box",
              widget_type: "dash.container",
              children: [
                { widget_id: "a_text", widget_type: "dash.text", props: { text: "hi" }, layout_override: { h: 2 } },
              ],
            },
          ],
        }}
      />
    );
    const counter = container.querySelector('[data-widget-id="a_counter"]') as HTMLElement;
    expect(counter.style.gridColumn).toBe("7 / span 6");
    expect(counter.style.gridRow).toBe("3 / span 3");
    // Configurable prop override applies; non-configurable data props stay Adapter-owned.
    expect(screen.getByText("Admin label")).toBeInTheDocument();
    expect(screen.getByText("3")).toBeInTheDocument();
    const text = container.querySelector('[data-widget-id="a_text"]') as HTMLElement;
    expect(text.style.minHeight).toBe("96px");
  });

  it("reports resize drags in Designer mode as w/h overrides", () => {
    const onLayoutChange = vi.fn();
    const { container } = render(
      <WidgetRenderer
        adapterInstance="a"
        generation={1}
        designerMode
        onLayoutChange={onLayoutChange}
        onWidgetRequest={vi.fn()}
        node={{
          widget_id: "a_root",
          widget_type: "dash.grid",
          children: [{ widget_id: "a_text", widget_type: "dash.text", props: { text: "x" }, layout_override: { w: 4, h: 2 } }],
        }}
      />
    );
    const handle = screen.getByRole("button", { name: "Resize a_text" });
    fireEvent.pointerDown(handle, { clientX: 0, clientY: 0 });
    act(() => {
      window.dispatchEvent(new MouseEvent("pointermove", { clientX: 0, clientY: 100 }));
      window.dispatchEvent(new MouseEvent("pointerup", { clientX: 0, clientY: 100 }));
    });
    expect(onLayoutChange).toHaveBeenCalledWith("a_text", { w: 4, h: 4 });
    expect(container.querySelector('[data-widget-id="a_text"]')).not.toBeNull();
  });

  it("loads the declared widget asset and never trusts the frame's Generation", () => {
    const onReq = vi.fn().mockResolvedValue(undefined);
    render(
      <WidgetRenderer
        adapterInstance="pr_table_adapter"
        generation={5}
        onWidgetRequest={onReq}
        node={{
          widget_id: "pr_table_adapter_pr_summary",
          widget_type: "engineering.pr_badge_widget",
          asset_url: "/widgets/engineering.pr_badge_widget/assets/app.html",
          props: {},
        }}
      />
    );
    const frame = screen.getByTitle("Custom widget pr_table_adapter_pr_summary") as HTMLIFrameElement;
    expect(frame.getAttribute("src")).toBe("/widgets/engineering.pr_badge_widget/assets/app.html");

    const send = (data: Record<string, unknown>) =>
      act(() => {
        window.dispatchEvent(new MessageEvent("message", { data, source: frame.contentWindow }));
      });
    const base = { protocol: "dash.widget/1", kind: "request", widget_id: "pr_table_adapter_pr_summary", request: "refresh_requested" };
    send({ ...base, generation: 4 });
    expect(onReq).not.toHaveBeenCalled();
    send({ ...base, generation: 5, payload: { a: 1 } });
    send({ ...base });
    expect(onReq).toHaveBeenCalledTimes(2);
    expect(onReq.mock.calls.every(([ctx]) => ctx.generation === 5)).toBe(true);
    expect(onReq.mock.calls[0][0].payload).toEqual({ a: 1 });
  });

  it("sizes the frame from the declared frame_height unless the Dashboard overrides h", () => {
    const node = (extra: Record<string, unknown>) => ({
      widget_id: "w_glance",
      widget_type: "weather.glance_widget",
      props: {},
      ...extra,
    });
    const frame = () => screen.getByTitle("Custom widget w_glance") as HTMLIFrameElement;
    const { rerender } = render(
      <WidgetRenderer adapterInstance="w" generation={1} onWidgetRequest={vi.fn()} node={node({})} />
    );
    expect(frame().style.height).toBe("54px");
    rerender(<WidgetRenderer adapterInstance="w" generation={1} onWidgetRequest={vi.fn()} node={node({ frame_height: 460 })} />);
    expect(frame().style.height).toBe("460px");
    rerender(
      <WidgetRenderer
        adapterInstance="w"
        generation={1}
        onWidgetRequest={vi.fn()}
        node={node({ frame_height: 460, layout_override: { h: 6 } })}
      />
    );
    expect(frame().style.height).toBe("288px");
  });

  it("rejects asset URLs outside the provider's assets", () => {
    expect(resolveWidgetAssetUrl("x.w", "/widgets/other/assets/a.html")).toBe("/widgets/x.w/assets/index.html");
    expect(resolveWidgetAssetUrl("x.w", "/widgets/x.w/assets/../runtime.luau")).toBe("/widgets/x.w/assets/index.html");
    expect(resolveWidgetAssetUrl("x.w", "https://evil.example/a.html")).toBe("/widgets/x.w/assets/index.html");
  });
});
