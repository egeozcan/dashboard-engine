import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ListWidget, normalizeListItems, shortRelative } from "./ListWidget";

const NOW = Date.parse("2026-11-27T12:00:00Z");

describe("dash.list", () => {
  it("formats compact relative times", () => {
    expect(shortRelative(NOW - 20_000, NOW)).toBe("just now");
    expect(shortRelative(NOW - 5 * 60_000, NOW)).toBe("5m ago");
    expect(shortRelative(NOW - 14 * 3600_000, NOW)).toBe("14h ago");
    expect(shortRelative(NOW - 16 * 86400_000, NOW)).toBe("16d ago");
    expect(shortRelative(NOW - 240 * 86400_000, NOW)).toBe("7mo ago");
    expect(shortRelative(Date.parse("2012-10-07T00:00:00Z"), NOW)).toBe("14y ago");
    expect(shortRelative(NOW + 3 * 86400_000, NOW)).toBe("in 3d");
  });

  it("drops untitled items and unsafe links", () => {
    const items = normalizeListItems([
      { title: "ok", url: "https://github.com/a/b", marker: "dot", time: "2026-11-27T00:00:00Z" },
      { title: "bad link", url: "javascript:alert(1)", marker: "rocket" },
      { url: "https://x" },
      "nope",
    ]);
    expect(items.map((i) => i.title)).toEqual(["ok", "bad link"]);
    expect(items[0]).toMatchObject({ url: "https://github.com/a/b", marker: "dot", time: Date.parse("2026-11-27T00:00:00Z") });
    expect(items[1]).toMatchObject({ url: undefined, marker: "none" });
  });

  it("renders each item as one link with its subtitle, meta and footer", () => {
    render(
      <ListWidget
        title="Open pull requests"
        subtitle="GitHub"
        footer="Refreshes every 10 minutes"
        items={normalizeListItems([
          {
            title: "adds inlineextend",
            url: "https://github.com/flosch/pongo2/pull/360",
            subtitle: "flosch/pongo2 #360",
            marker: "pull_request",
            meta: "Opened",
            time: new Date(Date.now() - 16 * 86400_000).toISOString(),
          },
          { title: "no link", subtitle: "5 commits since", subtitle_time: "2026-11-20T04:54:00Z" },
        ])}
      />
    );
    const list = screen.getByRole("list", { name: "Open pull requests" });
    const link = within(list).getByRole("link");
    expect(link).toHaveAttribute("href", "https://github.com/flosch/pongo2/pull/360");
    expect(link).toHaveAttribute("rel", "noopener noreferrer");
    expect(link).toHaveTextContent("adds inlineextend");
    expect(link).toHaveTextContent("Opened");
    expect(link).toHaveTextContent("16d ago");
    expect(screen.getByText(/^5 commits since \S/)).toBeInTheDocument();
    expect(screen.getByText("Refreshes every 10 minutes")).toBeInTheDocument();
  });

  it("shows the empty message", () => {
    render(<ListWidget title="Recent activity" items={[]} emptyMessage="No activity this week" />);
    expect(screen.getByText("No activity this week")).toBeInTheDocument();
  });
});
