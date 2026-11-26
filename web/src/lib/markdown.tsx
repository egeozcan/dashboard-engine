/**
 * Minimal, safe Markdown subset for `dash.text` (`markdown = true`).
 *
 * Supported: headings (`#`–`####`), paragraphs, `-`/`*` and numbered lists, `>` quotes, fenced
 * code blocks, `---` rules, and inline `code`, **bold**, *italic* or _italic_, and [links](url).
 * Output is built from React elements only (never `innerHTML`), so Adapter-supplied text cannot
 * inject markup; link targets pass `safeHref` (http(s), mailto, same-origin paths) or render as
 * plain text.
 */
import React from "react";
import { safeHref } from "./widgetFormat";

const INLINE_RE =
  /(`[^`\n]+`)|(\*\*[^*\n]+\*\*)|(\[[^\]\n]+\]\([^)\s]+\))|(\*[^*\s][^*\n]*\*)|(_[^_\s][^_\n]*_)/;

export function renderInline(text: string, keyPrefix = "i"): React.ReactNode[] {
  const out: React.ReactNode[] = [];
  let rest = text;
  let n = 0;
  while (rest.length > 0) {
    const m = INLINE_RE.exec(rest);
    if (!m) {
      out.push(rest);
      break;
    }
    if (m.index > 0) out.push(rest.slice(0, m.index));
    const tok = m[0];
    const key = `${keyPrefix}-${n++}`;
    if (m[1]) {
      out.push(<code key={key}>{tok.slice(1, -1)}</code>);
    } else if (m[2]) {
      out.push(<strong key={key}>{renderInline(tok.slice(2, -2), key)}</strong>);
    } else if (m[3]) {
      const close = tok.indexOf("](");
      const label = tok.slice(1, close);
      const href = safeHref(tok.slice(close + 2, -1));
      if (href) {
        const external = !href.startsWith("/");
        out.push(
          <a
            key={key}
            href={href}
            {...(external ? { target: "_blank", rel: "noopener noreferrer" } : {})}
          >
            {renderInline(label, key)}
          </a>
        );
      } else {
        out.push(<React.Fragment key={key}>{renderInline(label, key)}</React.Fragment>);
      }
    } else {
      out.push(<em key={key}>{renderInline(tok.slice(1, -1), key)}</em>);
    }
    rest = rest.slice(m.index + tok.length);
  }
  return out;
}

type Block =
  | { kind: "p"; text: string }
  | { kind: "h"; level: number; text: string }
  | { kind: "ul" | "ol"; items: string[] }
  | { kind: "quote"; text: string }
  | { kind: "code"; text: string }
  | { kind: "hr" };

export function parseBlocks(src: string): Block[] {
  const lines = src.replace(/\r\n?/g, "\n").split("\n");
  const blocks: Block[] = [];
  let para: string[] = [];
  const flush = () => {
    if (para.length) blocks.push({ kind: "p", text: para.join(" ") });
    para = [];
  };
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmed = line.trim();
    if (trimmed.startsWith("```")) {
      flush();
      const body: string[] = [];
      i += 1;
      while (i < lines.length && !lines[i].trim().startsWith("```")) {
        body.push(lines[i]);
        i += 1;
      }
      blocks.push({ kind: "code", text: body.join("\n") });
      continue;
    }
    if (trimmed === "") {
      flush();
      continue;
    }
    if (/^(-{3,}|\*{3,}|_{3,})$/.test(trimmed)) {
      flush();
      blocks.push({ kind: "hr" });
      continue;
    }
    const h = /^(#{1,4})\s+(.*)$/.exec(trimmed);
    if (h) {
      flush();
      blocks.push({ kind: "h", level: h[1].length, text: h[2] });
      continue;
    }
    const ul = /^[-*+]\s+(.*)$/.exec(trimmed);
    const ol = /^\d+[.)]\s+(.*)$/.exec(trimmed);
    if (ul || ol) {
      flush();
      const kind = ul ? "ul" : "ol";
      const last = blocks[blocks.length - 1];
      const item = (ul ?? ol)![1];
      if (last && last.kind === kind) last.items.push(item);
      else blocks.push({ kind, items: [item] });
      continue;
    }
    const q = /^>\s?(.*)$/.exec(trimmed);
    if (q) {
      flush();
      const last = blocks[blocks.length - 1];
      if (last && last.kind === "quote") last.text += ` ${q[1]}`;
      else blocks.push({ kind: "quote", text: q[1] });
      continue;
    }
    para.push(trimmed);
  }
  flush();
  return blocks;
}

export const Markdown: React.FC<{ text: string }> = ({ text }) => (
  <div className="md">
    {parseBlocks(text).map((b, i) => {
      const key = `b${i}`;
      switch (b.kind) {
        case "h": {
          const Tag = (["h3", "h4", "h5", "h6"] as const)[b.level - 1];
          return <Tag key={key}>{renderInline(b.text, key)}</Tag>;
        }
        case "ul":
        case "ol": {
          const Tag = b.kind;
          return (
            <Tag key={key}>
              {b.items.map((it, j) => (
                <li key={j}>{renderInline(it, `${key}-${j}`)}</li>
              ))}
            </Tag>
          );
        }
        case "quote":
          return <blockquote key={key}>{renderInline(b.text, key)}</blockquote>;
        case "code":
          return (
            <pre key={key} className="json-block">
              <code>{b.text}</code>
            </pre>
          );
        case "hr":
          return <hr key={key} />;
        default:
          return <p key={key}>{renderInline(b.text, key)}</p>;
      }
    })}
  </div>
);
