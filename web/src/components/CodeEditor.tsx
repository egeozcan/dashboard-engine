import React, { useEffect, useId, useMemo, useRef, useState } from "react";
import { filterCompletions, tokenBeforeCursor, type Diagnostic } from "../lib/luau";

/**
 * Luau source editor for Package Studio: line-number gutter with inline diagnostic markers,
 * and autocomplete (type to filter; Ctrl+Space to open; arrows + Enter/Tab to accept; Esc to
 * close) fed by generated Luau type declarations and the dash SDK.
 */
export const CodeEditor: React.FC<{
  label: string;
  value: string;
  onChange: (next: string) => void;
  completions: string[];
  diagnostics: Diagnostic[];
  readOnly?: boolean;
  minRows?: number;
  /** Increment to move the caret to `focusLine` (1-based). */
  focusRequest?: { line: number; seq: number } | null;
}> = ({ label, value, onChange, completions, diagnostics, readOnly, minRows = 8, focusRequest }) => {
  const taRef = useRef<HTMLTextAreaElement>(null);
  const gutterRef = useRef<HTMLDivElement>(null);
  const listId = useId();
  const [suggest, setSuggest] = useState<{ items: string[]; start: number; index: number } | null>(null);
  /** Set by Escape (no completion list open): the next Tab leaves the editor (WCAG 2.1.2). */
  const escapedRef = useRef(false);
  const hintId = useId();

  const lines = useMemo(() => Math.max(minRows, value.split("\n").length), [value, minRows]);
  const diagByLine = useMemo(() => {
    const m = new Map<number, Diagnostic[]>();
    for (const d of diagnostics) {
      if (d.line === null) continue;
      m.set(d.line, [...(m.get(d.line) ?? []), d]);
    }
    return m;
  }, [diagnostics]);

  useEffect(() => {
    if (!focusRequest || !taRef.current) return;
    const ta = taRef.current;
    const parts = value.split("\n");
    const ln = Math.min(Math.max(1, focusRequest.line), parts.length);
    const offset = parts.slice(0, ln - 1).reduce((acc, l) => acc + l.length + 1, 0);
    ta.focus();
    ta.setSelectionRange(offset, offset + (parts[ln - 1]?.length ?? 0));
  }, [focusRequest?.seq]);

  const refreshSuggest = (text: string, cursor: number, force = false) => {
    const { token, start } = tokenBeforeCursor(text, cursor);
    if (!force && token.length < 2) {
      setSuggest(null);
      return;
    }
    const items = filterCompletions(completions, token);
    setSuggest(items.length ? { items, start, index: 0 } : null);
  };

  const accept = (item: string) => {
    const ta = taRef.current;
    if (!ta || !suggest || readOnly) return;
    const cursor = ta.selectionStart;
    const next = value.slice(0, suggest.start) + item + value.slice(cursor);
    onChange(next);
    setSuggest(null);
    const pos = suggest.start + item.length;
    requestAnimationFrame(() => {
      ta.focus();
      ta.setSelectionRange(pos, pos);
    });
  };

  return (
    <div className="code-editor">
      <div className="code-editor-body">
        <div ref={gutterRef} className="code-gutter" aria-hidden="true">
          {Array.from({ length: lines }, (_, i) => {
            const ds = diagByLine.get(i + 1);
            return (
              <div
                key={i}
                className={ds ? "gutter-line has-diag" : "gutter-line"}
                title={ds?.map((d) => d.message).join("\n")}
              >
                {ds ? "●" : ""}
                {i + 1}
              </div>
            );
          })}
        </div>
        <textarea
          ref={taRef}
          className="code"
          aria-label={label}
          aria-autocomplete="list"
          aria-controls={suggest ? listId : undefined}
          aria-expanded={Boolean(suggest)}
          aria-invalid={diagnostics.length > 0 || undefined}
          aria-describedby={readOnly ? undefined : hintId}
          spellCheck={false}
          readOnly={readOnly}
          rows={lines}
          value={value}
          onScroll={(e) => {
            if (gutterRef.current) gutterRef.current.scrollTop = e.currentTarget.scrollTop;
          }}
          onChange={(e) => {
            escapedRef.current = false;
            onChange(e.target.value);
            refreshSuggest(e.target.value, e.target.selectionStart);
          }}
          onBlur={() => {
            escapedRef.current = false;
            setTimeout(() => setSuggest(null), 150);
          }}
          onKeyDown={(e) => {
            if (e.key !== "Tab" && e.key !== "Shift" && e.key !== "Escape") escapedRef.current = false;
            if (e.key === " " && e.ctrlKey) {
              e.preventDefault();
              if (!readOnly) refreshSuggest(value, e.currentTarget.selectionStart, true);
              return;
            }
            if (!suggest) {
              if (e.key === "Escape") {
                // Release keyboard focus: the next Tab moves on instead of indenting.
                escapedRef.current = true;
                return;
              }
              // Shift+Tab always moves focus back; Tab indents unless the user pressed Escape.
              if (e.key === "Tab" && !e.shiftKey && !readOnly && !escapedRef.current) {
                e.preventDefault();
                const ta = e.currentTarget;
                const s = ta.selectionStart;
                onChange(value.slice(0, s) + "    " + value.slice(ta.selectionEnd));
                requestAnimationFrame(() => ta.setSelectionRange(s + 4, s + 4));
              }
              return;
            }
            if (e.key === "ArrowDown" || e.key === "ArrowUp") {
              e.preventDefault();
              const delta = e.key === "ArrowDown" ? 1 : -1;
              setSuggest({
                ...suggest,
                index: (suggest.index + delta + suggest.items.length) % suggest.items.length,
              });
            } else if (e.key === "Enter" || e.key === "Tab") {
              e.preventDefault();
              accept(suggest.items[suggest.index]);
            } else if (e.key === "Escape") {
              e.preventDefault();
              setSuggest(null);
            }
          }}
        />
      </div>
      {!readOnly && (
        <div id={hintId} className="code-editor-hint" style={{ fontSize: 11, color: "var(--text-muted)" }}>
          Tab indents; press Esc then Tab (or Shift+Tab) to leave the editor.
        </div>
      )}
      {suggest && (
        <ul id={listId} role="listbox" aria-label={`${label} completions`} className="completion-list">
          {suggest.items.map((item, i) => (
            <li
              key={item}
              role="option"
              aria-selected={i === suggest.index}
              onMouseDown={(e) => {
                e.preventDefault();
                accept(item);
              }}
            >
              {item}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
};
