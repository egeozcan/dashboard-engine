// Built-in `dash.table`: typed/formatted columns, client-side search + sort (Client State),
// pagination, CSV export and per-row action Requests (responsive-table + sortable-table guides).
import React, { useEffect, useId, useMemo, useState } from "react";
import { useClientState } from "../../lib/clientState";
import {
  EMPTY_CELL,
  formatOptionsFrom,
  formatValue,
  parseTime,
  safeHref,
  statusTone,
  toNumber,
  toneClass,
  type FormatOptions,
  type Tone,
} from "../../lib/widgetFormat";

export interface TableColumn {
  key: string;
  label: string;
  /** `text` (default), `number`, `date`, `datetime`, `badge`/`status`, `link`, `boolean`. */
  type: string;
  fmt: FormatOptions;
  align: "left" | "right" | "center";
  width?: string;
  sortable: boolean;
  badges?: Record<string, string>;
  hrefKey?: string;
  maxChars?: number;
}

export interface RowAction {
  label: string;
  request: string;
  variant?: string;
  disabledKey?: string;
  hiddenKey?: string;
}

type SortDir = "ascending" | "descending";
export type SortState = { key: string; dir: SortDir } | null;

const DEFAULT_PAGE_SIZE = 50;
const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: "base" });

const TYPE_FORMATS: Record<string, string> = {
  number: "number",
  date: "date",
  datetime: "datetime",
  boolean: "boolean",
};

/** Column specs from the untrusted `columns` prop (entries without a string `key` are dropped). */
export function normalizeColumns(raw: unknown): TableColumn[] {
  if (!Array.isArray(raw)) return [];
  const out: TableColumn[] = [];
  for (const c of raw) {
    if (!c || typeof c !== "object") continue;
    const r = c as Record<string, unknown>;
    if (typeof r.key !== "string" || !r.key || r.hidden === true) continue;
    const type = typeof r.type === "string" ? r.type : "text";
    const fmt = formatOptionsFrom(r);
    if (!fmt.format && TYPE_FORMATS[type]) fmt.format = TYPE_FORMATS[type];
    const numeric = type === "number" || ["number", "integer", "compact", "percent", "currency", "bytes", "duration", "duration_ms"].includes(fmt.format ?? "");
    const align = r.align === "right" || r.align === "center" || r.align === "left" ? r.align : numeric ? "right" : "left";
    out.push({
      key: r.key,
      label: typeof r.label === "string" ? r.label : r.key,
      type: numeric && type === "text" ? "number" : type,
      fmt,
      align,
      width: typeof r.width === "number" ? `${r.width}px` : typeof r.width === "string" ? r.width : undefined,
      sortable: r.sortable !== false,
      badges:
        r.badges && typeof r.badges === "object" && !Array.isArray(r.badges)
          ? (r.badges as Record<string, string>)
          : undefined,
      hrefKey: typeof r.href_key === "string" ? r.href_key : undefined,
      maxChars: typeof r.max_chars === "number" && r.max_chars > 0 ? r.max_chars : undefined,
    });
  }
  return out;
}

/** `row_actions` (several) or the single legacy `row_action`. */
export function normalizeRowActions(rowActions: unknown, rowAction: unknown): RowAction[] {
  const list = Array.isArray(rowActions) ? rowActions : rowAction ? [rowAction] : [];
  const out: RowAction[] = [];
  for (const a of list) {
    if (!a || typeof a !== "object") continue;
    const r = a as Record<string, unknown>;
    const request = typeof r.request === "string" && r.request ? r.request : "action";
    out.push({
      label: typeof r.label === "string" ? r.label : request,
      request,
      variant: typeof r.variant === "string" ? r.variant : undefined,
      disabledKey: typeof r.disabled_key === "string" ? r.disabled_key : undefined,
      hiddenKey: typeof r.hidden_key === "string" ? r.hidden_key : undefined,
    });
  }
  return out;
}

function isEmpty(v: unknown): boolean {
  return v === null || v === undefined || v === "";
}

/** Sort key of a cell for its column type (numbers, timestamps, or collated text). */
function compareCells(col: TableColumn | undefined, av: unknown, bv: unknown): number {
  const kind = col?.type ?? "text";
  if (kind === "number" || (typeof av === "number" && typeof bv === "number")) {
    const an = toNumber(av);
    const bn = toNumber(bv);
    if (an !== null && bn !== null) return an - bn;
  }
  if (kind === "date" || kind === "datetime" || col?.fmt.format === "relative") {
    const at = parseTime(av);
    const bt = parseTime(bv);
    if (at !== null && bt !== null) return at - bt;
  }
  if (typeof av === "boolean" || typeof bv === "boolean") return Number(Boolean(av)) - Number(Boolean(bv));
  return collator.compare(String(av), String(bv));
}

/** Rows matching the search text (visible columns, raw or formatted), sorted; empty cells last. */
export function filterAndSortRows(
  rows: Array<Record<string, unknown>>,
  columns: TableColumn[],
  query: string,
  sort: SortState
): Array<Record<string, unknown>> {
  const q = query.trim().toLowerCase();
  const base = q
    ? rows.filter((r) =>
        columns.some((c) => {
          const v = r[c.key];
          if (isEmpty(v)) return false;
          const raw = typeof v === "object" ? JSON.stringify(v) : String(v);
          return raw.toLowerCase().includes(q) || formatValue(v, c.fmt).toLowerCase().includes(q);
        })
      )
    : [...rows];
  if (sort) {
    const col = columns.find((c) => c.key === sort.key);
    const factor = sort.dir === "ascending" ? 1 : -1;
    base.sort((a, b) => {
      const av = a[sort.key];
      const bv = b[sort.key];
      if (isEmpty(av) || isEmpty(bv)) return isEmpty(av) === isEmpty(bv) ? 0 : isEmpty(av) ? 1 : -1;
      return factor * compareCells(col, av, bv);
    });
  }
  return base;
}

function csvCell(v: unknown): string {
  if (isEmpty(v)) return "";
  let s = typeof v === "object" ? JSON.stringify(v) : String(v);
  // Spreadsheet formula injection: text starting with = + - @ is neutralized.
  if (typeof v === "string" && /^[=+\-@\t\r]/.test(s)) s = `'${s}`;
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

export function rowsToCsv(rows: Array<Record<string, unknown>>, columns: TableColumn[]): string {
  const head = columns.map((c) => csvCell(c.label)).join(",");
  const body = rows.map((r) => columns.map((c) => csvCell(r[c.key])).join(","));
  return [head, ...body].join("\r\n");
}

function downloadCsv(filename: string, csv: string) {
  if (typeof URL.createObjectURL !== "function") return;
  const url = URL.createObjectURL(new Blob([csv], { type: "text/csv;charset=utf-8" }));
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

function rowKeys(rows: Array<Record<string, unknown>>, rowKey?: string): string[] {
  const seen = new Map<string, number>();
  return rows.map((row, idx) => {
    const raw = rowKey ? row[rowKey] : row.id ?? row.key ?? row.number;
    const base = isEmpty(raw) ? `#${idx}` : String(raw);
    const n = seen.get(base) ?? 0;
    seen.set(base, n + 1);
    return n === 0 ? base : `${base}~${n}`;
  });
}

const Cell: React.FC<{ col: TableColumn; row: Record<string, unknown> }> = ({ col, row }) => {
  const v = row[col.key];
  if (isEmpty(v)) return <>{EMPTY_CELL}</>;
  let text = formatValue(v, col.fmt);
  let title: string | undefined;
  if (col.maxChars && text.length > col.maxChars) {
    title = text;
    text = `${text.slice(0, col.maxChars - 1)}…`;
  }
  if (col.type === "badge" || col.type === "status") {
    const mapped = col.badges?.[String(v)];
    const tone: Tone = mapped ? statusTone(mapped) : statusTone(v);
    return (
      <span className={`badge ${toneClass(tone)}`} title={title}>
        {text}
      </span>
    );
  }
  if (col.type === "link") {
    const href = safeHref(col.hrefKey ? row[col.hrefKey] : v);
    if (href) {
      const external = !href.startsWith("/");
      return (
        <a href={href} title={title} {...(external ? { target: "_blank", rel: "noopener noreferrer" } : {})}>
          {text}
        </a>
      );
    }
  }
  return title ? <span title={title}>{text}</span> : <>{text}</>;
};

export const TableWidget: React.FC<{
  title: string;
  columns: TableColumn[];
  rows: Array<Record<string, unknown>>;
  sortStateKey: string;
  filterStateKey: string;
  defaultSort?: SortState;
  searchable?: boolean;
  pageSize?: number;
  emptyMessage?: string;
  exportable?: boolean;
  rowKey?: string;
  rowActions: RowAction[];
  onRowAction: (action: RowAction, row: Record<string, unknown>) => Promise<void>;
}> = ({
  title,
  columns,
  rows,
  sortStateKey,
  filterStateKey,
  defaultSort = null,
  searchable = true,
  pageSize = DEFAULT_PAGE_SIZE,
  emptyMessage = "No rows",
  exportable = false,
  rowKey,
  rowActions,
  onRowAction,
}) => {
  const captionId = useId();
  const [storedSort, setSortState] = useClientState<SortState>(sortStateKey, null);
  const [filterText, setFilterText] = useClientState<string>(filterStateKey, "");
  const sortState = storedSort ?? defaultSort;
  const [statusMsg, setStatusMsg] = useState("");
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const query = searchable ? filterText : "";

  const filtered = useMemo(
    () => filterAndSortRows(rows, columns, query, sortState),
    [rows, columns, query, sortState]
  );
  const paged = pageSize > 0 && filtered.length > pageSize;
  const pageCount = paged ? Math.ceil(filtered.length / pageSize) : 1;
  const currentPage = Math.min(page, pageCount - 1);
  const pageRows = paged ? filtered.slice(currentPage * pageSize, (currentPage + 1) * pageSize) : filtered;
  const keys = useMemo(() => rowKeys(pageRows, rowKey), [pageRows, rowKey]);

  // A new search or sort starts from the first page.
  useEffect(() => setPage(0), [query, sortState?.key, sortState?.dir]);

  const toggleSort = (key: string, label: string) => {
    const nextDir: SortDir = sortState?.key === key && sortState.dir === "ascending" ? "descending" : "ascending";
    setSortState({ key, dir: nextDir });
    setStatusMsg(`Sorted by ${label}, ${nextDir}, ${filtered.length} rows`);
  };

  const countLabel =
    filtered.length === rows.length ? `${rows.length} rows` : `${filtered.length} of ${rows.length} rows`;

  return (
    <div className="card dash-table" style={{ marginBottom: 0 }}>
      <div className="row" style={{ justifyContent: "space-between", marginBottom: 10 }}>
        <div className="row" style={{ gap: 8 }}>
          <strong id={captionId}>{title}</strong>
          <span className="badge" aria-label="Row count">
            {countLabel}
          </span>
        </div>
        <div className="row" style={{ gap: 8 }}>
          {searchable && (
            <label className="row" style={{ fontSize: 12 }}>
              <span>Client filter:</span>
              <input
                type="search"
                aria-label={`Filter ${title}`}
                value={filterText}
                onChange={(e) => setFilterText(e.target.value)}
                placeholder="Search rows..."
              />
            </label>
          )}
          {exportable && (
            <button
              type="button"
              className="btn"
              onClick={() =>
                downloadCsv(`${title.replace(/[^\w-]+/g, "_").slice(0, 60) || "table"}.csv`, rowsToCsv(filtered, columns))
              }
            >
              Export CSV
            </button>
          )}
        </div>
      </div>
      <div role="status" className="sr-only">
        {statusMsg}
      </div>
      <div tabIndex={0} role="region" aria-labelledby={captionId} className="table-wrapper">
        <table>
          <caption className="sr-only">{title}</caption>
          <thead>
            <tr>
              {columns.map((c) => (
                <th
                  key={c.key}
                  scope="col"
                  aria-sort={sortState?.key === c.key ? sortState.dir : undefined}
                  style={{ width: c.width, textAlign: c.align }}
                >
                  {c.sortable ? (
                    <button type="button" onClick={() => toggleSort(c.key, c.label)}>
                      {c.label}
                    </button>
                  ) : (
                    c.label
                  )}
                </th>
              ))}
              {rowActions.length > 0 && <th scope="col">{rowActions.length > 1 ? "Actions" : "Action"}</th>}
            </tr>
          </thead>
          <tbody>
            {pageRows.map((row, idx) => {
              const rk = keys[idx];
              return (
                <tr key={rk}>
                  {columns.map((c) => (
                    <td key={c.key} data-label={c.label} style={{ textAlign: c.align }}>
                      <Cell col={c} row={row} />
                    </td>
                  ))}
                  {rowActions.length > 0 && (
                    <td data-label={rowActions.length > 1 ? "Actions" : "Action"}>
                      <div className="row" style={{ gap: 6, flexWrap: "nowrap" }}>
                        {rowActions
                          .filter((a) => !(a.hiddenKey && row[a.hiddenKey]))
                          .map((a) => {
                            const busy = busyKey === `${rk}\u0000${a.request}`;
                            return (
                              <button
                                key={a.request}
                                type="button"
                                className={`btn ${a.variant === "danger" ? "danger" : a.variant === "primary" ? "primary" : ""}`}
                                disabled={
                                  (busyKey !== null && busyKey.startsWith(`${rk}\u0000`)) ||
                                  Boolean(a.disabledKey && row[a.disabledKey])
                                }
                                onClick={async () => {
                                  setBusyKey(`${rk}\u0000${a.request}`);
                                  try {
                                    await onRowAction(a, row);
                                  } finally {
                                    setBusyKey(null);
                                  }
                                }}
                              >
                                {busy ? "Running..." : a.label}
                              </button>
                            );
                          })}
                      </div>
                    </td>
                  )}
                </tr>
              );
            })}
            {filtered.length === 0 && (
              <tr>
                <td colSpan={columns.length + (rowActions.length > 0 ? 1 : 0)}>
                  {rows.length === 0 ? emptyMessage : "No matching rows"}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      {paged && (
        <nav className="row" aria-label={`${title} pages`} style={{ justifyContent: "space-between", marginTop: 8, fontSize: 12 }}>
          <span>
            Showing {currentPage * pageSize + 1}–{Math.min(filtered.length, (currentPage + 1) * pageSize)} of{" "}
            {filtered.length}
          </span>
          <div className="row" style={{ gap: 6 }}>
            <button type="button" className="btn" disabled={currentPage === 0} onClick={() => setPage(currentPage - 1)}>
              Previous
            </button>
            <span>
              Page {currentPage + 1} of {pageCount}
            </span>
            <button
              type="button"
              className="btn"
              disabled={currentPage >= pageCount - 1}
              onClick={() => setPage(currentPage + 1)}
            >
              Next
            </button>
          </div>
        </nav>
      )}
    </div>
  );
};
