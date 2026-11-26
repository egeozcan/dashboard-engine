# Built-in widget providers and `dash.history_series`

The engine ships 11 host-rendered widget providers (`dash.*`) and one data provider. Their
contracts live in [`builtins/catalog.json`](../builtins/catalog.json). That file is compiled into
the binary, so rebuild after you edit it.

- **Props contracts are enforced.** When an Adapter renders a node with props that break its
  widget's contract (for example a column without a `key`, `rows` that isn't a list, or an
  unknown `kind`), that node becomes a `dash.text` error node with a
  `WIDGET_CONTRACT_VIOLATION` diagnostic. The rest of the tree still renders. Every prop is
  optional unless marked **required**, and unknown extra props are allowed.
- **Autocomplete.** Each built-in's generated Luau types include `export type WidgetProps`, which
  you can see in Package Studio and with `dash manifest dash.table`.
- **Isolation.** Each widget node renders inside its own error boundary. If the data crashes a
  renderer, the error replaces only that widget.
- **Requests.** `dash.button`, `dash.form`, `dash.table` and `dash.map` send the Request names
  their Adapter configures. All other built-ins never send Requests. Every Request goes through
  the Adapter's `on_request` and the action's Dry run, like any other widget Request.
- **Untrusted text.** Adapter strings always render as text. Links only accept `http(s)`,
  `mailto` and same-origin paths starting with `/`. Colors only accept hex, named, `rgb()`/`hsl()`
  and `var(--…)` values. Markdown is a fixed subset that is turned into React elements, never into
  `innerHTML`.

## Value formatting (shared)

`dash.table` columns, `dash.counter`, `dash.chart` and `dash.map` take the same formatting props:

| Prop | Meaning |
| --- | --- |
| `format` | `number`, `integer`, `compact` (`1.2K`), `percent` (the value is a ratio: `0.25` → `25%`), `currency`, `bytes`, `duration` (seconds), `duration_ms`, `date`, `datetime`, `time`, `relative` (`3 minutes ago`), `boolean`, `text` |
| `decimals` | Fraction digits (0–10) |
| `currency` | ISO 4217 code for `currency` (default `USD`) |
| `unit` / `prefix` | Text appended or prepended. `%` and units starting with `/` attach without a space. |

Values that don't fit their format (for example `"n/a"` under `number`) render unchanged.
Missing values render as `—`. Dates use the viewer's locale and time zone.

Status words map to colors: `ok`/`healthy`/`success`/`up` → green, `warning`/`degraded`/`pending`
→ amber, `critical`/`error`/`failed`/`down` → red, `info`/`running` → blue. Any other word is
shown in a neutral color.

## `dash.table`

| Prop | Type | Notes |
| --- | --- | --- |
| `title` | string | |
| `columns` | list of column | See below |
| `rows` | list of records | |
| `row_key` | string | Field used as the stable row identity. Default: `id`, then `key`, then `number`, then the position. |
| `default_sort` | `{ key, dir = "asc"\|"desc" }` | Applies until the viewer picks a sort. |
| `searchable` | boolean | Default `true`. Search covers the raw and formatted values of visible columns only. |
| `page_size` | number | Default `50`. `0` shows every row. |
| `empty_message` | string | Shown when `rows` is empty. |
| `exportable` | boolean | Adds **Export CSV** for the filtered rows. Cells that could run as spreadsheet formulas are neutralized. |
| `row_action` / `row_actions` | `{ label, request` (**required**)`, variant, disabled_key, hidden_key }` | The Request payload is the row. `disabled_key`/`hidden_key` name row fields that disable or hide the button. |
| `client_sort_state_key`, `client_filter_state_key` | string | Client State keys, namespaced per Dashboard and `widget_id`. |

A column is `{ key` (**required**)`, label, type, format, decimals, currency, unit, prefix, align,
width, hidden, sortable, badges, href_key, max_chars }`. `type` takes these values:

- `text` (the default).
- `number`: right-aligned and sorted numerically.
- `date` or `datetime`: sorted by time.
- `badge` or `status`: rendered as a colored badge. `badges` maps cell values to status words, for
  example `{ merged = "ok", closed = "danger" }`.
- `link`: links to the cell value, or to the field named by `href_key`.
- `boolean`.

Empty cells always sort last, whichever direction you sort in.

```lua
{
    key = "services",
    widget_type = "dash.table",
    props = {
        title = "Services",
        columns = {
            { key = "name", label = "Service", type = "link", href_key = "dashboard_url" },
            { key = "status", label = "Status", type = "badge" },
            { key = "p99_ms", label = "p99", format = "duration_ms" },
            { key = "error_rate", label = "Errors", format = "percent", decimals = 2 },
            { key = "deployed_at", label = "Deployed", format = "relative" },
        },
        rows = rows,
        default_sort = { key = "error_rate", dir = "desc" },
        page_size = 25,
        exportable = true,
        row_actions = {
            { label = "Restart", request = "restart_requested", variant = "danger", disabled_key = "restarting" },
            { label = "Silence", request = "silence_requested" },
        },
    },
}
```

## `dash.chart`

| Prop | Type | Notes |
| --- | --- | --- |
| `series` | list of records | The rows to plot. `dash.history_series` output works as-is. |
| `x_key` | string | Default `sampled_at`. |
| `y_key` / `y_keys` | string / list | One series, or several. Each `y_keys` entry is a string or `{ key, label, color }`. |
| `kind` | `line` \| `area` \| `bar` | Default `line`. Bars are grouped per x value. |
| `x_format` | `auto` \| `time` \| `number` \| `category` | `auto` treats ISO timestamp strings as time and numbers as linear. Epoch numbers need `time`. |
| `y_min`, `y_max`, `zero_baseline` | number, number, boolean | The y axis includes 0 unless `zero_baseline = false` on a line chart. |
| `thresholds` | list of `{ value` (**required**)`, label, status, color }` | Dashed horizontal lines, for example SLOs. |
| `height`, `show_legend`, `empty_message`, `series_label` | | The legend shows automatically when there are several series. |
| formatting props | | Apply to tooltips and the summary. Axis ticks use the compact form. |

Values that aren't numbers leave gaps in lines. Hovering the chart, or focusing it and pressing
←/→/Home/End, shows a tooltip. The SVG carries a text summary for screen readers (points, latest,
min and max per series).

## `dash.map`

`points` is a list of records with `lat`, and `lon` or `lng`, plus optional `label`, `status`,
`color` and `value`. Points with coordinates out of range are dropped and counted in an
"invalid" badge.

- **Fitting.** `fit = true` (the default) zooms to the points. `false` shows the whole world.
- **Markers.** Marker color follows `status`, or `color` if set. Marker area scales with `value`.
- **Markers as buttons.** Set `point_action = { label, request` (**required**) `}` to make every
  marker a keyboard-reachable button that sends the Request with the point as its payload.
- **No basemap tiles.** The shell's CSP allows no third-party network, so the map draws no tiles.
  Use a custom widget provider for street-level maps.

## `dash.counter`

| Prop | Notes |
| --- | --- |
| `label`, `value`, formatting props | |
| `status` | An explicit status word. Without one, `thresholds = { warning, critical, direction = "above"\|"below" }` derives `healthy`/`warning`/`critical`. Without either, no badge is shown. |
| `previous` / `delta` | Shows the change vs `previous` (a percent by default, `delta_format = "absolute"` for the plain difference), or an Adapter-computed `delta`. `higher_is_better = false` turns increases red. `delta_label` adds context such as `"vs last week"`. |
| `sparkline` | List of numbers drawn as a small trend line. |
| `description` | Help text under the value. |

## `dash.text`

`title`, `text`, and `variant` (`info` \| `success` \| `warning` \| `danger`, drawn as a callout).
`markdown = true` (or `format = "markdown"`) enables a Markdown subset: `#`–`####` headings, lists,
`>` quotes, fenced code, `---` rules, and inline `code`, `**bold**`, `*italic*` and `[links](url)`.
Error nodes created by the server render as `danger` alerts.

## `dash.form`

`title`, `request` (default `submitted`), `submit_label`, `variant`, `disabled`, `reset_on_submit`,
and `payload` (static values merged into every submission, such as the issue being edited).

A field is `{ name` (**required**)`, label, type, required, placeholder, help, default, options,
min, max, step, pattern, min_length, max_length }`.

- **Types.** `text`, `textarea`, `number`, `integer`, `boolean`, `select`, `date`, `datetime`,
  `email`, `url`. A field with `options` defaults to `select`. Each option is a value or
  `{ value, label }`.
- **Validation.** Fields are validated in the browser. Errors show next to each field, and focus
  moves to the first invalid one. The Adapter's Request schema and the action's argument schema
  remain the authority on the server.
- **Typed payload.** Numbers are sent as numbers and booleans as booleans. `datetime` is sent as
  an ISO 8601 UTC timestamp. Select values keep their original type. Empty optional fields are
  omitted.

## `dash.button`

`label`, `request` (default `clicked`), `payload`, `variant` (`primary` (the default) \|
`default` \| `danger`), `disabled`, `description` (tooltip and screen-reader text). The button is
disabled while its Request is resolving.

## `dash.filter`

Writes Dashboard State `state_key`. Its shown value follows the State the server pushes, so
another Client, an automation or an Agent changing the State updates the control. The Adapter's
`value` prop is the fallback.

| `control` | Writes |
| --- | --- |
| `select` (default) | the chosen option value (string). `all_label`/`placeholder` adds an empty option. |
| `multiselect` | list of selected values |
| `search` | the text, debounced by `debounce_ms` (default 300; Enter commits immediately) |
| `toggle` | boolean |
| `segmented` | the chosen option value. `all_label` adds an "all" segment that writes `""`. |

## Layout: `dash.container`, `dash.grid`, `dash.tabs`

- **`dash.container`** takes `direction` (`vertical` \| `horizontal`), `gap`, `wrap`, `align`
  (`start` \| `center` \| `end` \| `stretch` \| `baseline`), `justify` (`start` \| `center` \|
  `end` \| `between` \| `around` \| `evenly`) and an optional section `title`.
- **`dash.grid`** takes `columns`, `gap` and `title`. It is a 12-column grid. When the grid is
  narrower than 560px, every cell spans the full width.
- **`dash.tabs`** takes `title` (the tab list's accessible name), `default_tab` (an index or a
  child `key`) and `client_state_key`. Each child's tab label is its `tab_label` prop, then its
  `title`, then its `key`. Tabs follow the WAI-ARIA pattern: ←/→/Home/End move between tabs with a
  roving tabindex. The selected tab is remembered in Client State.

The Dashboard Designer can override presentation props per widget: titles, labels, `page_size`,
chart `kind`/`height`/`unit`, text `variant`, button `variant`, container alignment, and similar.
Data props stay owned by the Adapter.

## `dash.history_series` (data provider)

Instance `config`: `series_id` or `series_ids`. Query parameters:

| Param | Meaning |
| --- | --- |
| `series_id` / `params` | Select one configured series, by id or by its sampled query parameters. |
| `since` / `until` | RFC 3339 bounds, `since` inclusive and `until` exclusive. Converted to UTC. An empty window is an error. |
| `window` | A relative window instead of `since`, such as `"15m"`, `"24h"`, `"7d"`, `"2w"`, or a number of seconds. |
| `limit` | Default 500, maximum 2000. With a lower bound you get the oldest N of the window. Without one you get the newest N. Results are always sorted oldest first. |
| `bucket_secs` | Downsample into UTC buckets aligned to the epoch. Accepts a duration (`"5m"`, `"1h"`) or seconds, from 1s up to 366 days. `limit` then counts buckets. |
| `aggregate` | Per-bucket value: `avg` (default), `min`, `max`, `sum`, `count`, `first`, `last`. Requires `bucket_secs`. |

Raw samples are `{ sample_id, series_id, sampled_at, value_num, metadata }`. Buckets are
`{ series_id, sampled_at` (the bucket start)`, value_num, samples }`, so both chart with the default
`x_key`/`y_key`.

```lua
local hourly = ctx:await(ctx.inputs.history:query({
    params = { repository = repo }, window = "7d", bucket_secs = "1h", aggregate = "max",
}))
return {
    key = "trend", widget_type = "dash.chart",
    props = {
        title = "Open PRs (hourly max, 7d)", kind = "area", series = hourly,
        thresholds = { { value = 20, label = "WIP limit", status = "warning" } },
    },
}
```
