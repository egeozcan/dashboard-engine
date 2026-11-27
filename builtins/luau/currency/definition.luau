local dash = require("dash")

local rate = {
    date = dash.string(),
    base = dash.string(),
    quote = dash.string(),
    rate = dash.number(),
}
local pair = {
    base = dash.string(),
    quote = dash.string(),
    date = dash.optional(dash.string()),
    provider = dash.optional(dash.string()),
}
local convert = table.clone(pair)
convert.amount = dash.number()
local conversion = table.clone(rate)
conversion.amount = dash.number()
conversion.converted_amount = dash.number()

local batch = {
    base = dash.string(),
    quotes = dash.optional(dash.list(dash.string())),
    provider = dash.optional(dash.string()),
}
local snapshot = table.clone(batch)
snapshot.date = dash.optional(dash.string())
local series = table.clone(batch)
series.from = dash.string()
series.to = dash.string()

local function read(description, args, result)
    return {
        description = description, mode = "read", required_capabilities = { "rates.read" },
        args = dash.object(args), result = result,
    }
end

return dash.connector {
    id = "dash.currency",
    description = "Latest and historical reference exchange rates and conversions from Frankfurter v2",
    runtime = "runtime.luau",
    handler = "call",
    libraries = { "dash.http_util" },
    capabilities = { "rates.read" },
    config = dash.object {
        base_url = dash.optional(dash.string()),
        mock = dash.optional(dash.map(dash.any())),
    },
    operations = {
        rate = read("Get one reference rate and its actual observation date", pair, dash.object(rate)),
        convert = read("Convert an amount using the chosen pair and observation date", convert, dash.object(conversion)),
        rates = read("Get latest or single-date rates for a base and optional quote currencies", snapshot, dash.list(dash.object(rate))),
        time_series = read("List observations over an inclusive date range", series, dash.list(dash.object(rate))),
        currencies = read("List the current Frankfurter currency and asset catalogue", {}, dash.list(dash.object {
            iso_code = dash.string(), name = dash.string(), type = dash.string(),
        })),
    },
}
