# Weather and currency API selection

Reviewed 27 November 2026 for built-in connectors serving personal and commercial dashboards. For worldwide coordinate-based forecasts and daily reference exchange rates, use **MET Norway Locationforecast 2.0** and **Frankfurter v2**. Both supply JSON over HTTPS without an API key. [MET Norway API guide](https://api.met.no/weatherapi/locationforecast/2.0/documentation) · [Frankfurter API guide](https://frankfurter.dev/)

## Weather

| Provider | When it fits |
| --- | --- |
| **MET Norway Locationforecast** | Free for commercial use with attribution. The compact endpoint supplies nine days of forecasts for coordinates worldwide. The model updates hourly in the Nordic region and every six hours elsewhere. Label the latest time step as a forecast. [Forecast guide and example request](https://api.met.no/weatherapi/locationforecast/2.0/documentation) · [Commercial use](https://api.met.no/doc/FAQ) |
| Open-Meteo | A good option for personal dashboards that need geocoding and selectable weather models. The public tier allows 10,000 calls per day for noncommercial use. Commercial use of the hosted service requires a subscription, API key and customer endpoint. [Plans](https://open-meteo.com/en/pricing) · [Usage terms](https://open-meteo.com/en/terms) |
| WeatherAPI.com | A better fit if the widget needs city search and current conditions. Its free commercial tier includes 100,000 calls per month and a three-day forecast. Each app needs a key and must credit WeatherAPI.com on the free tier. Cache limits are one hour for current conditions and 24 hours for forecasts. [Pricing](https://www.weatherapi.com/pricing) · [Service terms](https://www.weatherapi.com/terms) |

The documented compact forecast request is `GET https://api.met.no/weatherapi/locationforecast/2.0/compact?lat=60.10&lon=9.58`. An authorized human must accept the terms before use. Send the deployment's name and contact details in its `User-Agent`. Truncate coordinates to four decimal places and cache each location until both `Expires` and the product's minimum repeat interval have passed (30 minutes in the Nordic region, three hours elsewhere). Revalidate using `If-Modified-Since`. Credit MET Norway in the dashboard, link to CC BY 4.0 and identify modifications. [Service terms](https://api.met.no/doc/TermsOfService) · [Forecast intervals](https://api.met.no/weatherapi/locationforecast/2.0/documentation) · [Attribution](https://api.met.no/doc/License)

The weather connector caches forecasts per location beyond the query engine’s 30-second default. The HTTP broker supports gzip decoding for MET responses. [Connector setup](./connectors.md#weather-forecasts-from-met-norway-dashweather) · [HTTP client features](../crates/dash-core/Cargo.toml) · [MET protocol requirements](https://api.met.no/doc/TermsOfService)

## Currency

**Frankfurter v2** supports latest rates, historical dates and time series with selectable base and quote currencies. Its daily blended rates can be displayed in paid products, cached indefinitely and used without attribution. Queries scoped to an institution such as the ECB, and any republished underlying observations, follow that institution's data terms. Choose the blend for a general conversion widget or an explicit provider when the dashboard needs a named reference source. [API and commercial-use FAQ](https://frankfurter.dev/) · [Provider terms](https://frankfurter.dev/license/)

Display the returned date beside the rate. A single-date blended lookup can fall back up to 14 days. For example, a verified ECB query for Sunday, 1 November 2026 returned USD and GBP observations dated Friday, 30 October. Daily reference rates suit conversion estimates and trend charts. Intraday trading would require a market-data feed. [Methodology](https://frankfurter.dev/methodology/) · [ECB example response](https://api.frankfurter.dev/v2/providers/ecb/rates?base=EUR&quotes=USD,GBP&date=2026-11-01)

**ExchangeRate-API Open Access** also provides free keyless daily conversions and permits caching. It requires visible attribution and prohibits redistributing its data, which makes it a less flexible default for dashboards with exports. [Open Access terms and limits](https://www.exchangerate-api.com/docs/free)
