# Massive API (formerly Polygon.io) — Reference for FinAlly

This document is the reference for how FinAlly talks to the Massive market data
API. It focuses only on the endpoints we actually need: snapshots for live
prices and daily aggregates for end-of-day prices.

## 1. Background

In October 2025 **Polygon.io** rebranded to **Massive**.

- Marketing site: `https://massive.com`
- API base URL: `https://api.massive.com`
- The legacy `https://api.polygon.io` host still resolves to the same backend
  for backwards compatibility, but new code should target `api.massive.com`.
- The official Python client renamed its PyPI distribution and import path
  from `polygon-api-client` / `from polygon import RESTClient`
  to `massive` / `from massive import RESTClient`.

The REST paths (`/v2/snapshot/...`, `/v2/aggs/...`) are unchanged from the
Polygon era, so older examples on the web still work against the new host.

## 2. Authentication

A single API key authenticates all REST calls. It can be supplied in two ways:

- **HTTP header** (preferred): `Authorization: Bearer <API_KEY>`
- **Query parameter** (also accepted): `?apiKey=<API_KEY>`

In FinAlly the key lives in the environment variable `MASSIVE_API_KEY`. When
this variable is unset or empty the backend falls back to the simulator and
does not import the Massive client at all (see `MARKET_INTERFACE.md`).

## 3. Rate Limits and Tiers

Massive's published tiers for the Stocks REST API are summarized below. Numbers
are the historical Polygon.io limits that still apply post-rebrand; consult
`https://massive.com/pricing` for the latest values before deployment.

| Tier       | Requests / minute | Data freshness       |
|------------|-------------------|----------------------|
| Basic      | 5                 | End-of-day only      |
| Starter    | 100               | 15-minute delayed    |
| Developer  | Unlimited         | 15-minute delayed    |
| Advanced   | Unlimited         | Real-time (consolidated) |
| Business   | Unlimited         | Real-time + extras   |

FinAlly's behaviour is keyed off the **poll interval**, not the tier directly.
The interval is configurable (`MASSIVE_POLL_SECONDS`, default `15`). At 15s
per poll we issue 4 requests/minute, which fits comfortably inside the Basic
tier's 5/min cap when polling the full-market snapshot in a single call.

We never burst: every cycle issues exactly one HTTP request that returns data
for the entire watchlist at once (see snapshot endpoint below).

## 4. Endpoints We Use

FinAlly only needs four endpoints. Everything else (trades, quotes, news,
financials, options chains) is out of scope.

### 4.1 Full-market snapshot — live prices for many tickers

`GET /v2/snapshot/locale/us/markets/stocks/tickers`

This is the workhorse for live prices. A single request returns the latest
snapshot for an arbitrary list of tickers.

Query parameters:

| Parameter     | Type    | Notes                                                          |
|---------------|---------|----------------------------------------------------------------|
| `tickers`     | string  | Comma-separated, case-sensitive (e.g. `AAPL,MSFT,GOOGL`). Optional; omitting returns the full market. |
| `include_otc` | boolean | Default `false`. We leave it off.                              |

Response shape (truncated to fields we read):

```json
{
  "status": "OK",
  "count": 3,
  "tickers": [
    {
      "ticker": "AAPL",
      "todaysChange": 1.23,
      "todaysChangePerc": 0.65,
      "updated": 1716494630512000000,
      "day":      { "o": 191.0, "h": 193.1, "l": 190.5, "c": 192.45, "v": 12345678, "vw": 192.10 },
      "prevDay":  { "o": 189.5, "h": 191.8, "l": 188.9, "c": 191.22, "v": 23456789, "vw": 190.65 },
      "lastTrade":{ "p": 192.45, "s": 100, "t": 1716494630512000000, "x": 4, "c": [] },
      "lastQuote":{ "P": 192.46, "S": 5, "p": 192.44, "s": 7, "t": 1716494630511000000 },
      "min":      { "o": 192.40, "h": 192.50, "l": 192.38, "c": 192.45, "v": 12345, "vw": 192.44 }
    }
  ]
}
```

Notes on fields:

- `lastTrade.p` is the live price. On weekends / outside extended hours this
  field can be the previous session's last print; that's fine for our use.
- `updated` is a Unix nanosecond timestamp.
- `todaysChange` / `todaysChangePerc` are computed against the previous
  session close — useful as a sanity check but FinAlly computes its own deltas
  from streaming ticks.

### 4.2 Single-ticker snapshot

`GET /v2/snapshot/locale/us/markets/stocks/tickers/{stocksTicker}`

Same response shape as above wrapped under a `ticker` object. We only call
this on demand (e.g. when the user adds one new ticker and we don't want to
wait for the next batch poll).

### 4.3 Previous close

`GET /v2/aggs/ticker/{stocksTicker}/prev`

Query parameters:

| Parameter   | Type    | Notes                                          |
|-------------|---------|------------------------------------------------|
| `adjusted`  | boolean | Default `true` (split-adjusted). We leave it.  |

Response:

```json
{
  "status": "OK",
  "ticker": "AAPL",
  "queryCount": 1,
  "resultsCount": 1,
  "adjusted": true,
  "results": [
    { "T": "AAPL", "o": 189.5, "h": 191.8, "l": 188.9, "c": 191.22,
      "v": 23456789, "vw": 190.65, "t": 1716422400000 }
  ]
}
```

Used once on cold start to seed the previous-day close so the heatmap and P&L
displays have a meaningful baseline before the first live tick arrives.

### 4.4 Grouped daily bars — end-of-day for the whole market

`GET /v2/aggs/grouped/locale/us/market/stocks/{date}`

`{date}` is a path parameter in `YYYY-MM-DD` form (the trading date).

Query parameters: `adjusted` (default `true`), `include_otc` (default `false`).

Response `results[]` items use the same single-letter keys as previous close
(`T, o, h, l, c, v, vw, t, n`). A single request returns one row per ticker
for the entire US market, which is the efficient way to backfill historical
prior closes for any watchlist.

## 5. Python Client Quickstart

We install the official client:

```bash
uv add massive
```

If `massive` is not available on PyPI at install time, the predecessor
`polygon-api-client` package is identical at the wire level:

```bash
uv add polygon-api-client  # legacy fallback; from polygon import RESTClient
```

### 5.1 Initialising the client

```python
import os
from massive import RESTClient

client = RESTClient(api_key=os.environ["MASSIVE_API_KEY"])
```

The constructor also accepts:

- `trace=True` — log every HTTP request to stderr (debugging only).
- `verbose=True` — verbose logging from the underlying urllib3 pool.
- `pagination=True` (default) — iterators auto-paginate. Irrelevant for
  snapshot/previous-close calls which are single-page.

### 5.2 Live snapshot for a watchlist

```python
def fetch_snapshot(client: RESTClient, tickers: list[str]) -> dict[str, float]:
    """Return {ticker: latest_price} for the given symbols in one request."""
    snapshots = client.get_snapshot_all(
        market_type="stocks",
        tickers=tickers,
    )
    prices: dict[str, float] = {}
    for snap in snapshots:
        # Prefer the last trade print; fall back to the day's close.
        price = (snap.last_trade and snap.last_trade.price) or (snap.day and snap.day.close)
        if price:
            prices[snap.ticker] = price
    return prices
```

`snapshots` is a list of `TickerSnapshot` objects. The shape mirrors the JSON:
`snap.ticker`, `snap.todays_change`, `snap.todays_change_percent`,
`snap.day.close`, `snap.prev_day.close`, `snap.last_trade.price`,
`snap.updated` (nanoseconds).

### 5.3 Single-ticker snapshot

```python
snap = client.get_snapshot_ticker(market_type="stocks", ticker="AAPL")
print(snap.last_trade.price, snap.todays_change_percent)
```

### 5.4 Previous close

```python
prev = client.get_previous_close_agg(ticker="AAPL")
bar = prev[0]            # list of Agg with one element
print(bar.close, bar.timestamp)
```

### 5.5 Grouped daily bars (end-of-day for entire market)

```python
from datetime import date

bars = client.get_grouped_daily_aggs(date=date.today().isoformat())
closes = {bar.ticker: bar.close for bar in bars}
```

### 5.6 Error handling

The client raises `massive.exceptions.AuthError` on 401/403 and
`massive.exceptions.BadResponse` for 4xx/5xx with the API's JSON error body
attached. We catch both at the adapter boundary and translate to our own
`MarketDataError` so callers above the interface never import massive types.

```python
from massive.exceptions import AuthError, BadResponse

try:
    snapshots = client.get_snapshot_all("stocks", tickers)
except AuthError as exc:
    raise MarketDataError("Invalid MASSIVE_API_KEY") from exc
except BadResponse as exc:
    raise MarketDataError(f"Massive API error: {exc}") from exc
```

### 5.7 Timeouts and retries

The client wraps `urllib3` with a default connect/read timeout of 10s and no
automatic retry. For FinAlly we wrap calls in a simple retry-with-backoff
helper (3 attempts, 1s / 2s / 4s) so a transient blip during one poll cycle
doesn't kill the price stream. Specifics live in the adapter implementation,
not in this reference document.

## 6. Raw HTTP — Fallback Without the Client

If we ever need to drop the dependency the API is plain JSON over HTTPS:

```python
import os, httpx

def snapshot(tickers: list[str]) -> dict:
    resp = httpx.get(
        "https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers",
        params={"tickers": ",".join(tickers)},
        headers={"Authorization": f"Bearer {os.environ['MASSIVE_API_KEY']}"},
        timeout=10.0,
    )
    resp.raise_for_status()
    return resp.json()
```

This is also the simplest way to mock the API in unit tests — point httpx at
a local respx fixture and assert against the JSON shape documented above.

## 7. What We Deliberately Do Not Use

- **WebSocket streams** (`wss://socket.massive.com/...`). Real-time
  WebSockets require the Advanced or Business tier and add a second
  connection-management surface. Polling the snapshot endpoint at 15s is fast
  enough for the demo and keeps the architecture identical across all tiers.
- **Trades / quotes endpoints** (`/v3/trades/{ticker}`, etc.). We never need
  individual prints; the snapshot's `lastTrade` and `lastQuote` are sufficient.
- **Reference data** (tickers, splits, dividends, financials). Not needed for
  the MVP.
- **Pagination iterators** (`list_aggs`, `list_trades`). Our two read patterns
  — current snapshot of the watchlist and previous-day close — are both
  single-page, so we use the eager `get_*` methods exclusively.
