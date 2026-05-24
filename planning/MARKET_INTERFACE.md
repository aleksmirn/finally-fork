# Market Data Interface — Unified Design

This document specifies the single Python interface that all FinAlly server
code uses to obtain stock prices. It hides the choice between the
**Massive API** (when `MASSIVE_API_KEY` is set) and the built-in
**simulator** (default).

Companion documents:

- `MASSIVE_API.md` — REST endpoints and Python client usage.
- `MARKET_SIMULATOR.md` — how the simulator generates prices.

## 1. Design Goals

1. **One import for callers.** SSE streaming, the chat agent, and trade
   pricing all import the same object. They never branch on the data source.
2. **Replaceable at startup.** The decision between live and simulated data
   is made once, when the backend boots, based on `os.environ`. After that the
   selected provider is a singleton for the process lifetime.
3. **Push, don't pull, downstream.** Both providers feed a shared in-memory
   price cache on the same cadence (~500ms emit). The SSE layer reads from
   the cache, never from the provider directly. This keeps the SSE loop
   identical regardless of source and absorbs the difference between the
   simulator's 500ms tick and the Massive API's 15s poll.
4. **No locking on the hot path.** The cache is a plain dict guarded by a
   single `asyncio.Lock` only on writes; reads are best-effort and may see a
   slightly stale value, which is fine for a tick-driven UI.
5. **Async-first.** All public methods are `async`. The simulator runs as an
   `asyncio.Task`; the Massive poller uses `httpx.AsyncClient` (or the
   `massive` client wrapped in `asyncio.to_thread` if the sync client is the
   only option at the time of writing).

## 2. Public Interface

A single abstract base class, `MarketDataProvider`, defines the contract:

```python
# backend/market/interface.py
from __future__ import annotations
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class PriceTick:
    """One price observation for a single ticker."""
    ticker: str
    price: float
    prev_tick_price: float       # what we emitted on the previous tick
    timestamp: datetime          # tz-aware, UTC
    direction: str               # "up" | "down" | "flat"


class MarketDataProvider(ABC):
    """The single contract for getting prices into FinAlly."""

    @abstractmethod
    async def start(self) -> None:
        """Begin populating the internal price cache. Returns once the first
        full set of prices is available (so the API can answer immediately)."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task. Idempotent."""

    @abstractmethod
    def get_price(self, ticker: str) -> float | None:
        """Latest cached price, or None if the ticker isn't tracked yet."""

    @abstractmethod
    def get_prices(self) -> dict[str, float]:
        """Snapshot copy of all currently tracked ticker prices."""

    @abstractmethod
    def add_ticker(self, ticker: str) -> None:
        """Begin tracking a ticker. Must be safe to call concurrently and
        idempotent. For the simulator: starts a new GBM path. For Massive:
        adds the symbol to the next poll's URL."""

    @abstractmethod
    def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker. Idempotent."""

    @abstractmethod
    def subscribe(self) -> AsyncIterator[PriceTick]:
        """Yield every PriceTick the provider emits, in real time, until the
        consumer disconnects. Used by the SSE endpoint. Each subscriber gets
        its own independent queue; back-pressure drops the oldest tick."""
```

Notes:

- `get_price` / `get_prices` are **synchronous** because they read from a
  process-local dict. The hot read path doesn't need an event loop.
- `subscribe()` is an async generator. The internal implementation is an
  `asyncio.Queue` per subscriber that the dispatcher fans out to.
- Mutation methods (`add_ticker` / `remove_ticker`) are sync; they only
  modify the tracked-set and let the next tick handle the consequences.

## 3. Factory and Selection

A single factory chooses the implementation at startup:

```python
# backend/market/factory.py
import os
from .interface import MarketDataProvider
from .simulator import SimulatorProvider
from .massive_provider import MassiveProvider


def build_provider(initial_tickers: list[str]) -> MarketDataProvider:
    """Return the right provider based on the environment.

    Massive when MASSIVE_API_KEY is set and non-empty; simulator otherwise.
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        return MassiveProvider(
            api_key=api_key,
            initial_tickers=initial_tickers,
            poll_seconds=float(os.environ.get("MASSIVE_POLL_SECONDS", "15")),
            emit_interval=float(os.environ.get("MARKET_EMIT_INTERVAL", "0.5")),
        )
    return SimulatorProvider(
        initial_tickers=initial_tickers,
        tick_interval=float(os.environ.get("MARKET_EMIT_INTERVAL", "0.5")),
    )
```

The FastAPI app wires this up in its lifespan handler:

```python
# backend/main.py (sketch)
from contextlib import asynccontextmanager
from fastapi import FastAPI
from .market.factory import build_provider
from .db import load_watchlist_tickers

@asynccontextmanager
async def lifespan(app: FastAPI):
    provider = build_provider(load_watchlist_tickers())
    await provider.start()
    app.state.market = provider
    try:
        yield
    finally:
        await provider.stop()

app = FastAPI(lifespan=lifespan)
```

Anywhere in the code that needs prices:

```python
def current_price(request, ticker: str) -> float:
    price = request.app.state.market.get_price(ticker)
    if price is None:
        raise MarketDataError(f"No price yet for {ticker}")
    return price
```

## 4. The Shared Price Cache

Both providers extend the same base class that owns the cache and the
fan-out dispatcher. The provider-specific classes only fill the cache; the
base class handles emit cadence and subscriber dispatch.

```python
# backend/market/base.py
import asyncio
from datetime import datetime, timezone
from .interface import MarketDataProvider, PriceTick


class BaseProvider(MarketDataProvider):
    def __init__(self, emit_interval: float):
        self._emit_interval = emit_interval
        self._latest: dict[str, float] = {}      # ticker -> last cached price
        self._prev_emitted: dict[str, float] = {}  # ticker -> last *emitted* price
        self._tickers: set[str] = set()
        self._subscribers: list[asyncio.Queue[PriceTick]] = []
        self._emit_task: asyncio.Task | None = None

    # --- sync read API --------------------------------------------------
    def get_price(self, ticker: str) -> float | None:
        return self._latest.get(ticker)

    def get_prices(self) -> dict[str, float]:
        return dict(self._latest)

    def add_ticker(self, ticker: str) -> None:
        self._tickers.add(ticker)

    def remove_ticker(self, ticker: str) -> None:
        self._tickers.discard(ticker)
        self._latest.pop(ticker, None)
        self._prev_emitted.pop(ticker, None)

    # --- subscriber fan-out --------------------------------------------
    async def subscribe(self):
        queue: asyncio.Queue[PriceTick] = asyncio.Queue(maxsize=256)
        self._subscribers.append(queue)
        try:
            while True:
                yield await queue.get()
        finally:
            self._subscribers.remove(queue)

    async def _emit_loop(self):
        """Every emit_interval, push one PriceTick per tracked ticker to
        every subscriber. Computes direction relative to the prior emit."""
        while True:
            now = datetime.now(timezone.utc)
            for ticker in list(self._tickers):
                price = self._latest.get(ticker)
                if price is None:
                    continue
                prev = self._prev_emitted.get(ticker, price)
                direction = (
                    "up" if price > prev else "down" if price < prev else "flat"
                )
                tick = PriceTick(ticker, price, prev, now, direction)
                self._prev_emitted[ticker] = price
                for q in self._subscribers:
                    if q.full():
                        q.get_nowait()  # drop oldest
                    q.put_nowait(tick)
            await asyncio.sleep(self._emit_interval)
```

The two concrete providers fill `self._latest` on their own cadence; the
emit loop reads from it on a fixed cadence, independent of the source rate.

## 5. SimulatorProvider (default)

See `MARKET_SIMULATOR.md` for the maths and seed-price table. From this
document's perspective:

- Updates `self._latest[ticker]` every ~500ms (same as the emit interval, so
  effectively every emit ships a fresh GBM step).
- `add_ticker(...)` starts a new random walk from a seed price; if no seed is
  known for that symbol it picks one in `$50..$500`.
- Has no external dependencies; runs as one `asyncio.Task`.

## 6. MassiveProvider (when MASSIVE_API_KEY is set)

Calls the Massive snapshot endpoint on a slow cadence and updates the
cache. The emit loop keeps repeating the last cached price between polls so
the UI doesn't go silent. The flash is only triggered when an actual change
happens, which on the Massive side means roughly every poll_seconds rather
than every emit_interval — that's an accurate reflection of how often the
upstream data actually changes.

```python
# backend/market/massive_provider.py  (sketch)
import asyncio
from massive import RESTClient
from .base import BaseProvider


class MassiveProvider(BaseProvider):
    def __init__(self, api_key, initial_tickers, poll_seconds, emit_interval):
        super().__init__(emit_interval=emit_interval)
        self._client = RESTClient(api_key=api_key)
        self._poll_seconds = poll_seconds
        self._tickers.update(initial_tickers)
        self._poll_task: asyncio.Task | None = None

    async def start(self):
        await self._poll_once()                          # block until first prices
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._emit_task = asyncio.create_task(self._emit_loop())

    async def stop(self):
        for t in (self._poll_task, self._emit_task):
            if t:
                t.cancel()
        # swallow CancelledError on await

    async def _poll_loop(self):
        while True:
            await asyncio.sleep(self._poll_seconds)
            try:
                await self._poll_once()
            except Exception as exc:
                # log and keep going; never crash the loop
                ...

    async def _poll_once(self):
        if not self._tickers:
            return
        tickers = list(self._tickers)
        snapshots = await asyncio.to_thread(
            self._client.get_snapshot_all, "stocks", tickers
        )
        for snap in snapshots:
            price = (snap.last_trade and snap.last_trade.price) \
                    or (snap.day and snap.day.close)
            if price:
                self._latest[snap.ticker] = float(price)
```

Notes:

- `asyncio.to_thread` is used because the official client is sync. If we
  switch to raw `httpx.AsyncClient` (see `MASSIVE_API.md` §6) this thread hop
  goes away.
- The poll fetches the **union** of all currently tracked tickers in one
  request — no per-ticker calls.
- `add_ticker` does not trigger an immediate poll; the new symbol just shows
  up in the next cycle. A user adding a ticker may wait up to
  `poll_seconds` for its first price. Acceptable for the demo.

## 7. Failure Modes

| Scenario | Behaviour |
|----------|-----------|
| `MASSIVE_API_KEY` set but invalid | First poll raises `AuthError` from `start()`. The backend logs the error and falls back to the simulator at startup. We never silently serve simulated data once we've claimed to be live mid-session. |
| Single poll fails (network, 5xx) | Logged at WARNING; cache retains last known prices; emit loop keeps re-emitting them. After three consecutive failures, the connection status indicator transitions to "reconnecting". |
| `add_ticker` for an unknown symbol | Massive returns no row for it; cache stays empty for that symbol; SSE simply doesn't emit it. The watchlist API surface decides whether to surface an `UNKNOWN_TICKER` error to the user (it does, via a one-shot single-ticker snapshot call). |
| Simulator over-volatile (price < $0.01) | Floor at $0.01. See `MARKET_SIMULATOR.md`. |

## 8. Testing the Interface

Three layers of tests live in `backend/tests/market/`:

1. **Conformance tests.** A `pytest` parametrized fixture runs the same
   assertions against both providers: `get_price` returns float after
   `start()`, `add_ticker` causes the symbol to appear within N ticks,
   `subscribe()` yields ticks with correct `direction`. This is the contract
   guard that prevents the two implementations from drifting apart.
2. **Simulator-specific tests.** GBM math, seed prices, no negative prices.
3. **Massive-specific tests.** `respx` (or `pytest-httpx`) fixtures stub the
   REST endpoints and assert: correct URL, correct `tickers=` query string,
   correct extraction of `lastTrade.p` from the response, graceful handling
   of `{"status":"ERROR"}` bodies, retry-on-timeout behaviour.

Neither path requires a real `MASSIVE_API_KEY` at test time.

## 9. Future Extensions (Out of Scope)

- WebSocket-based providers — same interface, different fill mechanism.
- Multi-user scoping — `add_ticker` becomes per-user. The cache is global by
  design today because the union of all users' watchlists is what we need
  to poll anyway. The `subscribe()` API already supports per-connection
  fan-out, which is the only multi-user-relevant surface.
- Historical backfill — a separate `get_history(ticker, range)` method on
  the interface would let us call grouped-daily or custom-bars endpoints
  without changing the streaming code.
