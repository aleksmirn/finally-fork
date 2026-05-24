# Market Simulator — Design and Implementation

This document specifies the built-in price simulator. It is the default
market data source when `MASSIVE_API_KEY` is unset. It implements the
`MarketDataProvider` contract defined in `MARKET_INTERFACE.md`, so the rest of
the backend is agnostic to whether prices are simulated or live.

## 1. Goals

1. **Look believable on screen.** Prices should drift, jitter, and produce a
   mix of green/red flashes such that the UI doesn't feel static. The
   sparklines should look like real stock charts, not random noise.
2. **Run forever without external dependencies.** One `asyncio.Task`, pure
   Python, no network. Boot to first price in well under a second.
3. **Be deterministic for tests.** A seeded random generator means the same
   `seed` produces the same price sequence. The E2E suite relies on this.
4. **Be cheap.** ~20 tickers updating every 500ms is trivial CPU; we never
   want this loop to be a noticeable load even in the slowest environments.

Non-goals: realistic news shocks, intraday seasonality (open/close volume),
sector correlation. These are mentioned as stretch ideas in `PLAN.md` §6 but
do not ship in the MVP.

## 2. Model: Geometric Brownian Motion

Each ticker is an independent GBM path with its own drift `mu` and
volatility `sigma`. The discrete-time update for one tick of length `dt`
(in years) is:

```
P_{t+1} = P_t * exp( (mu - 0.5 * sigma^2) * dt  +  sigma * sqrt(dt) * Z )
```

where `Z ~ N(0, 1)` is a standard normal random variable drawn fresh per
tick per ticker.

Why GBM:

- Prices are strictly positive by construction (we never need an awkward
  clamp at zero except as a safety floor).
- It's the textbook model for log-returns and produces visually-correct
  charts at any timescale.
- The two parameters are intuitive: annualised drift and annualised vol.

### Time scaling

The simulator ticks every `tick_interval` seconds (default `0.5`). Converted
to years for the GBM formula:

```python
SECONDS_PER_TRADING_YEAR = 252 * 6.5 * 60 * 60     # 252 trading days * 6.5h
dt = tick_interval / SECONDS_PER_TRADING_YEAR
```

That is the right unit because `mu` and `sigma` are expressed annualised, the
way every finance textbook quotes them. A `sigma=0.30` (30% annual vol) on a
0.5s tick produces a per-tick standard deviation of returns of roughly
`0.30 * sqrt(0.5 / 5_896_800) ≈ 0.00009`, i.e. about 1 basis point per tick.
That feels lively without looking jittery.

## 3. Parameters per Ticker

Each supported ticker has a triple: `seed_price`, `mu`, `sigma`. Drifts are
small (a handful of basis points to a few percent annualised); vols are
chosen so visually each name has a recognisable rhythm — `TSLA` rocky,
`JPM` calm.

```python
# backend/market/seeds.py
TICKER_SEEDS: dict[str, tuple[float, float, float]] = {
    # ticker:    (seed_price, mu_annual, sigma_annual)
    "AAPL":  (192.45, 0.08, 0.25),
    "GOOGL": (175.30, 0.07, 0.28),
    "MSFT":  (415.20, 0.09, 0.22),
    "AMZN":  (185.10, 0.10, 0.30),
    "TSLA":  (248.60, 0.05, 0.55),   # high vol
    "NVDA":  (920.40, 0.15, 0.45),
    "META":  (495.80, 0.10, 0.32),
    "JPM":   (198.25, 0.05, 0.18),   # low vol
    "V":     (276.40, 0.07, 0.18),
    "NFLX":  (612.30, 0.08, 0.35),

    "AMD":   (165.20, 0.10, 0.45),
    "INTC":  (31.40,  0.02, 0.30),
    "ORCL":  (138.60, 0.06, 0.22),
    "CRM":   (302.80, 0.07, 0.30),
    "ADBE":  (515.40, 0.06, 0.28),
    "COST":  (815.90, 0.08, 0.18),
    "MA":    (478.10, 0.07, 0.20),
    "HD":    (348.50, 0.05, 0.22),
    "DIS":   (104.30, 0.03, 0.28),
    "BA":    (180.70, 0.02, 0.40),
}

# Used when the LLM or the user adds a ticker we don't have a seed for.
FALLBACK = (100.0, 0.05, 0.30)
```

Seeds are deliberately rounded to two decimals in the realistic range for
2026. They're not promises about reality; they just have to be plausible.

## 4. Module Structure

```
backend/
└── market/
    ├── __init__.py
    ├── interface.py          # MarketDataProvider + PriceTick (see MARKET_INTERFACE.md)
    ├── base.py               # BaseProvider with cache + emit loop
    ├── factory.py            # build_provider()
    ├── seeds.py              # TICKER_SEEDS, FALLBACK
    ├── simulator.py          # SimulatorProvider
    └── massive_provider.py   # MassiveProvider
```

Keeping `seeds.py` separate makes it the one place to tweak default
behaviour and the easy thing to monkey-patch in unit tests.

## 5. SimulatorProvider Implementation

```python
# backend/market/simulator.py
from __future__ import annotations
import asyncio
import math
import random
from .base import BaseProvider
from .seeds import TICKER_SEEDS, FALLBACK

SECONDS_PER_TRADING_YEAR = 252 * 6.5 * 60 * 60
PRICE_FLOOR = 0.01


class SimulatorProvider(BaseProvider):
    """In-process price simulator using one GBM path per ticker."""

    def __init__(
        self,
        initial_tickers: list[str],
        tick_interval: float = 0.5,
        seed: int | None = None,
    ):
        super().__init__(emit_interval=tick_interval)
        self._tick_interval = tick_interval
        self._rng = random.Random(seed)
        self._params: dict[str, tuple[float, float]] = {}   # ticker -> (mu, sigma)
        self._tick_task: asyncio.Task | None = None

        for ticker in initial_tickers:
            self._init_ticker(ticker)

    # --- ticker lifecycle ----------------------------------------------
    def _init_ticker(self, ticker: str) -> None:
        seed_price, mu, sigma = TICKER_SEEDS.get(ticker, FALLBACK)
        self._latest[ticker] = seed_price
        self._params[ticker] = (mu, sigma)
        self._tickers.add(ticker)

    def add_ticker(self, ticker: str) -> None:
        if ticker not in self._tickers:
            self._init_ticker(ticker)

    def remove_ticker(self, ticker: str) -> None:
        super().remove_ticker(ticker)
        self._params.pop(ticker, None)

    # --- lifecycle ------------------------------------------------------
    async def start(self) -> None:
        # _latest is already populated from seeds, so the cache is hot the
        # moment start() returns; no need to wait for the first tick.
        self._tick_task = asyncio.create_task(self._tick_loop())
        self._emit_task = asyncio.create_task(self._emit_loop())

    async def stop(self) -> None:
        for t in (self._tick_task, self._emit_task):
            if t:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass

    # --- GBM tick loop --------------------------------------------------
    async def _tick_loop(self) -> None:
        dt = self._tick_interval / SECONDS_PER_TRADING_YEAR
        sqrt_dt = math.sqrt(dt)
        while True:
            for ticker in list(self._tickers):
                mu, sigma = self._params[ticker]
                z = self._rng.gauss(0.0, 1.0)
                price = self._latest[ticker]
                price *= math.exp((mu - 0.5 * sigma * sigma) * dt + sigma * sqrt_dt * z)
                self._latest[ticker] = max(price, PRICE_FLOOR)
            await asyncio.sleep(self._tick_interval)
```

Notes on the implementation choices:

- `random.Random(seed)` is a single per-provider RNG. Drawing all tickers
  from one stream is fine because they're independent and we don't need
  reproducibility per ticker — only at the provider level.
- The tick loop and the emit loop run at the same cadence, but they're
  separate tasks. That's deliberate: keeping them decoupled means the emit
  cadence (the user-visible UI rate) can later differ from the tick cadence
  if we want to slow down or speed up the simulation independently.
- The GBM constants (`mu - 0.5 sigma^2`, `sqrt(dt)`) are recomputed each
  tick. This is trivial CPU and keeps the loop body readable; do not
  optimise unless a profile shows it matters.
- The price floor protects against the (extremely rare) case where the
  random draws produce a sub-cent value over a long session.

## 6. Behavioural Properties

What the UI sees, by design:

- **Always some movement.** Every tick changes every price by a small
  amount, so the flash effect fires constantly. Roughly 50/50 green vs red
  per ticker — GBM is symmetric in log-space around the drift.
- **Drift over a session.** With `mu=0.08` and a 30-minute session,
  expected return is `0.08 * (30 * 60) / 5_896_800 ≈ 0.024%` — invisible at
  a glance, which is correct: we want noise to dominate the visuals, not
  drift.
- **Volatility differentiation.** TSLA visibly jitters more than JPM in
  sparklines. This is the most important visual property — it's what makes
  the watchlist feel like real markets.
- **No correlation.** The XLF holdings (JPM, V, MA) move independently. A
  future enhancement could add a one-factor sector correlation matrix, but
  the MVP doesn't need it.

## 7. Testing the Simulator

Tests live in `backend/tests/market/test_simulator.py`:

1. **Reproducibility.** With `seed=42` and a fixed initial ticker list,
   stepping the loop N times produces a known price sequence (snapshot).
2. **Positivity.** After 100_000 ticks with a high-vol parameter set, no
   price falls below the floor.
3. **Drift sanity.** Mean log-return over a long horizon converges to
   `(mu - 0.5*sigma^2) * dt` within a tolerance (statistical test with a
   generous `n`).
4. **Conformance.** Inherits the parametrized conformance tests defined in
   `MARKET_INTERFACE.md` §8.
5. **Add/remove.** `add_ticker("PLTR")` produces a seeded price within one
   tick; `remove_ticker` makes `get_price` return None.

Tests do **not** rely on `asyncio.sleep` real time — they call `_tick_loop`'s
body directly via a helper, so the suite stays fast.

## 8. Optional Embellishments (Not in MVP)

Tracked here for completeness; do not implement without an updated
`PLAN.md`:

- **Sector correlation.** Replace independent `z` draws with samples from a
  multivariate normal whose covariance reflects sector groupings. Adds two
  lines using `numpy.random.multivariate_normal`.
- **Event spikes.** With small probability per tick, multiply the next
  step's `sigma` by 5–10x for one tick to create a visible jump.
- **Mean reversion.** Replace pure GBM with an Ornstein–Uhlenbeck process
  around the seed price so paths don't wander arbitrarily far from
  realistic levels over very long sessions.
- **Open / close volume profile.** Increase `sigma` at start and end of the
  trading day to mimic the U-shape of intraday volatility. Requires a
  notion of "trading day" the MVP doesn't currently track.
