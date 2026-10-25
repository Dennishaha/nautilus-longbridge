# Changelog

## 0.1.5 — 2026-10-25

### Bar catalog persistence

- Add `bar_catalog_streaming` and a runnable market-data recorder using NT's
  native streaming lifecycle, periodic Parquet promotion, close-time promotion
  and retained Feather sources for recovery.
- Correct SDK 5.2.2 period conversion (`Min_60` for hourly bars and enums without
  `.name`); reject unsupported periods and non-LAST/non-external bar requests.
- Track subscriptions by symbol and period. Preserve full OHLCV and confirmation
  state in update deduplication; suppress repeated/stale confirmed live frames.
- Drain accepted market-data events before stopping the adapter consumer.
- Verify minute/hour/day Bar and Equity round trips through a native LiveNode
  and ParquetDataCatalog, including catalog visibility while the node runs.

### Validation and scope

- 84 connector tests pass locally on Python 3.14 with NautilusTrader 2.0.0rc7
  and Longbridge 5.2.2; Ruff and mypy pass.
- Native integration tests cover periodic and close-time promotion, retained
  Feather recovery, and rejection of overlapping manual imports in rc7.
- Source distributions include the changelog, example configuration, recorder
  scripts and test fixtures.
- Recording covers confirmed live bars. Historical gap backfill and business-key
  deduplication across separate runs are not included.

## 0.1.4

### Paper cross-currency margin

- Add explicit `account_mode="MARGIN"` for USD-valued paper accounts; keep CASH
  as the connector default. Register a Nautilus `MarginAccount` using
  broker-reported collateral equity and initial/maintenance margins.
- Preserve per-currency cash, negative available cash, buying power, financing
  limits and broker risk separately. Missing USD cash no longer blocks margin
  mode and is never replaced with buying power.
- Query symbol- and order-specific purchase capacity before each buy. Check
  fresh quotes, broker risk, positions, pending orders and local fills, with
  limits of three positions and one times net-equity exposure.
- Allow sales only to reduce existing longs, including pending-exit reservations.
  Preserve cancellation intent during preflight, in-flight submission and
  uncertain-order reconciliation. Margin amendments require cancel and resubmit.
- Limit margin mode to paper US-equity execution; it does not enable short
  selling, automatic currency exchange or real-account margin.

### ORB data and execution support

- Add confirmed-candle filtering, explicit supported bar periods and public
  connection-state accessors.
- Generate BBO quotes from depth receipt timestamps. Queue delay, last-trade
  updates and reconnection cannot make stale depth appear fresh.
- Add reconciled paper snapshots and terminal-order fee queries. Emit known
  executions before partial-cancel/rejection terminal events.
- Preserve the actual instant of SDK local-naive datetimes when converting
  timestamps to UTC.

### Validation

- 71 connector tests pass locally on Python 3.14 with NautilusTrader 2.0.0rc7
  and Longbridge 5.2.2; Ruff and mypy pass.
- A native Nautilus LiveNode test completes a buy/fill/sell/fill round trip
  through a fake broker with HKD-only cash details.
- An actual paper-account read-only probe registers a `MarginAccount` and
  returns positive market-order margin capacity without USD cash. No broker
  orders were submitted; actual paper matching and financing postings remain
  unverified.
