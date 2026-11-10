# faith-tape

A data-only repository for FAITH (decision S5): a plain Node recorder that reads documented public market-data endpoints once an hour on GitHub Actions and commits what it read. No API keys, no accounts, no orders, no strategy code. Strategy research lives in `mrastro18/faith` and reads this repository as input.

## What it records

| stream | source | file | row |
| --- | --- | --- | --- |
| Binance.US trades, every USDT, USDC and USD pair, complete by aggregate-trade id, back to 2026-04-22 | `api.binance.us` `/api/v3/aggTrades` | `data/binanceus/trades/<day>/<SYMBOL>.ndjson` | `[aggId, ms, price, qty, buyerIsMaker]` |
| Binance.US book, top 10 levels each side, once a run | `/api/v3/depth` | `data/binanceus/depth/<day>.ndjson` | `[symbol, ms, lastUpdateId, bids, asks]` |
| Binance.US 24 h stats, once a run | `/api/v3/ticker/24hr` | `data/binanceus/stats24h/<day>.ndjson` | `[ms, [[symbol, count, quoteVolume, bid, ask], ...]]` |
| Binance.US pair list, daily | `/api/v3/exchangeInfo` | `data/binanceus/pairs/<day>.json` | |
| Reference price, one-second bars, from the start of recording | Coinbase Exchange `/products/{id}/trades` | `data/coinbase/bars1s/<day>/<BASE>.ndjson` | `[sec, open, high, low, close, volume, count, vwap]` |
| Reference history before recording began, one-minute candles | Coinbase `/products/{id}/candles` | `data/coinbase/candles1m/<day>/<BASE>.ndjson` | `[ms, open, high, low, close, volume]` |
| Second reference, one-second bars | Kraken `/0/public/Trades` | `data/kraken/bars1s/<day>/<BASE>.ndjson` | as Coinbase |
| Endpoint probes and fee/terms page hashes, daily | see `recorder/config.json` | `data/probes/<day>.ndjson`, `data/pages/<name>/<day>.html` on change | |
| Run health: calls, non-200 answers, per-task results | | `data/health/<day>.ndjson`, `state/last-run.json` | |
| Known holes in a tape | | `data/gaps/<day>.ndjson` | |

Reference bases are BTC, ETH, SOL, USDT, USDC and the base of every Binance.US pair that Coinbase or Kraken quotes in dollars (Kraken: at most 40).

## Guarantees and limits

- **Complete tapes.** Every stream resumes from a cursor in `state/state.json`, so a late or skipped scheduled run loses nothing on Binance.US. Coinbase pages back at most 40 pages a run; a longer outage on a busy product is written to `data/gaps/` with the missing ids, never silently skipped.
- **Settled seconds only.** A one-second bar is written only once the fetch happened at least 5 s after that second ended, and each second is written once.
- **The book is a snapshot**, once an hour. It measures spread and depth at that moment, not what happened between runs.
- **Run times are not exact.** GitHub may start a scheduled run late or skip one. Nothing here depends on the run time.
- **Runner location.** GitHub-hosted runners are US data centres in practice, not by promise. A venue that refuses the runner shows its answer in `data/health/` and `data/probes/`.

## Running locally

```
node --test test/*.test.mjs
node recorder/record.mjs .
```

Changes to what is recorded go in `CHANGES.md` with the date, because a universe or format change mid-tape matters to anyone reading the data.
