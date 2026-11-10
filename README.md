# faith-tape

A data-only repository for FAITH (decision S5): a plain Node recorder that reads documented public market-data endpoints once an hour on GitHub Actions and commits what it read. No API keys, no accounts, no orders, no strategy code. Strategy research lives in `mrastro18/faith` and reads this repository as input.

## What it records

| stream | source | file | row |
| --- | --- | --- | --- |
| Binance.US trades, every USDT, USDC and USD pair in the universe below, complete by aggregate-trade id, back to 2026-04-22 | `api.binance.us` `/api/v3/aggTrades` | `data/binanceus/trades/<day>/<SYMBOL>.ndjson` | `[aggId, ms, price, qty, buyerIsMaker]` |
| Binance.US book for universe pairs, top 10 levels each side, once a run | `/api/v3/depth` | `data/binanceus/depth/<day>.ndjson` | `[symbol, ms, lastUpdateId, bids, asks]` |
| Binance.US 24 h stats for every listed pair, once a run | `/api/v3/ticker/24hr` | `data/binanceus/stats24h/<day>.ndjson` | `[ms, [[symbol, count, quoteVolume, bid, ask], ...]]` |
| Binance.US pair list, daily | `/api/v3/exchangeInfo` | `data/binanceus/pairs/<day>.json` | |
| Binance.US daily candles for every listed pair, rewritten daily | `/api/v3/klines` | `data/binanceus/klines1d/<SYMBOL>.json` | `[openMs, close, quoteVolume, trades]`, completed days only |
| Reference price, one-second bars, from the start of recording | Coinbase Exchange `/products/{id}/trades` | `data/coinbase/bars1s/<day>/<BASE>.ndjson` | `[sec, open, high, low, close, volume, count, vwap]` |
| Reference history before recording began, one-minute candles | Coinbase `/products/{id}/candles` | `data/coinbase/candles1m/<day>/<BASE>.ndjson` | `[ms, open, high, low, close, volume]` |
| Second reference, one-second bars | Kraken `/0/public/Trades` | `data/kraken/bars1s/<day>/<BASE>.ndjson` | as Coinbase |
| Kalshi perpetuals, crypto markets, once a run | `external-api.kalshi.com/trade-api/v2/margin/markets` | `data/kalshi/perps/<day>.ndjson` | `[ms, [market objects as served]]` |
| Endpoint probes and fee/terms page hashes, daily | see `recorder/config.json` | `data/probes/<day>.ndjson`, `data/pages/<name>/<day>.html` on change | |
| Run health: calls, non-200 answers, per-task results | | `data/health/<day>.ndjson`, `state/last-run.json` | |
| Known holes in a tape | | `data/gaps/<day>.ndjson` | |

**Binance.US universe.** A pair is recorded if its 24-hour trade count was 5,000 or fewer the first time the recorder saw it, and stops being recorded if the count later goes above 50,000. Each decision and its count is in `state/state.json` under `binanceus.universe`. The rule reads only the count at that moment, but it was first applied on 2026-11-10 to a history that starts on 2026-04-22, so a pair that was thin in spring and busy on 2026-11-10 has no tape: the daily candles (kept for every pair) show which pairs that touches.

**Reference bases** are BTC, ETH, SOL, USDT, USDC and the base of every universe pair that Coinbase or Kraken quotes in dollars (Kraken: at most 15, the five above first). One-minute history before recording began is kept for the five, then for every base with a Binance.US pair that averaged 50 or more trades a day in some calendar month, busiest first.

## Guarantees and limits

- **Complete tapes.** Every stream resumes from a cursor in `state/state.json`, so a late or skipped scheduled run loses nothing on Binance.US. Coinbase pages back at most 120 pages a run; a longer outage on a busy product is written to `data/gaps/` with the missing ids, never silently skipped.
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
