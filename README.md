# faith-tape

A data-only repository for FAITH (decision S5): a plain Node recorder that reads documented public market-data endpoints once an hour on GitHub Actions and commits what it read. No API keys, no accounts, no orders, no strategy code. Strategy research lives in `mrastro18/faith` and reads this repository as input.

## What it records

| stream | source | file | row |
| --- | --- | --- | --- |
| Binance.US trades, every USDT, USDC and USD pair in the universe below, complete by aggregate-trade id, back to 2026-04-22 | `api.binance.us` `/api/v3/aggTrades` | `data/binanceus/trades/<day>/<SYMBOL>.ndjson` | `[aggId, ms, price, qty, buyerIsMaker]` |
| Binance.US book for universe pairs, top 10 levels each side, once a run | `/api/v3/depth` | `data/binanceus/depth/<day>.ndjson` | `[symbol, ms, lastUpdateId, bids, asks]` |
| Binance.US best bid and ask, every listed leg of each universe base, polled every 5 s while the chain waits for its next run (about 20 of every 30 minutes) | `/api/v3/ticker/bookTicker`, one call for every pair | `data/binanceus/quotes/<day>/<HHMMSS>Z-<run>.ndjson.gz`, one gzip file per session (sessions on 2026-11-10 up to about 18:07 UTC: `2026-11-10.ndjson`) | `["s", ms, run, everyMs]` session start; `["f", ms, symbol, bid, bidQty, ask, askQty]` full snapshot (start, then every 10 minutes); `["q", ...]` same fields, a change of best bid or ask price; `["x", ms, status]` a failed poll; `["c", ms]` the session's byte budget ran out; `["e", ms, polls, failures, capped]` session end |
| Coinbase Exchange and Kraken spot best bid and ask, BTC, ETH, SOL, XRP and DOGE against USD (list in `recorder/config.json`, `quotes.venues`), polled every 5 s beside Binance.US, from 2026-11-11 | Coinbase `/products/{id}/book?level=1`, one call per product; Kraken `/0/public/Ticker?pair=...`, one call for every pair | `data/coinbase/quotes/<day>/<HHMMSS>Z-<run>.ndjson.gz`, `data/kraken/quotes/<day>/...`, one gzip file per venue per session | as Binance.US, with the venue's own symbols (`BTC-USD`, `XBTUSD`); also `["x", ms, status, symbol]` when one product alone failed in a poll that otherwise worked; Kraken error answers give their text as status |
| Binance.US 24 h stats for every listed pair, once a run | `/api/v3/ticker/24hr` | `data/binanceus/stats24h/<day>.ndjson` | `[ms, [[symbol, count, quoteVolume, bid, ask], ...]]` |
| Binance.US pair list, daily | `/api/v3/exchangeInfo` | `data/binanceus/pairs/<day>.json` | |
| Binance.US daily candles for every listed pair, rewritten daily | `/api/v3/klines` | `data/binanceus/klines1d/<SYMBOL>.json` | `[openMs, close, quoteVolume, trades]`, completed days only |
| Reference price, one-second bars, from the start of recording | Coinbase Exchange `/products/{id}/trades` | `data/coinbase/bars1s/<day>/<BASE>.ndjson` | `[sec, open, high, low, close, volume, count, vwap]` |
| Reference history before recording began, one-minute candles | Coinbase `/products/{id}/candles` | `data/coinbase/candles1m/<day>/<BASE>.ndjson` | `[ms, open, high, low, close, volume]` |
| History for FAITH's intraday screens, five-minute candles, 2021-01 to 2025-12 (2026 is FAITH's holdout and is not fetched), one-off by hand (`backfill.yml`) | Coinbase `/products/{id}/candles` | `data/coinbase/candles5m/<BASE>/<YYYY-MM>.ndjson.gz` | `[ms, open, high, low, close, volume]` |
| Offshore perp history, signals only (trading stays on US venues), five-minute candles, 2021-01 to 2025-12 (2026 is not fetched), one-off by hand (`backfill-binance-um.yml`) | Binance USD-M archive `data.binance.vision/data/futures/um/monthly/klines/<SYM>/5m/`, sha256 checked | `data/binance-um/klines5m/<SYM>/<YYYY-MM>.ndjson.gz` | `[openMs, open, high, low, close, volume, quoteVolume, trades, takerBuyVolume]` |
| Offshore perp funding, same symbols and range | `.../monthly/fundingRate/<SYM>/` | `data/binance-um/funding/<SYM>/<YYYY>.ndjson.gz` | `[fundingTimeMs, rate]` (`calc_time`, `last_funding_rate` as served) |
| Offshore perp open interest and long/short ratios, five-minute, same symbols and range | `.../daily/metrics/<SYM>/` | `data/binance-um/metrics/<SYM>/<YYYY-MM>.ndjson.gz` | `[ms, sumOpenInterest, sumOpenInterestValue, topTraderAccountLS, topTraderPositionLS, accountLS, takerBuySellVolRatio]` (from `count_toptrader_long_short_ratio`, `sum_toptrader_long_short_ratio`, `count_long_short_ratio`, `sum_taker_long_short_vol_ratio`; `null` where a file has no value). Symbols BTCUSDT, ETHUSDT, SOLUSDT, DOGEUSDT; months or days the archive lacks are listed in `data/binance-um/<kind>/<SYM>/missing.json` |
| P-OF2 open-interest research data, for a possible FAITH study (research only; trading stays on US venues; 2026 is FAITH's holdout and is refused), 2022-01 to 2025-12, one-off by hand (`pof2.yml`). Coins picked by a fixed rule from exchange metadata only (Binance.US USD/USDT base, Coinbase USD product with 2022-01 candles, USD-M daily metrics for 2022-01-01 and 2025-12-31, at most 15 by earliest perp archive month): rule, each step's candidates and drop reasons, and the final list in `data/universe/pof2.json`; coverage from timestamps in `data/universe/pof2-coverage.json` | Coinbase `/products/{id}/candles`; Binance USD-M archive `.../daily/metrics/<SYM>/`, sha256 checked | `data/coinbase/candles5m/<BASE>/<YYYY-MM>.ndjson.gz`; `data/binance-um/metrics/<SYM>/<YYYY-MM>.ndjson.gz` | candles as above; metrics **two columns only**, `[ms, sumOpenInterest]` (`sum_open_interest`), unlike the four symbols above; missing days in each `missing.json`. Coins: ADA, BCH, ETC, LINK, LTC, XLM, ATOM, BAT, DASH, XTZ, ZEC, ALGO, COMP, KNC, ZRX (perps `<BASE>USDT`); 130.1 MB gzipped (candles 83.7 MB, metrics 46.5 MB) |
| Second reference, one-second bars | Kraken `/0/public/Trades` | `data/kraken/bars1s/<day>/<BASE>.ndjson` | as Coinbase |
| Kalshi perpetuals, crypto markets, once a run | `external-api.kalshi.com/trade-api/v2/margin/markets` | `data/kalshi/perps/<day>.ndjson` | `[ms, [market objects as served]]` |
| Kalshi perpetuals, funding-rate history for every market, once a day | `external-api.kalshi.com/trade-api/v2/margin/funding_rates/historical` (public) | `data/kalshi/funding/history.json`, rewritten daily | `{at, json}`, the answer as served |
| Endpoint probes and fee/terms page hashes, daily | see `recorder/config.json` | `data/probes/<day>.ndjson`, `data/pages/<name>/<day>.html` on change | |
| Run health: calls, non-200 answers, per-task results | | `data/health/<day>.ndjson`, `state/last-run.json` | |
| Known holes in a tape | | `data/gaps/<day>.ndjson` | |

**Binance.US universe.** A pair is recorded if its 24-hour trade count was 5,000 or fewer the first time the recorder saw it, and stops being recorded if the count later goes above 50,000. Each decision and its count is in `state/state.json` under `binanceus.universe`. The rule reads only the count at that moment, but it was first applied on 2026-11-10 to a history that starts on 2026-04-22, so a pair that was thin in spring and busy on 2026-11-10 has no tape: the daily candles (kept for every pair) show which pairs that touches.

**Reference bases** are BTC, ETH, SOL, USDT, USDC and the base of every universe pair that Coinbase or Kraken quotes in dollars (Kraken: at most 15, the five above first). One-minute history before recording began is kept for the five, then for every base with a Binance.US pair that averaged 50 or more trades a day in some calendar month, busiest first.

## Guarantees and limits

- **Complete tapes.** Every stream resumes from a cursor in `state/state.json`, so a late or skipped scheduled run loses nothing on Binance.US. Coinbase pages back at most 120 pages a run; a longer outage on a busy product is written to `data/gaps/` with the missing ids, never silently skipped.
- **Settled seconds only.** A one-second bar is written only once the fetch happened at least 5 s after that second ended, and each second is written once.
- **The book is a snapshot**, once a run. It measures spread and depth at that moment, not what happened between runs.
- **Quotes are sampled, and only inside sessions.** Between a quote session's `s` and `e` rows, except at failed polls (`x`) and after a `c` row, every change of best price is recorded at 5 s resolution; a price that came and went between two polls is not, and a size change alone is not a row. Outside sessions (the record job itself, runner start-up) nothing is known, and a session killed before its end has no `e` row. From 2026-11-12 a bridge workflow polls across each half-hourly handover into `data/<venue>/quotes-bridge/`, same files and rows; merge the two streams by time for continuous quotes. `ms` is when the answer arrived. Each session may write 3 MB raw of Binance.US changes and 250 kB raw per added venue; past that only the 10-minute snapshots are written, so a busy day loses detail evenly across its hours rather than every afternoon. Each venue has its own loop, session and failure count: one that fails or is rate limited stops alone, for the rest of that session. A venue whose first answer in a session never came has no file for that session. Coinbase products are read one after another, so a Coinbase `ms` is when the poll's last answer arrived, under about a second after its first.
- **Run times are not exact.** GitHub may start a scheduled run late or skip one. Nothing here depends on the run time.
- **Runner location.** GitHub-hosted runners are US data centres in practice, not by promise. A venue that refuses the runner shows its answer in `data/health/` and `data/probes/`.

## Data kept in release assets

From 2026-11-12, quote session files (`data/*/quotes/`) and one-second bars (`data/*/bars1s/`) are no longer committed. Each run packs what it wrote into one `tar.gz` per UTC day and uploads it to the release tagged `tape-<day>`; the repository keeps one line per bundle in `manifests/<day>.ndjson` (asset name, run, size, sha256, and every file with its path, mode and sha256). Bar day files committed before the switch stay in git, and later rows are bundled as appended chunks. To read these streams, first rebuild them in a checkout you do not commit from:

```
node recorder/materialize.mjs .                      # every day; --from/--to YYYY-MM-DD to limit
node recorder/materialize.mjs . --check              # assets no manifest names, lines with no asset, parked chunks
node recorder/materialize.mjs . --adopt              # write the line for an asset that has none (commit manifests/ after)
```

Every bundle and file is checked against its sha256 before anything is written, a file entry never overwrites different content, and a bar chunk goes only at its recorded offset, after the chunk before it, once. A bars upload that fails leaves its chunk in `state/offload-retry/` for the next run, so git never holds bar rows out of place, and a run adopts a lost bar line of its own day before it sends. A lost line from a day's last run needs `--adopt` by hand; `--check` also lists days whose bars are held (`state/offload-held.json`). The result is byte for byte what the old commits would have held. Reason: these two streams grew the repository by about 27 MB a day, which would pass GitHub's recommended 1 GB in late November 2026.

`node recorder/coverage.mjs . [--from D] [--to D] [--venue V] [--json]` estimates quote coverage per venue and day from file names only, without opening any quote file, and flags days under 720 minutes.

## Running locally

```
node --test test/*.test.mjs
node recorder/record.mjs .
```

Changes to what is recorded go in `CHANGES.md` with the date, because a universe or format change mid-tape matters to anyone reading the data.
