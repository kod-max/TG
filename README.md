# QML H4 Signal Bot — audit revision

Telegram **signal-only** bot (no broker orders) for exactly two instruments:

- `XAU/USD` — Gold spot
- `EUR/USD` — EUR/USD

Primary structural timeframe: H4. No independent LTF signals are generated.

## Important limitations

This is an audited development revision, not a validated profitable strategy or a guarantee of live-market correctness. No authenticated Twelve Data request or Render deployment can be verified without the owner's credentials and service access. Run forward testing in a private Telegram chat before relying on alerts.

The QML definition in `QML_RULEBOOK.md` is explicit and testable, but still a strategy hypothesis. Targets are selected from confirmed H4 swing structure and a minimum spacing filter; if three levels do not qualify, the candidate is rejected rather than padded with arbitrary distances.

## Local setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN='...'
export TELEGRAM_CHAT_ID='...'
export TWELVE_DATA_API_KEY='...'
python qml_h4_bot.py
```

Do not put secrets in source or commit `.env`, local config files, logs, or the SQLite DB. Use Render Environment Variables for deployed secrets.

## Render

This project is configured as a **Render Web Service (Free)** because you chose that service type. Telegram still uses `getUpdates` long polling for commands, while a small HTTP server listens on `0.0.0.0:$PORT` and exposes `/health` for Render health checks and an uptime monitor. The HTTP handler is separate from the market/Telegram loop and does not create a second scanner.

Build command: `pip install -r requirements.txt`

Start command: `python qml_h4_bot.py`

Set the three secret environment variables in Render. The Free Web Service has an ephemeral filesystem and cannot attach a persistent disk. Therefore SQLite state under `/tmp` is lost on a spin-down, restart, or redeploy. This is a material limitation for signal deduplication and active-signal recovery; for durable state, use an external database (the free Render Postgres option currently has its own expiry/limits) or a paid persistent disk. Free web services also spin down after 15 minutes without inbound traffic; an external uptime monitor may ping `/health`, but this is not a guarantee of uninterrupted operation and Render may restart the service. Do not treat the free tier as production-grade trade monitoring.

## Lifecycle and data behavior

- H4 bars are filtered to closed bars only; one history request per symbol is made per scan cycle.
- Quotes are cached once per symbol within each monitor cycle, even if multiple signals are open for that instrument.
- A first-run per-symbol scan watermark is stored; startup establishes a baseline instead of sending the historical backlog.
- Subsequent scan candidates must be confirmed after the persisted watermark and be within the configured lifetime.
- Signal IDs are deterministic and unique in SQLite. State survives process restarts when the database is on persistent storage.
- Entry is a QML retest limit: bearish price should remain below QML while waiting to retest upward; bullish price should remain above QML while waiting to retest downward.
- The BOS candle and subsequent closed H4 bars are checked for entry/TP1 touches; a touched setup is not republished as a new pending alert.
- Monitoring uses `/quote` with a 1-minute interval at 15-minute default cadence and validates `last_quote_at` (20-minute max age by default). Stale quotes are rejected, not relabeled with the local request time. Quote sampling still cannot reconstruct exact tick-by-tick order if stop and targets are crossed between polls; use a more granular licensed data source if exact sequence attribution is required.

## Approximate API-credit budget

Twelve Data documents `/time_series` and `/quote` at 1 credit per symbol. With 2 symbols, 6 H4 scans/day, one H4 history request and one scan-time quote per symbol, plus `/quote` monitoring every 15 minutes:

- H4 history: 2 × 6 = 12 credits/day
- Scan-time spot checks: 2 × 6 = 12 credits/day
- Monitor quotes: 2 × 4 × 24 = 192 credits/day
- Approximate total: **216 credits/day**, before retries, startup, or any user commands.

Actual credit accounting and plan access must be checked in the user's Twelve Data account. Exactly one API key is used. Set `TWELVE_DATA_DAILY_CREDIT_LIMIT` to the actual daily quota shown in the account; the bot stops requesting at 80% of that configured limit to retain a reserve; usage is stored in SQLite so it survives worker restarts. A value of `0` disables the daily guard because the account quota is unknown. No multiple-key quota pooling or limit evasion is implemented.

## Tests

```bash
python -m py_compile qml_h4_bot.py test_qml_h4_bot.py
python -m unittest -v
```

Tests are unit-level; they are not a historical strategy backtest. To report real historical performance, export provider OHLC data and run a no-lookahead event-driven backtest with spread/slippage assumptions.
