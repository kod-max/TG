# QML H4 Rulebook — auditable working definition

This document defines the current detector so that results can be tested. It is not a claim that this is the only canonical QML definition or that it is profitable.

## Instruments and timeframe

Only `XAU/USD` and `EUR/USD`. Structure and model confirmation are H4-only. The detector uses closed H4 candles; it does not emit LTF trade setups.

## Confirmed pivots

A pivot high/low uses `PIVOT_LEFT` candles to its left and `PIVOT_RIGHT` candles to its right. It is not available until the right-side bars close. Equal-high/low ties are handled by the detector's pivot normalization. No pivot is considered confirmed on the candle where it first appears.

## Bearish QML

1. A = confirmed swing high.
2. B = following confirmed swing low.
3. C = following confirmed swing high that sweeps A by at least `MIN_HEAD_SWEEP_ATR × ATR(14)`.
4. C must have completed its right-side pivot confirmation bars.
5. The model is confirmed only on the first subsequent closed H4 candle with close below B.
6. QML entry = A (the original high) and is a limit retest from below.
7. Invalidation = beyond C; stop = C + `SL_ATR_BUFFER × ATR(14)`.

## Bullish mirror

1. A = confirmed swing low.
2. B = following confirmed swing high.
3. C = following confirmed swing low that sweeps below A by the configured ATR threshold.
4. Wait until C's right-side bars confirm.
5. Model confirms on the first subsequent closed H4 candle with close above B.
6. Entry = A, a limit retest from above.
7. Invalidation = below C; stop = C − ATR buffer.

## Freshness and persistence

- Model formation time = A pivot timestamp.
- Confirmation time = BOS candle close time (candle open timestamp + 4h).
- A candidate is eligible only if confirmed after the persisted per-symbol scan watermark and younger than `SETUP_MAX_AGE_H4_BARS`.
- On first boot, the watermark is initialized to the latest closed H4 bar; old history is not sent as new alerts.
- A deterministic SHA-256-derived Signal ID is stored as the SQLite primary key.
- The BOS candle and all later closed H4 bars are checked for entry/TP1 touches; any such touch prevents republishing the candidate as a new pending setup. Current quote must still be on the retest-waiting side of entry.
- Pending signals expire at confirmation time + configured lifetime. Existing SQLite records are restored rather than recreated from historical scans.

## Targets and risk

- SL is outside the head extremum plus/minus an ATR buffer.
- Candidate targets are confirmed H4 swing lows for SELL or swing highs for BUY, with the broken B level included as a structural candidate.
- Targets must be in the correct order and satisfy minimum RR floors (1.0, 1.5, 2.0 respectively).
- A spacing threshold based on ATR rejects clustered levels; no fixed price distance is invented to force three targets.
- If fewer than three structural levels qualify, no signal is sent.
- RR = absolute(entry − target) / absolute(entry − stop). Directional order and stop-side checks are validated before insertion.
- `target_basis` records the BOS level or confirmed H4 swing origin in the signal payload.

## Monitoring caveat

The current monitor samples Twelve Data `/price` every 15 minutes by default. It deduplicates event IDs and persists status. If stop and target levels are both crossed between quote samples, price-only data cannot prove the intraperiod sequence; exact attribution needs a more granular data feed. This is stated as a data limitation, not resolved by guessing.
