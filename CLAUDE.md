# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-process Polymarket arbitrage/market-making bot (`polymarket_bot_v2.py`, "V2"). It polls Polymarket's
Gamma and CLOB REST APIs on a fixed interval (`LOOP_SECONDS`), scans for arbitrage opportunities across three
strategies, and (in real mode) submits Fill-or-Kill batch orders via `py-clob-client`. There is no test suite,
build step, or package structure — it's an operational script deployed via pm2, not a library.

Despite the docstring's claim of using a WebSocket feed, the current implementation is REST-polling only
(`scan_all()` → `get_active_events()`/`get_active_markets()` each cycle); `websockets` is imported but unused
in the main loop.

## Reglas de operación — NO NEGOCIABLES

- Este bot corre en producción bajo PM2 como `bot-polymarket`. Reiniciar solo con:
  `pm2 restart bot-polymarket --update-env`, y después SIEMPRE `pm2 save`. Los restarts los ejecuta el
  usuario, no Claude.
- NUNCA cambiar `DRY_RUN`, `MAX_CAPITAL` ni límites de riesgo sin autorización explícita del usuario.
- Antes de editar `polymarket_bot_v2.py`: backup con timestamp. Diff antes de aplicar. `ast.parse` después.
- Hito 15 de julio 2026: decisión go/no-go para piloto con $20 reales.
  Criterio reencuadrado (auditoría 2026-07-06): el go/no-go NO se decide por WR de DRY_RUN (sintético). Se
  decide por: (1) fix de fills parciales implementado y validado, (2) fontanería estable. El piloto de $20
  es un instrumento de MEDICIÓN (tasa de fill FOK multi-pata, slippage real, fees/gas reales), no un
  escalamiento.

## Hallazgos de la auditoría 2026-07-06 (leer antes de interpretar métricas)

- El `avg_slippage_pct` (~4.73%) es un ARTEFACTO del DRY_RUN, no slippage real: `record_slippage()` solo
  registra 0% (wins exactos) o 30-40% (pérdidas sintéticas del F2). Nunca se ha ejecutado una orden real.
- El WR de DRY_RUN (~92%) NO valida rentabilidad económica: el modelo F2 garantiza expectativa positiva por
  construcción. El DRY_RUN valida fontanería (23 días sin crashes), no el edge.
- 100% del flujo real de oportunidades es NEGRISK; los scanners `binary` y `near_resolution` nunca han
  producido una oportunidad ejecutable con los filtros actuales.
- BLOQUEANTE PARA GO-LIVE identificado: `execute_opportunity()` no verificaba el estado de fill por pata en
  la respuesta del batch. Un fill parcial (normal en CLOB) se registraba como win completo con profit
  teórico, dejando posición direccional desnuda con capital real. FOK garantiza atomicidad por orden, NO
  entre patas. **Fix implementado 2026-07-06** (PUNTO CIEGO #5 — ver arquitectura §3/§10) y validado con un
  harness offline (`test_fill_classification.py`, 6/6 casos OK). Al mismo tiempo se encontró que el código
  real de trading nunca se había podido ejecutar en absoluto: `OrderArgs(time_in_force="FOK")` no es un
  parámetro válido del dataclass, y `client.create_and_post_orders()` no existe en `py-clob-client` — ambos
  corregidos como parte del mismo parche. **Sigue pendiente de validar contra fills reales** — el bot sigue
  en `DRY_RUN=true`, así que este camino de código aún no se ha ejercitado con dinero real; eso es
  justamente lo que mide el piloto de $20.

## Running / operating the bot

- Python env: `venv/` (Python 3.12, stdlib venv). Key deps: `py_clob_client`, `web3`, `websockets`, `requests`,
  `python-dotenv`. No `requirements.txt` — check `venv/lib/python*/site-packages` for what's actually installed.
- Config is entirely via `.env` (loaded with `load_dotenv(override=True)`). **`.env` and `.env.*` are
  read-denied by `.claude/settings.json`** — do not attempt to read secrets (PRIVATE_KEY, FUNDER_ADDRESS,
  TELEGRAM_BOT_TOKEN). When asked to change a config value, edit `.env` by key name without echoing its
  current secret values. Since config is read once at import time, changes require a restart to take effect
  — and per the rules above, only the user restarts.
- State persists to `state_polymarket_v2.json` (path configurable via `STATE_FILE`), written atomically
  (tmp file + `os.replace`). `.bak.*` files in the repo root are manual snapshots from prior fix runs, not
  automatically rotated.
- Logs go to `logs/poly_v2_YYYY-MM-DD.log`, one file per day, in addition to stdout. Because `.env` is
  read-denied, the authoritative way to check *current live config values* (MIN_ROI_PCT, MAX_TRADES_PER_DAY,
  COOLDOWN_SECONDS, etc.) is the startup banner the bot logs on every restart (`grep` for "Calidad mínima",
  "Liquidez mín", etc. in the most recent daily log), or empirically from trade timestamps/counts — not by
  reading `.env` directly.
- There's no lint/format/test tooling configured for this repo — verifying a change means reading the diff
  carefully and, if behavior needs checking, running the script directly in `DRY_RUN` and watching the logs.
- `fix_polymarket.py`'s "backup, diff, `ast.parse`, then the user restarts" sequence is the established
  protocol for any patch to `polymarket_bot_v2.py` — follow it for future fixes too.

## Architecture (single file: `polymarket_bot_v2.py`, ~1600 lines)

The file is organized top-to-bottom as pipeline stages, all module-level functions operating on a plain
`dict` state object (no classes except the `Opportunity` dataclass). Reading order to understand a change:

1. **Config block (top of file)** — every tunable is `os.getenv(...)` with an inline default; this doubles
   as the documentation of all available `.env` keys. Comments here reference numbered "PUNTO CIEGO" (blind
   spot) fixes and "FIX #N" annotations that describe *why* a value exists — read them before changing a
   threshold.
2. **State** (`DEFAULT_STATE`, `load_state`/`save_state`) — the bot is fully stateful across restarts:
   capital, PnL, per-strategy stats, dedup history, slippage history, balance-drift tracking, and pending
   compound transfers all live in `state_polymarket_v2.json`. `trade_history` and `slippage_history` are
   rolling windows (last 50 / last 20 respectively) — full history must be reconstructed from `logs/` if
   needed (see the 2026-07-06 audit method: `EXEC` lines paired with their `SLIPPAGE` line). When adding a
   new tracked metric, add its default to `DEFAULT_STATE` too, or old state files will silently lack the key.
3. **"PUNTO CIEGO" (blind spot) subsystems** — four self-contained guardrails added after real post-mortems,
   each documented with the incident that motivated it:
   - `check_balance_drift` — compares JSON-tracked capital against on-chain balance; **halts the bot**
     (`balance_halted`) if drift ≥ `BALANCE_DRIFT_HALT`. This is the existing precedent/pattern for the
     "auto-pause on threshold" guardrail design discussed for partial-fill handling.
   - `estimate_slippage`/`adjust_profit_for_slippage`/`record_slippage` — a slippage *model* used to filter
     opportunities pre-trade, and a slippage *tracker* (`record_slippage`, called from `register_trade`)
     that compares each trade's expected vs. realized ROI to build `avg_slippage_pct`. In DRY_RUN this
     tracker only ever sees two possible diffs (0%, or the F2 synthetic-loss ROI) — see the audit findings
     above before trusting this number as market slippage.
   - `check_fee_changes`/`fetch_current_fee` — periodically polls the CLOB fee-rate endpoint and
     auto-updates `state["fee_current"]` if Polymarket's real fee drifts from `TAKER_FEE_PCT`.
   - `execute_compound_transfer` — accumulates the wallet-bound half of profits and only fires a real
     on-chain withdrawal once `COMPOUND_MIN_TRANSFER` is reached (gated by `COMPOUND_TRANSFER_ENABLED`).
   - `register_execution_outcome` (added 2026-07-06, PUNTO CIEGO #5) — tracks a rolling window (last 20) of
     real-trading execution outcomes (`full`/`partial`/`kill`) and **halts the bot** (`execution_halted`) if
     the problem rate (`partial`+`kill`) reaches `MAX_PARTIAL_FILL_RATE` (default 30%) once the window has
     ≥`MIN_EXECUTION_SAMPLES` (default 5) attempts. Only active when `DRY_RUN=false` — meaningless otherwise
     since there are no real fills to classify. New env vars: `MAX_PARTIAL_FILL_RATE`, `MIN_EXECUTION_SAMPLES`,
     `UNWIND_MIN_PRICE`. New `DEFAULT_STATE` fields: `execution_attempts`, `partial_fill_count`,
     `total_kill_count`, `execution_outcome_window`, `execution_halted`, `open_positions`.
4. **Deduplication** (`is_duplicate`/`register_execution`) — keyed by `event_id` (fallback: truncated title)
   in `state["executed_today"]`, reset daily. Exists because a single mispriced event was re-executed 193×/day
   before this existed — any strategy scanner must call `is_duplicate` before treating an opportunity as new.
5. **Risk gate** (`can_trade`) — a single chokepoint checked both before scanning and again between each
   trade within a cycle (`opps[:3]` per cycle, re-checked per-opportunity). Adding a new safety limit means
   adding a branch here, not scattering checks elsewhere.
6. **Fee/profit math** (`profit_after_fees`) — the canonical post-fee profit calculation (taker fee on
   payout + per-order gas), used by all three scanners. `MIN_ROI_PCT`/`MAX_ROI_PCT` bound realistic
   opportunities (extremely high ROI is treated as a signal of a stale/illiquid book, not a great trade).
7. **Liquidity filter** (`check_liquidity`) — queries CLOB order-book depth for a token and requires
   available depth ≥ `MIN_BOOK_DEPTH_RATIO` × position size; treated as a hard reject on API failure
   ("fail closed").
8. **Scanners** — three independent strategies, each returning `list[Opportunity]`, all following the same
   shape (dedup check → parse prices → profitability check → ROI sanity check → liquidity check → slippage
   adjustment → quality score → append):
   - `scan_negrisk` — multi-outcome events where `sum(yes_prices) < 1.0`. **This is the only strategy that
     has ever produced an executed trade** — 100% of the 282 live trades to date.
   - `scan_binary` — two-outcome markets where `YES + NO < 0.975`. Never produces an executable opportunity
     under current filters (confirmed via `SCAN COMPLETO` log lines showing `binary=0` every cycle).
   - `scan_near_resolution` — markets resolving within `NEAR_RESOLUTION_HOURS` priced above
     `NEAR_RESOLUTION_THRESHOLD`. Same — `near_res=0` every cycle observed so far.
   - `scan_all` merges + sorts by `(quality_score, roi_pct)` desc; this ordering is what `main()`'s
     `opps[:3]` executes each cycle.
9. **Quality score** (`compute_quality_score`) — a 0–100 heuristic (ROI weight, liquidity, near-resolution
   bonus, fewer-markets bonus, absolute profit) gating execution via `MIN_QUALITY_SCORE`. If you change
   scoring weights, note that `MIN_QUALITY_SCORE` was tuned against the current weights.
10. **Execution** (`execute_opportunity`) — real path signs one order per market leg via `client.create_order()`,
    wraps each as `PostOrdersArgs(order=signed_order, orderType=OrderType.FOK)`, and submits in batches of
    ≤15 via `client.post_orders()`. **(2026-07-06: this replaced a real-trading path that had never worked —
    `OrderArgs` has no `time_in_force` field, and `client.create_and_post_orders()` doesn't exist in
    `py-clob-client`; both calls would have raised before ever reaching the exchange.)**

    The batch response is flattened and passed to `classify_batch_result(opp, resp_list)` (pure function,
    no side effects — see `test_fill_classification.py`), which categorizes the outcome per NegRisk arb:
    - **`full`** (all legs `status=="matched"`) — the only case that returns the full theoretical
      `opp.profit_net`, same as before.
    - **`kill`** (0 legs filled) — no exposure was taken; returns `profit_usdt=0`, does not count as a
      market loss (`consecutive_losses` untouched), but does feed the `register_execution_outcome` guardrail.
    - **`partial`** (some legs filled, others killed — FOK guarantees atomicity *per order*, not *across*
      the legs of one arb) — routed to `handle_partial_fill()`, which attempts one unwind sell per filled
      leg (`attempt_unwind()`, respecting `UNWIND_MIN_PRICE` as a safety floor), sends an immediate Telegram
      alert with the per-leg detail (filled/killed, unwind price, realized loss), and registers any leg that
      *can't* be unwound into `state["open_positions"]` as a residual naked position rather than fabricating
      a closed-trade profit for it. `register_trade()` uses `result["actual_cost"]` (real cost of the
      filled/unwound legs) instead of `opp.cost_total` (the full theoretical arb cost) as the ROI-realized
      denominator specifically for this case, so a partial doesn't get a diluted/misleading ROI.

    The `DRY_RUN` path is unchanged by this fix and does *not* just return success either: it injects a
    synthetic loss (`loss_prob` 6–18%, scaled inversely with ROI, losing 25% of cost) to make simulated
    win-rate/PnL resemble production — see `fix_polymarket.py`'s F2 for the rationale, and the 2026-07-06
    audit for why this makes DRY_RUN win-rate/slippage stats uninformative about real execution risk. The
    new partial/kill classification and guardrail only engage when `DRY_RUN=false`, so none of this has been
    exercised against a real fill yet — that's what the $20 pilot is for.
11. **Compounding** (`apply_compound`) — splits realized profit `COMPOUND_TO_WALLET_PCT` /
    `COMPOUND_TO_CAPITAL_PCT`, grows `active_capital` uncapped (see fix F1 — it used to incorrectly cap at
    `MAX_CAPITAL`), and triggers `execute_compound_transfer`.
12. **`main()`** — the loop: reset-daily → heartbeat → balance-drift check → fee check → `can_trade` gate →
    `scan_all` → execute top 3 opportunities (re-gating `can_trade` between each) → cycle summary → sleep
    `LOOP_SECONDS`.

## `fix_polymarket.py`

A one-shot, idempotent patch script (not a module to import) that was run once (2026-06-22 23:38) against a
live VPS deployment to apply three surgical fixes (F1–F3, described in its own header) plus `.env` tuning
and a partial state reset. It works by asserting exact source-string anchors exist, replacing them,
re-parsing with `ast` to confirm valid syntax, and rolling back from its own backup on any failure. **Its
fixes are already merged into `polymarket_bot_v2.py`** — treat this file as a historical/reference record of
*why* certain code shapes look the way they do (each `[F1]`/`[F2]`/`[F3]` comment in the main file traces
back here), not as something to re-run. It is also the reference implementation for the
backup→diff→`ast.parse`→handoff-to-user-restart protocol expected for future patches.

`test_fill_classification.py` (added 2026-07-06) is the same kind of standalone, no-framework script, but
it's a *reusable* offline check rather than a one-shot patch: it imports `polymarket_bot_v2` as a module
(safe — nothing runs at import time beyond config/env reads; `main()` is guarded by
`if __name__ == "__main__"`) and asserts `classify_batch_result()` categorizes 6 hand-built batch responses
correctly (full fill, total kill, partial 1-of-2, partial 1-of-3, unexpected order status, malformed/missing
fields). Run it with `python3 test_fill_classification.py` any time `classify_batch_result` changes — no
network, no `DRY_RUN` flip, no PM2 restart needed.

## Conventions worth preserving

- Config values are read once at import time into module-level constants — there is no hot-reload; changing
  `.env` requires a process restart (`pm2 restart bot-polymarket --update-env`, run by the user).
- Comments are in Spanish and frequently cite the specific production incident or external source that
  motivated a piece of logic (e.g. "log del día 3 mostró que FIFA se ejecutó 193 veces"). Preserve this
  style — it's load-bearing documentation, not clutter — when editing nearby code.
- Bare `except: continue`/`except: pass` is used deliberately in scanner loops so one malformed market/event
  doesn't abort the whole scan; don't "clean this up" into broad exception handling removal without
  preserving that resilience.
- Telegram (`send_telegram`) is the out-of-band alerting channel for heartbeats, balance halts, fee-change
  alerts, and trade results — any new critical condition (e.g. a partial-fill guardrail) should likely
  notify through it too, following the `check_balance_drift`/`balance_halted` pattern as precedent.

## Bitácora de sesiones
Al cierre de cada sesión con cambios significativos (parches, configuración, hallazgos), ofrecer al usuario generar la entrada de bitácora del día en /root/bitacora/AAAA-MM-DD.md con formato: Qué pasó / Decisiones / Aprendizajes / Pendientes. Si el archivo del día ya existe, agregar una sección nueva con la hora, no sobrescribir.
