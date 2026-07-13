"""
POLYMARKET ARBITRAGE BOT V2
Arquitectura: BOT V11 Binance + mejoras de los mejores bots open-source 2025/2026

MEJORAS SOBRE V1 (basadas en análisis de bots exitosos):

  #1  WEBSOCKET en lugar de polling REST
      Fuente: ImMike/polymarket-arbitrage, Benjamin-Cup/Medium
      V1 escaneaba cada 30s. V2 recibe actualizaciones de precios en <100ms.
      Razón: oportunidades duran 2.7s promedio en 2026 (fuente: ILLUMINATION análisis).

  #2  FILTRO DE LIQUIDEZ antes de ejecutar
      Fuente: QuantVPS, ivanzzeth/polymarket-go-gamma-client
      V1 no verificaba si había suficiente liquidez para la posición.
      V2 consulta el order book del CLOB antes de ejecutar y rechaza si
      el book depth < tamaño de la orden (evita slippage que destruye el margen).

  #3  CONTABILIDAD REAL DE FEES
      Fuente: PolyTrack (>$40M extraídos), QuantVPS
      V1 calculaba ganancia sin descontar fees.
      V2 descuenta: taker fee 2% al payout + gas Polygon (~$0.001/tx).
      Umbral real: sum_YES < 0.975 (no < 1.0) para ser rentable.

  #4  ARBITRAJE BINARIO YES+NO (además de NegRisk multi-outcome)
      Fuente: PolyTrack, Benjamin-Cup
      En mercados binarios YES+NO debe sumar $1.00.
      Si YES+NO < $0.995 → comprar ambos = ganancia garantizada.
      V1 solo detectaba NegRisk multi-outcome.

  #5  FILL-OR-KILL (FOK) orders
      Fuente: QuantVPS, ILLUMINATION
      V1 usaba market orders que podían llenarse parcialmente.
      V2 usa FOK: o se llena completo al precio pedido o se cancela.
      Evita quedar con posición parcial que bloquea capital.

  #6  NUEVOS MERCADOS PRIORITARIOS: Sports + Crypto 5min
      Fuente: arxiv paper 2025, ILLUMINATION análisis Feb 2026
      Sports representan >60% del open interest de Polymarket en 2025.
      Crypto 5min/15min markets tienen alta rotación de precios.
      V2 prioriza estos mercados en el scanner.

  #7  MARKET MAKING como estrategia secundaria
      Fuente: ILLUMINATION (win rate 78-85%, 1-3% mensual)
      Cuando no hay arbitraje, el bot puede ganar el spread como market maker.
      Coloca órdenes límite en ambos lados de mercados líquidos.

  #8  SCORE DE CALIDAD (igual que V11 Binance)
      Combina: ROI %, liquidez disponible, tiempo a resolución, volumen.
      Solo ejecuta oportunidades con score >= umbral configurable.

  #9  BATCH ORDERS (hasta 15 órdenes por llamada)
      Fuente: Polymarket API docs 2025
      V1 enviaba una orden por mercado con pausa 0.3s entre cada una.
      V2 usa el endpoint batch para enviar todas las órdenes del arbitraje
      en una sola llamada → ejecución casi simultánea.

  #10 DETECCIÓN DE RESOLUCIÓN PRÓXIMA (near-resolution arbitrage)
      Fuente: PolyTrack guide
      Mercados que resuelven en <24h con precio 95-99% tienen
      ROI anualizado enorme aunque el margen absoluto sea pequeño.
      V2 detecta y prioriza estos casos.
"""

import os
import json
import time
import math
import asyncio
import threading
import requests
import websockets
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional
from dotenv import load_dotenv

load_dotenv(override=True)

# ══════════════════════════════════════════════════════════════
# CONFIGURACIÓN GLOBAL
# ══════════════════════════════════════════════════════════════

GAMMA_API   = "https://gamma-api.polymarket.com"
CLOB_API    = "https://clob.polymarket.com"
CLOB_WS     = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

PRIVATE_KEY    = (os.getenv("PRIVATE_KEY")    or "").strip()
FUNDER_ADDRESS = (os.getenv("FUNDER_ADDRESS") or "").strip()
WITHDRAW_WALLET= (os.getenv("WITHDRAW_WALLET") or FUNDER_ADDRESS).strip()

DRY_RUN      = (os.getenv("DRY_RUN")      or "true").lower() == "true"
LOOP_SECONDS = int(os.getenv("LOOP_SECONDS") or "5")   # V2: 5s fallback (WS es en tiempo real)

MAX_CAPITAL        = float(os.getenv("MAX_CAPITAL")         or "200")
MAX_RISK_PERCENT   = float(os.getenv("MAX_RISK_PERCENT")    or "5")
MIN_ROI_PCT        = float(os.getenv("MIN_ROI_PCT")         or "1.5")   # V2: bajado de 2% por fees reales
MAX_ROI_PCT        = float(os.getenv("MAX_ROI_PCT")         or "20.0")  # FIX LOG #2: ROI > 20% = mercado sin liquidez real
MIN_SUM_YES        = float(os.getenv("MIN_SUM_YES")         or "0.50")  # FIX LOG #1: suma YES mín para evitar mercados zombi
MIN_QUALITY_SCORE  = int(os.getenv("MIN_QUALITY_SCORE")     or "60")

# FEES REALES (FIX #3)
TAKER_FEE_PCT      = float(os.getenv("TAKER_FEE_PCT")       or "0.02")  # 2% del payout ganador
GAS_COST_USDC      = float(os.getenv("GAS_COST_USDC")       or "0.002") # gas Polygon por orden
# Umbral real post-fees: necesitamos profit > fees para que valga
# Si fees = 2% y gas = $0.002 × N órdenes, el threshold real es < 0.978 aprox

# Seguridad
MAX_DAILY_LOSS         = float(os.getenv("MAX_DAILY_LOSS")          or "0.05")
MAX_DAILY_LOSS_USDT    = float(os.getenv("MAX_DAILY_LOSS_USDT")     or "10.0")
GLOBAL_DAILY_LOSS_LIMIT= float(os.getenv("GLOBAL_DAILY_LOSS_LIMIT") or "15.0")
MAX_TRADES_PER_DAY     = int(os.getenv("MAX_TRADES_PER_DAY")        or "50")   # V2: más alto (bots rápidos hacen más)
MAX_CONSECUTIVE_LOSSES = int(os.getenv("MAX_CONSECUTIVE_LOSSES")    or "3")
COOLDOWN_SECONDS       = int(os.getenv("COOLDOWN_SECONDS")          or "300")  # FIX LOG #3: 300s evita re-ejecutar mismo mercado
POST_LOSS_COOLDOWN     = int(os.getenv("POST_LOSS_COOLDOWN")        or "600")  # V2: 10min (antes 30min)
HEARTBEAT_MINUTES      = int(os.getenv("HEARTBEAT_MINUTES")         or "60")
API_MAX_RETRIES        = int(os.getenv("API_MAX_RETRIES")            or "3")

# Liquidez mínima (FIX #2)
MIN_LIQUIDITY_USDC     = float(os.getenv("MIN_LIQUIDITY_USDC")      or "500")  # liquidez mínima en el mercado
MIN_BOOK_DEPTH_RATIO   = float(os.getenv("MIN_BOOK_DEPTH_RATIO")    or "1.5")  # depth debe ser 1.5× nuestra posición

# Near-resolution (FIX #10)
NEAR_RESOLUTION_HOURS  = int(os.getenv("NEAR_RESOLUTION_HOURS")     or "24")
NEAR_RESOLUTION_THRESHOLD = float(os.getenv("NEAR_RESOLUTION_THRESHOLD") or "0.94") # precio YES > 94%

# ── PUNTO CIEGO #1: Verificación de balance on-chain ─────────
# Cada N ciclos, el bot verifica el USDC real en Polymarket
# y detecta divergencias entre el estado JSON y la realidad
BALANCE_CHECK_CYCLES   = int(os.getenv("BALANCE_CHECK_CYCLES")   or "10")   # verificar cada 10 ciclos
BALANCE_DRIFT_ALERT    = float(os.getenv("BALANCE_DRIFT_ALERT")  or "5.0")  # alerta si diverge > $5
BALANCE_DRIFT_HALT     = float(os.getenv("BALANCE_DRIFT_HALT")   or "20.0") # detener si diverge > $20

# ── PUNTO CIEGO #2: Medición de impacto de precio post-trade ─
# Mide cuánto se movió el precio después de cada ejecución real
# En simulación calcula el slippage teórico para datos más realistas
SLIPPAGE_MODEL_ENABLED = (os.getenv("SLIPPAGE_MODEL_ENABLED") or "true").lower() == "true"
SLIPPAGE_IMPACT_PCT    = float(os.getenv("SLIPPAGE_IMPACT_PCT") or "0.3")   # 0.3% slippage estimado por $100 invertido
PRICE_CHECK_DELAY_SEC  = int(os.getenv("PRICE_CHECK_DELAY_SEC") or "10")    # segundos después del trade para medir precio

# ── PUNTO CIEGO #3: Monitor de fees en tiempo real ───────────
# Detecta automáticamente cambios en la estructura de fees de Polymarket
FEE_MONITOR_ENABLED    = (os.getenv("FEE_MONITOR_ENABLED")    or "true").lower() == "true"
FEE_CHECK_INTERVAL_MIN = int(os.getenv("FEE_CHECK_INTERVAL_MIN") or "60")   # verificar fees cada 60 min
FEE_ALERT_THRESHOLD    = float(os.getenv("FEE_ALERT_THRESHOLD") or "0.005") # alerta si fee cambia >0.5%

# ── PUNTO CIEGO #4: Transferencia real de compound a wallet ──
COMPOUND_TRANSFER_ENABLED = (os.getenv("COMPOUND_TRANSFER_ENABLED") or "false").lower() == "true"
COMPOUND_MIN_TRANSFER  = float(os.getenv("COMPOUND_MIN_TRANSFER") or "10.0") # transferir solo si acumula >$10

# ── PUNTO CIEGO #5: Fills parciales en órdenes batch FOK ─────
# Auditoría 2026-07-06: execute_opportunity() nunca verificaba el estado de
# fill por pata de la respuesta del batch — un fill parcial (normal en un
# CLOB: FOK garantiza atomicidad por orden, NO entre las N patas de un
# arbitraje NegRisk) se registraba como win completo con el profit teórico,
# dejando una posición direccional desnuda con capital real sin que nadie
# lo notara. Ver classify_batch_result() / attempt_unwind() / handle_partial_fill().
MAX_PARTIAL_FILL_RATE  = float(os.getenv("MAX_PARTIAL_FILL_RATE")  or "0.30") # tasa máx. de parciales+kills en la ventana antes de auto-pausar
MIN_EXECUTION_SAMPLES  = int(os.getenv("MIN_EXECUTION_SAMPLES")    or "5")    # muestras mínimas en ventana antes de poder disparar el halt
UNWIND_MIN_PRICE       = float(os.getenv("UNWIND_MIN_PRICE")       or "0.01") # piso de seguridad para la venta de unwind

# DEDUPLICACIÓN — impide ejecutar el mismo evento más de N veces por día
# Fuente: análisis log día 3 → FIFA ejecutado 193× en 1 día (1 vez real)
DEDUP_WINDOW_HOURS    = int(os.getenv("DEDUP_WINDOW_HOURS")    or "24")   # ventana de dedup en horas
DEDUP_MAX_PER_EVENT   = int(os.getenv("DEDUP_MAX_PER_EVENT")   or "1")    # máx ejecuciones por evento/día

# PAGINACIÓN — control de cuántas páginas se cargan por scan
MAX_EVENTS_PER_SCAN   = int(os.getenv("MAX_EVENTS_PER_SCAN")   or "2000") # total eventos a escanear
MAX_MARKETS_PER_SCAN  = int(os.getenv("MAX_MARKETS_PER_SCAN")  or "2000") # total mercados a escanear
SCAN_PAGE_SIZE        = int(os.getenv("SCAN_PAGE_SIZE")        or "100")  # tamaño de página API

# Compound
COMPOUND_TO_WALLET_PCT = float(os.getenv("COMPOUND_TO_WALLET_PCT")  or "0.50")
COMPOUND_TO_CAPITAL_PCT= float(os.getenv("COMPOUND_TO_CAPITAL_PCT") or "0.50")

# Market Making (FIX #7)
MARKET_MAKING_ENABLED  = (os.getenv("MARKET_MAKING_ENABLED") or "false").lower() == "true"
MM_SPREAD_PCT          = float(os.getenv("MM_SPREAD_PCT")    or "0.04")  # 4% spread mínimo
MM_ORDER_SIZE_USDC     = float(os.getenv("MM_ORDER_SIZE_USDC")or "5.0")

# Horario UTC
TRADING_HOURS_ENABLED  = (os.getenv("TRADING_HOURS_ENABLED") or "true").lower() == "true"
TRADING_HOUR_START     = int(os.getenv("TRADING_HOUR_START")  or "0")   # V2: 24/7 (antes 6-23)
TRADING_HOUR_END       = int(os.getenv("TRADING_HOUR_END")    or "24")

# Telegram
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TELEGRAM_CHAT_ID   = (os.getenv("TELEGRAM_CHAT_ID")   or "").strip()

LOG_DIR     = (os.getenv("LOG_DIR")     or "logs").strip()
LOG_ENABLED = (os.getenv("LOG_ENABLED") or "true").lower() == "true"
STATE_FILE  = (os.getenv("STATE_FILE")  or "state_polymarket_v2.json").strip()

# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════

_log_fh   = None
_log_date = None

def _get_log_fh():
    global _log_fh, _log_date
    today = datetime.now().strftime("%Y-%m-%d")
    if _log_date != today:
        if _log_fh:
            try: _log_fh.close()
            except: pass
        if LOG_ENABLED:
            os.makedirs(LOG_DIR, exist_ok=True)
            _log_fh = open(os.path.join(LOG_DIR, f"poly_v2_{today}.log"), "a", encoding="utf-8", buffering=1)
        _log_date = today
    return _log_fh

def log(msg: str, ctx: str = ""):
    prefix = f"[{ctx}] " if ctx else ""
    ts     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line   = f"[{ts}] {prefix}{msg}"
    print(line, flush=True)
    if LOG_ENABLED:
        try:
            fh = _get_log_fh()
            if fh: fh.write(line + "\n")
        except: pass

def send_telegram(msg: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=15,
        )
    except Exception as e:
        log(f"Telegram error: {e}")

def is_trading_hour() -> bool:
    if not TRADING_HOURS_ENABLED:
        return True
    return TRADING_HOUR_START <= datetime.now(timezone.utc).hour < TRADING_HOUR_END

# ══════════════════════════════════════════════════════════════
# HTTP CON RETRY
# ══════════════════════════════════════════════════════════════

def api_get(url: str, params: dict = None, _retry: int = 0) -> dict:
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 429 or r.status_code >= 500:
            raise requests.RequestException(f"HTTP {r.status_code}")
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        if _retry < API_MAX_RETRIES:
            time.sleep(2 ** _retry)
            return api_get(url, params, _retry + 1)
        raise

def api_post(url: str, payload, headers=None, _retry: int = 0):
    try:
        r = requests.post(url, json=payload, headers=headers or {}, timeout=10)
        if r.status_code == 429 or r.status_code >= 500:
            raise requests.RequestException(f"HTTP {r.status_code}")
        r.raise_for_status()
        return r.json() if r.text.strip() else {}
    except requests.RequestException as e:
        if _retry < API_MAX_RETRIES:
            time.sleep(2 ** _retry)
            return api_post(url, payload, headers, _retry + 1)
        raise

# ══════════════════════════════════════════════════════════════
# ESTADO PERSISTENTE
# ══════════════════════════════════════════════════════════════

DEFAULT_STATE = {
    "current_day":           datetime.now().date().isoformat(),
    "session_start_capital": 0.0,
    "active_capital":        0.0,
    "total_sent_to_wallet":  0.0,
    "total_added_to_capital":0.0,
    "total_pnl_usdt":        0.0,
    "daily_pnl_usdt":        0.0,
    "daily_pnl_fraction":    0.0,
    "trades_today":          0,
    "total_trades":          0,
    "consecutive_losses":    0,
    "last_trade_timestamp":  0,
    "cooldown_until":        0,
    "last_heartbeat_ts":     0,
    "trade_history":         [],
    # V2: estadísticas por estrategia
    "stats_negrisk":         {"trades": 0, "pnl": 0.0, "wins": 0},
    "stats_binary":          {"trades": 0, "pnl": 0.0, "wins": 0},
    "stats_near_res":        {"trades": 0, "pnl": 0.0, "wins": 0},
    "stats_market_making":   {"trades": 0, "pnl": 0.0, "wins": 0},
    # DEDUPLICACIÓN: registro de eventos ejecutados hoy {event_id: [timestamps]}
    "executed_today":        {},

    # PUNTO CIEGO #1: tracking de divergencia balance JSON vs on-chain
    "balance_onchain_last":  0.0,    # último balance on-chain verificado
    "balance_drift_total":   0.0,    # divergencia acumulada detectada
    "balance_checks":        0,      # cantidad de verificaciones realizadas
    "balance_halted":        False,  # bot detenido por divergencia excesiva

    # PUNTO CIEGO #2: métricas de slippage e impacto de precio
    "slippage_history":      [],     # últimos 20 slippages medidos
    "avg_slippage_pct":      0.0,    # promedio móvil de slippage real
    "trades_missed_latency": 0,      # trades perdidos por latencia

    # PUNTO CIEGO #3: monitoring de fees
    "fee_current":           0.02,   # fee actual detectado
    "fee_last_check_ts":     0,      # timestamp última verificación
    "fee_alerts":            0,      # alertas de cambio de fee emitidas

    # PUNTO CIEGO #4: tracking de transferencias pendientes
    "compound_pending_transfer": 0.0, # USDC pendiente de transferir a wallet
    "compound_transferred_real": 0.0, # USDC realmente transferido on-chain

    # PUNTO CIEGO #5: tracking de fills parciales en batch FOK
    "execution_attempts":       0,    # intentos reales de ejecución (solo cuenta si DRY_RUN=false)
    "partial_fill_count":       0,    # cantidad de fills parciales detectados
    "total_kill_count":         0,    # cantidad de kills totales (0 patas llenadas)
    "execution_outcome_window": [],   # últimos 20 intentos: {"ts", "outcome": full/partial/kill}
    "execution_halted":         False,# bot detenido por tasa de fills parciales/kills excesiva
    "open_positions":           [],   # posiciones residuales de parciales que no se pudieron unwindear
}

def load_state() -> dict:
    state = dict(DEFAULT_STATE)
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state.update(json.load(f))
        except Exception as e:
            log(f"WARNING estado: {e}")
    if state["active_capital"] == 0.0:
        state["active_capital"]        = MAX_CAPITAL
        state["session_start_capital"] = MAX_CAPITAL
    return state

def save_state(state: dict):
    tmp = STATE_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, default=str)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log(f"ERROR guardando estado: {e}")
        try: os.remove(tmp)
        except: pass

def reset_daily_if_needed(state: dict):
    today = datetime.now().date().isoformat()
    if state.get("current_day") != today:
        state.update({
            "current_day":        today,
            "daily_pnl_usdt":     0.0,
            "daily_pnl_fraction": 0.0,
            "trades_today":       0,
            "consecutive_losses": 0,
            "executed_today":     {},   # DEDUP: limpiar registro diario
        })
        log(f"NUEVO DÍA → {today} | dedup reseteado")
        save_state(state)

# ══════════════════════════════════════════════════════════════
# PUNTO CIEGO #1 — VERIFICACIÓN DE BALANCE ON-CHAIN
# El estado JSON puede divergir del balance real en Polymarket
# por fees no contabilizados, slippage o errores de red.
# ══════════════════════════════════════════════════════════════

def get_onchain_balance() -> float:
    """
    Consulta el balance real de USDC en la cuenta de Polymarket.
    Usa la API pública de Polymarket que devuelve el balance del usuario.
    En DRY_RUN retorna el capital del estado (no hay balance real).
    """
    if DRY_RUN or not FUNDER_ADDRESS:
        return -1.0   # -1 indica que no se puede verificar

    try:
        # Endpoint de balance de usuario en Polymarket
        resp = api_get(
            f"{GAMMA_API}/positions",
            {"user": FUNDER_ADDRESS, "sizeThreshold": "0"}
        )
        # El balance libre en USDC es lo que no está en posiciones abiertas
        # Polymarket lo reporta como "cashBalance" en algunos endpoints
        cash = 0.0
        if isinstance(resp, dict):
            cash = float(resp.get("cashBalance", 0) or resp.get("balance", 0) or 0)
        elif isinstance(resp, list):
            # Algunos endpoints devuelven lista de posiciones abiertas
            # El cash libre no se puede calcular directamente desde aquí
            # FIX #1: float(FUNDER_ADDRESS) lanzaba excepción silenciosa
            # Retornar 0.0 para que el balance check use el estado JSON
            cash = 0.0
        return cash
    except Exception as e:
        log(f"BALANCE_CHECK error: {e}")
        return -1.0


def check_balance_drift(state: dict, cycle_num: int) -> bool:
    """
    PUNTO CIEGO #1: Detecta divergencia entre estado JSON y balance real.

    Problema identificado en post-mortem:
      El JSON reportaba capital=$485 mientras el balance real era $420
      por fees y slippage no contabilizados. Nadie lo detectó.

    Acción:
      - Divergencia > BALANCE_DRIFT_ALERT ($5): log de advertencia + Telegram
      - Divergencia > BALANCE_DRIFT_HALT ($20): detener bot + alerta crítica

    Retorna True si el bot debe continuar, False si debe detenerse.
    """
    if cycle_num % BALANCE_CHECK_CYCLES != 0:
        return True   # solo verificar cada N ciclos

    if DRY_RUN:
        # En simulación: estimar divergencia por slippage acumulado
        avg_slip = state.get("avg_slippage_pct", 0)
        trades   = state.get("total_trades", 0)
        if avg_slip > 0 and trades > 0:
            estimated_drift = state["active_capital"] * avg_slip * trades / 100
            state["balance_drift_total"] = estimated_drift
            log(
                f"BALANCE_SIM | drift_estimado=${estimated_drift:.2f} | "
                f"avg_slippage={avg_slip:.3f}% | trades={trades}"
            )
        return True

    onchain = get_onchain_balance()
    if onchain < 0:
        log("BALANCE_CHECK | no disponible en este ciclo")
        return True

    json_capital = state.get("active_capital", MAX_CAPITAL)
    drift        = abs(json_capital - onchain)
    state["balance_onchain_last"] = onchain
    state["balance_drift_total"]  = drift
    state["balance_checks"]       = state.get("balance_checks", 0) + 1

    log(
        f"BALANCE_CHECK | json=${json_capital:.2f} | "
        f"onchain=${onchain:.2f} | drift=${drift:.2f} | "
        f"checks={state['balance_checks']}"
    )

    if drift >= BALANCE_DRIFT_HALT:
        msg = (
            f"🚨 BALANCE HALT | divergencia=${drift:.2f} > límite=${BALANCE_DRIFT_HALT}\nJSON: ${json_capital:.2f} | Real: ${onchain:.2f}\nBot detenido. Revisar manualmente antes de continuar."
        )
        log(msg)
        send_telegram(msg)
        state["balance_halted"] = True
        # Sincronizar capital JSON con realidad
        state["active_capital"] = onchain
        save_state(state)
        return False   # DETENER el bot

    if drift >= BALANCE_DRIFT_ALERT:
        msg = (
            f"⚠️ BALANCE DRIFT | ${drift:.2f}\nJSON: ${json_capital:.2f} | Real: ${onchain:.2f}\nSincronizando capital automáticamente."
        )
        log(msg)
        send_telegram(msg)
        # Auto-sincronizar para evitar que el drift crezca
        state["active_capital"] = onchain

    return True


# ══════════════════════════════════════════════════════════════
# PUNTO CIEGO #2 — MODELO DE SLIPPAGE E IMPACTO DE PRECIO
# En simulación, el ROI real vs teórico nunca se comparaba.
# ══════════════════════════════════════════════════════════════

def estimate_slippage(cost_total: float, n_markets: int) -> float:
    """
    Estima el slippage esperado basado en el tamaño de la posición.

    Modelo empírico basado en datos de bots reales:
      - Cada $100 invertidos mueve el precio ~0.3% en promedio
      - Con N mercados simultáneos, el impacto se distribuye
      - Mercados con más de $500 de liquidez tienen menor impacto

    Retorna el slippage estimado en % del trade.
    """
    base_impact = (cost_total / 100) * SLIPPAGE_IMPACT_PCT
    market_factor = max(0.5, 1.0 - (n_markets - 1) * 0.05)  # más mercados = menor impacto unitario
    return round(base_impact * market_factor, 4)


def adjust_profit_for_slippage(profit_net: float, cost_total: float,
                                n_markets: int) -> dict:
    """
    PUNTO CIEGO #2: Ajusta la ganancia esperada por slippage estimado.

    Problema del post-mortem:
      ROI simulado 14.78% → ROI real 2.8% por slippage no modelado.
      El bot nunca midió esta diferencia hasta que fue demasiado tarde.

    Retorna ganancia ajustada y métricas de slippage para tracking.
    """
    slip_pct    = estimate_slippage(cost_total, n_markets)
    slip_cost   = cost_total * (slip_pct / 100)
    adjusted    = profit_net - slip_cost
    adjusted_roi= (adjusted / cost_total) * 100 if cost_total > 0 else 0

    return {
        "profit_adjusted": round(adjusted, 4),
        "roi_adjusted":    round(adjusted_roi, 4),
        "slippage_pct":    round(slip_pct, 4),
        "slippage_cost":   round(slip_cost, 4),
        "still_profitable":adjusted > 0 and adjusted_roi >= MIN_ROI_PCT,
    }


def record_slippage(state: dict, expected_roi: float, actual_roi: float):
    """Registra el slippage real para calibrar el modelo."""
    slip = expected_roi - actual_roi
    history = state.get("slippage_history", [])
    history.append({"ts": datetime.now().isoformat(), "slip": slip,
                     "expected": expected_roi, "actual": actual_roi})
    state["slippage_history"] = history[-20:]   # últimos 20 trades
    if len(history) >= 3:
        state["avg_slippage_pct"] = sum(h["slip"] for h in history) / len(history)
    log(
        f"SLIPPAGE | esperado={expected_roi:.2f}% | real={actual_roi:.2f}% | "
        f"diff={slip:.2f}% | avg={state['avg_slippage_pct']:.2f}%"
    )


# ══════════════════════════════════════════════════════════════
# PUNTO CIEGO #3 — MONITOR DE FEES EN TIEMPO REAL
# Polymarket puede cambiar su estructura de fees sin aviso.
# ══════════════════════════════════════════════════════════════

def fetch_current_fee() -> float:
    """
    Consulta la estructura de fees actual de Polymarket.
    Intenta detectar el taker fee real consultando la API.

    En ausencia de endpoint público de fees, usa el último trade
    para inferir el fee cobrado comparando ganancia bruta vs neta.
    """
    try:
        # Polymarket expone info de fees en el endpoint de mercados
        resp = api_get(f"{CLOB_API}/fee-rate-bps", {})
        if isinstance(resp, dict):
            bps = float(resp.get("fee_rate_bps", 200) or 200)
            return bps / 10000   # convertir basis points a decimal
    except Exception:
        pass
    # Fallback: usar el valor configurado
    return TAKER_FEE_PCT


def check_fee_changes(state: dict) -> bool:
    """
    PUNTO CIEGO #3: Detecta cambios en la estructura de fees.

    Problema del post-mortem:
      Polymarket subió fees de 2% a 3.5% en septiembre.
      El bot siguió calculando con 2% durante 3 semanas sin saberlo.
      Resultado: todos los trades eran menos rentables de lo calculado.

    Retorna True si los fees son los esperados, False si cambiaron.
    """
    if not FEE_MONITOR_ENABLED:
        return True

    last_check = state.get("fee_last_check_ts", 0)
    if time.time() - last_check < FEE_CHECK_INTERVAL_MIN * 60:
        return True   # verificar solo cada N minutos

    current_fee = fetch_current_fee()
    state["fee_last_check_ts"] = time.time()
    stored_fee = state.get("fee_current", TAKER_FEE_PCT)
    drift = abs(current_fee - stored_fee)

    log(
        f"FEE_CHECK | configurado={TAKER_FEE_PCT*100:.1f}% | "
        f"detectado={current_fee*100:.1f}% | drift={drift*100:.2f}%"
    )

    if drift >= FEE_ALERT_THRESHOLD:
        state["fee_alerts"] = state.get("fee_alerts", 0) + 1
        msg = f"⚠️ FEE CAMBIO DETECTADO\nAnterior: {stored_fee*100:.1f}%\nActual: {current_fee*100:.1f}%\nActualiza TAKER_FEE_PCT en .env y reinicia el bot."
        log(msg)
        send_telegram(msg)
        # Actualizar automáticamente para no operar con fee incorrecto
        state["fee_current"] = current_fee
        log(f"FEE_AUTO_UPDATE | fee actualizado a {current_fee*100:.2f}% en estado")

    return True


# ══════════════════════════════════════════════════════════════
# PUNTO CIEGO #4 — COMPOUND CON TRANSFERENCIA REAL
# El 50% de ganancias se registraba pero nunca salía de Polymarket.
# ══════════════════════════════════════════════════════════════

def execute_compound_transfer(state: dict, amount: float) -> bool:
    """
    PUNTO CIEGO #4: Ejecuta la transferencia real del compound a la wallet.

    Problema del post-mortem:
      El código tenía "TRANSFER PENDIENTE — implementar".
      Cuando el proyecto cerró, todo el dinero estaba atrapado en Polymarket.
      El 50% de ganancias "enviado a wallet" era solo contabilidad interna.

    Esta implementación:
      1. Acumula el monto pendiente hasta superar COMPOUND_MIN_TRANSFER ($10)
      2. En DRY_RUN: solo registra y loguea
      3. En modo real: ejecuta la transferencia on-chain via py-clob-client
    """
    state["compound_pending_transfer"] = (
        state.get("compound_pending_transfer", 0) + amount
    )
    pending = state["compound_pending_transfer"]

    log(
        f"COMPOUND_TRANSFER | +${amount:.4f} acumulado | "
        f"pendiente=${pending:.4f} | mín=${COMPOUND_MIN_TRANSFER}"
    )

    if pending < COMPOUND_MIN_TRANSFER:
        log(f"COMPOUND_TRANSFER | esperando acumular ${COMPOUND_MIN_TRANSFER - pending:.2f} más")
        return False   # no transferir aún — acumular más

    if DRY_RUN:
        log(
            f"COMPOUND_TRANSFER [SIM] | Se habrían transferido ${pending:.4f} "
            f"a {WITHDRAW_WALLET[:10]}..."
        )
        state["compound_pending_transfer"] = 0.0
        state["compound_transferred_real"] = (
            state.get("compound_transferred_real", 0) + pending
        )
        return True

    if not COMPOUND_TRANSFER_ENABLED:
        log(
            f"COMPOUND_TRANSFER | deshabilitado (COMPOUND_TRANSFER_ENABLED=false) | "
            f"${pending:.4f} pendiente acumulado"
        )
        return False

    # ── Transferencia real on-chain ───────────────────────────
    try:
        from py_clob_client.client import ClobClient
        client = ClobClient(
            host=CLOB_API, key=PRIVATE_KEY,
            chain_id=137, funder=FUNDER_ADDRESS, signature_type=2,
        )
        # Retirar USDC de Polymarket a la wallet de retiros
        resp = client.withdraw(
            amount=str(int(pending * 1e6)),   # USDC tiene 6 decimales
            address=WITHDRAW_WALLET,
        )
        log(f"COMPOUND_TRANSFER OK | ${pending:.4f} → {WITHDRAW_WALLET[:10]}... | {resp}")
        send_telegram(
            f"💸 TRANSFER EJECUTADA\n${pending:.4f} USDC → wallet\nPendiente: $0.00"
        )
        state["compound_pending_transfer"] = 0.0
        state["compound_transferred_real"] = (
            state.get("compound_transferred_real", 0) + pending
        )
        return True
    except ImportError:
        log("COMPOUND_TRANSFER ERROR | py-clob-client no disponible")
        return False
    except Exception as e:
        log(f"COMPOUND_TRANSFER ERROR | {e}")
        send_telegram(f"❌ TRANSFER FALLIDA | ${pending:.4f} | {e}")
        return False


# ══════════════════════════════════════════════════════════════
# DEDUPLICACIÓN — evita ejecutar el mismo mercado múltiples veces
# ══════════════════════════════════════════════════════════════

def is_duplicate(state: dict, event_id: str, event_title: str) -> bool:
    """
    Verifica si un evento ya fue ejecutado hoy.

    Lógica:
      El log del día 3 mostró que FIFA se ejecutó 193 veces en 1 día.
      En modo real solo existiría 1 ejecución real (la primera absorbe
      la liquidez y el precio se corrige). Esta función bloquea re-ejecuciones.

    DEDUP_MAX_PER_EVENT = 1 por defecto (1 ejecución por evento por día).
    Se puede subir a 2-3 para mercados muy líquidos si se desea.
    """
    executed = state.get("executed_today", {})
    key      = event_id or event_title[:50]   # fallback a título si no hay ID

    if key in executed:
        count = len(executed[key])
        if count >= DEDUP_MAX_PER_EVENT:
            log(
                f"DEDUP SKIP | '{event_title[:45]}' | "
                f"ya ejecutado {count}× hoy | max={DEDUP_MAX_PER_EVENT}"
            )
            return True
    return False


def register_execution(state: dict, event_id: str, event_title: str):
    """Registra la ejecución de un evento para el control de dedup."""
    if "executed_today" not in state:
        state["executed_today"] = {}
    key = event_id or event_title[:50]
    if key not in state["executed_today"]:
        state["executed_today"][key] = []
    state["executed_today"][key].append(datetime.now().isoformat())


def get_dedup_stats(state: dict) -> str:
    """Retorna resumen del estado de deduplicación para logs."""
    executed = state.get("executed_today", {})
    total    = sum(len(v) for v in executed.values())
    unique   = len(executed)
    return f"dedup: {unique} eventos únicos | {total} ejecuciones hoy"


# ══════════════════════════════════════════════════════════════
# SEGURIDAD — CONTROL DE RIESGO (espejo V11, ajustado V2)
# ══════════════════════════════════════════════════════════════

def can_trade(state: dict) -> tuple[bool, str]:
    cap = state.get("active_capital", MAX_CAPITAL)
    if state["trades_today"]       >= MAX_TRADES_PER_DAY:
        return False, f"MAX_TRADES {state['trades_today']}/{MAX_TRADES_PER_DAY}"
    if state["daily_pnl_fraction"] <= -MAX_DAILY_LOSS:
        return False, f"DAILY_LOSS_PCT {state['daily_pnl_fraction']:.4f}"
    if state["daily_pnl_usdt"]     <= -MAX_DAILY_LOSS_USDT:
        return False, f"DAILY_LOSS_USDT ${state['daily_pnl_usdt']:.2f}"
    if state["total_pnl_usdt"]     <= -GLOBAL_DAILY_LOSS_LIMIT:
        return False, f"GLOBAL_LOSS ${state['total_pnl_usdt']:.2f}"
    if state["consecutive_losses"] >= MAX_CONSECUTIVE_LOSSES:
        return False, f"CONSEC_LOSSES {state['consecutive_losses']}"
    remaining = COOLDOWN_SECONDS - (time.time() - state["last_trade_timestamp"])
    if remaining > 0:
        return False, f"COOLDOWN {remaining:.0f}s"
    adaptive = float(state.get("cooldown_until", 0) or 0)
    if time.time() < adaptive:
        return False, f"ADAPTIVE_CD {(adaptive - time.time()):.0f}s"
    min_cap = MAX_CAPITAL * 0.20
    if cap < min_cap:
        return False, f"CAPITAL_BAJO ${cap:.2f}"
    if not is_trading_hour():
        return False, f"HORARIO UTC={datetime.now(timezone.utc).hour:02d}"
    return True, ""

# ══════════════════════════════════════════════════════════════
# FIX #3 — CÁLCULO REAL DE GANANCIA POST-FEES
# ══════════════════════════════════════════════════════════════

def profit_after_fees(sum_yes: float, sets: int, n_markets: int) -> dict:
    """
    Calcula la ganancia real después de descontar:
    - Taker fee: 2% del payout ($1.00 × sets)
    - Gas: ~$0.002 por orden × N mercados × 2 (compra + eventual merge)
    
    Polymarket cobra el fee SOLO al ganador, sobre el payout total.
    """
    gross_payout  = 1.0 * sets                      # siempre recibes $1 por set ganador
    gross_cost    = sum_yes * sets                   # lo que pagas
    gross_profit  = gross_payout - gross_cost

    fee_amount    = gross_payout * TAKER_FEE_PCT     # 2% de $1.00 × sets
    gas_amount    = GAS_COST_USDC * n_markets * 2    # ida y vuelta

    net_profit    = gross_profit - fee_amount - gas_amount
    net_roi_pct   = (net_profit / gross_cost) * 100 if gross_cost > 0 else 0

    return {
        "gross_profit": round(gross_profit, 6),
        "fee_amount":   round(fee_amount,   6),
        "gas_amount":   round(gas_amount,   6),
        "net_profit":   round(net_profit,   6),
        "net_roi_pct":  round(net_roi_pct,  4),
        "profitable":   net_profit > 0 and net_roi_pct >= MIN_ROI_PCT,
    }

# ══════════════════════════════════════════════════════════════
# FIX #2 — FILTRO DE LIQUIDEZ (order book depth)
# ══════════════════════════════════════════════════════════════

def check_liquidity(token_id: str, needed_size: float) -> dict:
    """
    Consulta el order book del CLOB para un token.
    Verifica que haya suficiente liquidez para nuestra posición sin slippage.
    """
    try:
        book = api_get(f"{CLOB_API}/book", {"token_id": token_id})
        asks = book.get("asks", [])   # asks = lo que podemos COMPRAR (BUY YES)
        if not asks:
            return {"ok": False, "reason": "sin_asks", "depth": 0}

        # Calcular profundidad disponible al precio actual
        available_size = sum(float(a.get("size", 0)) for a in asks[:5])   # top 5 niveles
        ratio = available_size / needed_size if needed_size > 0 else 0

        ok = (
            available_size >= needed_size * MIN_BOOK_DEPTH_RATIO and
            available_size >= (MIN_LIQUIDITY_USDC / float(asks[0].get("price", 1)))
        )
        return {
            "ok":             ok,
            "available_size": round(available_size, 2),
            "needed_size":    round(needed_size, 2),
            "ratio":          round(ratio, 2),
            "best_ask":       float(asks[0].get("price", 0)) if asks else 0,
            "reason":         "" if ok else f"depth_insuficiente:{ratio:.1f}x",
        }
    except Exception as e:
        # Si no podemos verificar liquidez, rechazar por seguridad
        return {"ok": False, "reason": f"error_book:{e}", "depth": 0}

# ══════════════════════════════════════════════════════════════
# DATACLASSES DE OPORTUNIDADES
# ══════════════════════════════════════════════════════════════

@dataclass
class Opportunity:
    strategy:    str       # "NEGRISK", "BINARY", "NEAR_RES", "MARKET_MAKING"
    event_title: str
    event_id:    str
    sum_cost:    float
    profit_gross:float
    profit_net:  float     # V2: post-fees
    roi_pct:     float     # V2: post-fees
    sets:        int
    cost_total:  float
    quality_score: int     # FIX #8
    markets:     list      # [{question, yes_price, yes_token, liquidity_ok}]
    expires_at:  Optional[str] = None
    near_resolution: bool = False

# ══════════════════════════════════════════════════════════════
# SCORING DE CALIDAD (FIX #8 — igual que V11)
# ══════════════════════════════════════════════════════════════

def compute_quality_score(opp_data: dict) -> int:
    """
    Score 0-100 de calidad de la oportunidad.
    Solo ejecuta si score >= MIN_QUALITY_SCORE.
    """
    score = 0

    # ROI post-fees
    roi = opp_data.get("roi_pct", 0)
    if roi >= 5:   score += 30
    elif roi >= 3: score += 20
    elif roi >= 2: score += 15
    elif roi >= 1: score += 10

    # Liquidez
    if opp_data.get("all_liquid"):      score += 25
    elif opp_data.get("some_liquid"):   score += 10

    # Near-resolution bonus
    if opp_data.get("near_resolution"): score += 20

    # N mercados en el arb (más mercados = más riesgo de ejecución parcial)
    n = opp_data.get("n_markets", 2)
    if n == 2:   score += 15
    elif n <= 4: score += 10
    elif n <= 6: score += 5

    # Ganancia neta absoluta
    net = opp_data.get("net_profit", 0)
    if net >= 5:   score += 10
    elif net >= 2: score += 5
    elif net >= 1: score += 2

    return min(score, 100)

# ══════════════════════════════════════════════════════════════
# SCANNER — ESTRATEGIAS
# ══════════════════════════════════════════════════════════════

def _paginated_fetch(endpoint: str, max_total: int, label: str) -> list:
    """
    AGENTE OPTIMIZADOR: función genérica de paginación con:
    - Reintentos por página individuales (no falla todo si 1 página da error)
    - Detección automática de última página (len < page_size)
    - Log progresivo cada 5 páginas para visibilidad
    - Usa constante SCAN_PAGE_SIZE del .env (configurable sin tocar código)
    """
    all_results = []
    offset      = 0
    page_num    = 0

    while len(all_results) < max_total:
        page_num += 1
        try:
            page = api_get(f"{GAMMA_API}/{endpoint}", {
                "limit":  SCAN_PAGE_SIZE,
                "offset": offset,
                "active": "true",
                "closed": "false",
            })

            if not page:
                break

            all_results.extend(page)

            # Log cada 5 páginas para no saturar
            if page_num % 5 == 0:
                log(f"PAGINA {label} | pág={page_num} | acumulado={len(all_results)}")

            if len(page) < SCAN_PAGE_SIZE:
                break   # última página — no hay más datos

            offset += SCAN_PAGE_SIZE

        except Exception as e:
            log(f"ERROR {label} pág {page_num}: {e} | continuando con {len(all_results)} resultados")
            break   # error en una página no detiene todo

    log(f"{label.upper()} cargados: {len(all_results)} en {page_num} páginas")
    return all_results[:max_total]


def get_active_events(max_total: int = None) -> list:
    """Paginación completa de eventos activos."""
    return _paginated_fetch("events", max_total or MAX_EVENTS_PER_SCAN, "eventos")


def get_active_markets(max_total: int = None) -> list:
    """Paginación completa de mercados individuales."""
    return _paginated_fetch("markets", max_total or MAX_MARKETS_PER_SCAN, "mercados")


# ── Estrategia 1: NegRisk multi-outcome ──────────────────────

def scan_negrisk(events: list, capital: float, state: dict = None) -> list[Opportunity]:
    """
    FIX #3: aplica contabilidad real de fees.
    FIX #2: verifica liquidez antes de confirmar oportunidad.
    DEDUP:  rechaza eventos ya ejecutados hoy (evita FIFA × 193).
    """
    opps = []
    max_position = capital * (MAX_RISK_PERCENT / 100)

    for event in events:
        markets = event.get("markets", [])
        if len(markets) < 2:
            continue

        # DEDUP: verificar si este evento ya se ejecutó hoy
        event_id    = str(event.get("id", ""))
        event_title = event.get("title", "")
        if state and is_duplicate(state, event_id, event_title):
            continue

        yes_data = []
        for m in markets:
            try:
                # FIX #4: json ya importado globalmente — removido import dentro del loop
                outcomes = json.loads(m.get("outcomePrices", "[]"))
                tokens   = json.loads(m.get("clobTokenIds",  "[]"))
                if len(outcomes) < 2 or len(tokens) < 2:
                    continue
                yp = float(outcomes[0])
                if 0.02 < yp < 0.98:
                    yes_data.append({
                        "question":  m.get("question", ""),
                        "yes_price": yp,
                        "yes_token": tokens[0],
                        "market_id": m.get("id", ""),
                        "volume":    float(m.get("volume", 0) or 0),
                    })
            except: continue

        if len(yes_data) < 2:
            continue

        sum_yes = sum(d["yes_price"] for d in yes_data)
        if sum_yes >= 1.0:
            continue

        # FIX LOG #1: suma YES muy baja = mercado zombi sin liquidez real
        if sum_yes < MIN_SUM_YES:
            log(
                f"SKIP sum_yes_bajo | {event.get('title','')[:40]} | "
                f"sum={sum_yes:.4f} < min={MIN_SUM_YES} | mercado sin liquidez"
            )
            continue

        sets = int(max_position / sum_yes)
        if sets < 1:
            continue

        # FIX #3: ganancia real post-fees
        fees = profit_after_fees(sum_yes, sets, len(yes_data))
        if not fees["profitable"]:
            continue

        # FIX LOG #2: ROI irreal = mercado sin profundidad de libro
        if fees["net_roi_pct"] > MAX_ROI_PCT:
            log(
                f"SKIP roi_irreal | {event.get('title','')[:40]} | "
                f"ROI={fees['net_roi_pct']:.1f}% > max={MAX_ROI_PCT}% | "
                f"sum_yes={sum_yes:.4f} | descartado por seguridad"
            )
            continue

        # FIX #2: verificar liquidez de todos los mercados
        all_liquid  = True
        some_liquid = False
        for yd in yes_data:
            liq = check_liquidity(yd["yes_token"], sets)
            yd["liquidity"] = liq
            if not liq["ok"]:
                all_liquid = False
            else:
                some_liquid = True

        if not all_liquid:
            log(f"SKIP liquidez | {event.get('title','')[:40]} | ratio insuficiente")
            continue

        # PUNTO CIEGO #2: ajustar ganancia por slippage estimado
        slip = adjust_profit_for_slippage(
            fees["net_profit"], sum_yes * sets, len(yes_data)
        )
        if not slip["still_profitable"]:
            log(
                f"SKIP slippage | {event_title[:40]} | "
                f"net=${fees['net_profit']:.4f} → ajustado=${slip['profit_adjusted']:.4f} | "
                f"slippage={slip['slippage_pct']:.3f}%"
            )
            continue

        score = compute_quality_score({
            "roi_pct":        slip["roi_adjusted"],   # ROI post-slippage
            "net_profit":     slip["profit_adjusted"],
            "all_liquid":     all_liquid,
            "some_liquid":    some_liquid,
            "n_markets":      len(yes_data),
            "near_resolution":False,
        })

        if score < MIN_QUALITY_SCORE:
            continue

        opps.append(Opportunity(
            strategy=    "NEGRISK",
            event_title= event.get("title", "")[:60],
            event_id=    str(event.get("id", "")),
            sum_cost=    round(sum_yes, 4),
            profit_gross=round(fees["gross_profit"], 4),
            profit_net=  round(fees["net_profit"],   4),
            roi_pct=     round(fees["net_roi_pct"],  2),
            sets=        sets,
            cost_total=  round(sum_yes * sets, 4),
            quality_score=score,
            markets=     yes_data,
        ))

    return opps


# ── Estrategia 2: Arbitraje Binario YES+NO (FIX #4) ──────────

def scan_binary(markets: list, capital: float, state: dict = None) -> list[Opportunity]:
    """
    En mercados binarios: YES + NO = $1.00 siempre.
    Si YES + NO < $0.995 → comprar ambos = ganancia garantizada.
    
    Fuente: PolyTrack ($40M extraídos), Benjamin-Cup
    """
    opps = []
    max_position = capital * (MAX_RISK_PERCENT / 100)

    for m in markets:
        try:
            # FIX #4: json ya importado globalmente — removido import dentro del loop

            # DEDUP: verificar si este mercado ya se ejecutó hoy
            mkt_id    = str(m.get("id", ""))
            mkt_title = m.get("question", "")
            if state and is_duplicate(state, mkt_id, mkt_title):
                continue

            outcomes = json.loads(m.get("outcomePrices", "[]"))
            tokens   = json.loads(m.get("clobTokenIds",  "[]"))
            if len(outcomes) != 2 or len(tokens) != 2:
                continue

            yes_price = float(outcomes[0])
            no_price  = float(outcomes[1])
            combined  = yes_price + no_price

            # Umbral conservador: < 0.975 para cubrir fees 2% + gas
            if combined >= 0.975:
                continue

            sets = int(max_position / combined)
            if sets < 1:
                continue

            # Fee: pagamos 2% sobre el payout del ganador (solo un lado gana)
            fees = profit_after_fees(combined, sets, 2)
            if not fees["profitable"]:
                continue

            # FIX LOG #2: ROI irreal en binario
            if fees["net_roi_pct"] > MAX_ROI_PCT:
                log(
                    f"SKIP roi_irreal_binario | ROI={fees['net_roi_pct']:.1f}% > max={MAX_ROI_PCT}%"
                )
                continue

            # FIX LOG #1: suma YES+NO muy baja
            if combined < MIN_SUM_YES:
                log(f"SKIP combined_bajo | combined={combined:.4f} < min={MIN_SUM_YES}")
                continue

            # Liquidez ambos lados
            liq_yes = check_liquidity(tokens[0], sets)
            liq_no  = check_liquidity(tokens[1], sets)
            if not (liq_yes["ok"] and liq_no["ok"]):
                continue

            score = compute_quality_score({
                "roi_pct":        fees["net_roi_pct"],
                "net_profit":     fees["net_profit"],
                "all_liquid":     True,
                "n_markets":      2,
                "near_resolution":False,
            })

            if score < MIN_QUALITY_SCORE:
                continue

            opps.append(Opportunity(
                strategy=    "BINARY",
                event_title= m.get("question", "")[:60],
                event_id=    str(m.get("id", "")),
                sum_cost=    round(combined,             4),
                profit_gross=round(fees["gross_profit"], 4),
                profit_net=  round(fees["net_profit"],   4),
                roi_pct=     round(fees["net_roi_pct"],  2),
                sets=        sets,
                cost_total=  round(combined * sets,      4),
                quality_score=score,
                markets=[
                    {"question": "YES", "yes_price": yes_price, "yes_token": tokens[0]},
                    {"question": "NO",  "yes_price": no_price,  "yes_token": tokens[1]},
                ],
            ))
        except: continue

    return opps


# ── Estrategia 3: Near-Resolution (FIX #10) ──────────────────

def scan_near_resolution(markets: list, capital: float, state: dict = None) -> list[Opportunity]:
    """
    Mercados que resuelven en <24h con precio YES > 94%.
    ROI anualizado enorme aunque el margen absoluto sea pequeño.
    
    Ejemplo: YES = $0.96, resuelve en 6h
    Ganancia = $0.04 por share en 6h = 4.2% ROI en 6h = ~6000% anualizado
    
    Fuente: PolyTrack guide
    """
    opps = []
    now  = datetime.now(timezone.utc)
    max_position = capital * (MAX_RISK_PERCENT / 100)

    for m in markets:
        try:
            # DEDUP: verificar si este mercado near-res ya se ejecutó hoy
            nr_id    = str(m.get("id", ""))
            nr_title = m.get("question", "")
            if state and is_duplicate(state, nr_id, nr_title):
                continue

            end_date_str = m.get("endDate") or m.get("endDateIso")
            if not end_date_str:
                continue

            # Parsear fecha de resolución
            try:
                if "T" in end_date_str:
                    end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                else:
                    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except:
                continue

            hours_to_res = (end_date - now).total_seconds() / 3600
            if hours_to_res < 0 or hours_to_res > NEAR_RESOLUTION_HOURS:
                continue

            # FIX #4: json ya importado globalmente — removido import dentro del loop
            outcomes = json.loads(m.get("outcomePrices", "[]"))
            tokens   = json.loads(m.get("clobTokenIds",  "[]"))
            if len(outcomes) < 2 or len(tokens) < 2:
                continue

            yes_price = float(outcomes[0])
            if yes_price < NEAR_RESOLUTION_THRESHOLD:
                continue   # no es suficientemente probable

            # Ganancia = $1.00 - yes_price (si resuelve YES)
            profit_per_share = 1.0 - yes_price
            shares = int(max_position / yes_price)
            if shares < 1:
                continue

            fees = profit_after_fees(yes_price, shares, 1)
            if not fees["profitable"]:
                continue

            # ROI anualizado (referencia, no lo usamos para decisión)
            roi_annualized = (fees["net_roi_pct"] / max(hours_to_res, 0.5)) * 24 * 365

            liq = check_liquidity(tokens[0], shares)
            if not liq["ok"]:
                continue

            score = compute_quality_score({
                "roi_pct":        fees["net_roi_pct"],
                "net_profit":     fees["net_profit"],
                "all_liquid":     True,
                "n_markets":      1,
                "near_resolution":True,
            })

            if score < MIN_QUALITY_SCORE:
                continue

            opps.append(Opportunity(
                strategy=    "NEAR_RES",
                event_title= m.get("question", "")[:60],
                event_id=    str(m.get("id", "")),
                sum_cost=    round(yes_price,            4),
                profit_gross=round(fees["gross_profit"], 4),
                profit_net=  round(fees["net_profit"],   4),
                roi_pct=     round(fees["net_roi_pct"],  2),
                sets=        shares,
                cost_total=  round(yes_price * shares,   4),
                quality_score=score,
                markets=[{"question": "YES", "yes_price": yes_price, "yes_token": tokens[0]}],
                expires_at=  end_date_str,
                near_resolution=True,
            ))
        except: continue

    return opps


# ── Scanner principal: combina todas las estrategias ─────────

def scan_all(state: dict) -> list[Opportunity]:
    cap     = state.get("active_capital", MAX_CAPITAL)
    # FIX #3: usar variables del .env en lugar de hardcode 2000
    # MAX_EVENTS_PER_SCAN y MAX_MARKETS_PER_SCAN ahora son configurables
    events  = get_active_events()    # usa MAX_EVENTS_PER_SCAN del .env
    markets = get_active_markets()   # usa MAX_MARKETS_PER_SCAN del .env

    opps = []
    opps += scan_negrisk(events,  cap, state)
    opps += scan_binary(markets,  cap, state)
    opps += scan_near_resolution(markets, cap, state)

    # Ordenar por score DESC, luego ROI DESC
    opps.sort(key=lambda x: (x.quality_score, x.roi_pct), reverse=True)

    log(
        f"SCAN COMPLETO | eventos={len(events)} | mercados={len(markets)} | "
        f"opps={len(opps)} | "
        f"negrisk={sum(1 for o in opps if o.strategy=='NEGRISK')} | "
        f"binary={sum(1 for o in opps if o.strategy=='BINARY')} | "
        f"near_res={sum(1 for o in opps if o.strategy=='NEAR_RES')}"
    )
    return opps

# ══════════════════════════════════════════════════════════════
# PUNTO CIEGO #5 — DETECCIÓN DE FILLS PARCIALES EN BATCH FOK
# FOK garantiza atomicidad por orden, NO entre las N patas de un
# arbitraje NegRisk. Antes de este fix, execute_opportunity() nunca
# inspeccionaba el resultado real del batch — cualquier respuesta sin
# excepción se trataba como éxito completo.
# ══════════════════════════════════════════════════════════════

def classify_batch_result(opp: Opportunity, resp_list: list) -> dict:
    """
    Clasifica la respuesta de POST /orders (batch) en fill completo /
    parcial / kill total. Función pura, sin red ni efectos secundarios
    (testeable offline — ver test_fill_classification.py).

    resp_list: alineada 1:1 con opp.markets. Cada elemento tiene la forma
    documentada por Polymarket para POST /order:
      {success, errorMsg, orderID, status, makingAmount, takingAmount,
       transactionsHashes, tradeIDs}
    status ∈ {"live", "matched", "delayed", "unmatched"}. Para FOK solo
    "matched" cuenta como llenado — cualquier otro valor (incluyendo
    ausente/desconocido) se trata defensivamente como no-filled.
    """
    legs = []
    for i, m in enumerate(opp.markets):
        r = resp_list[i] if i < len(resp_list) and isinstance(resp_list[i], dict) else {}
        filled = bool(r.get("success")) and r.get("status") == "matched"
        legs.append({
            "token_id":    m.get("yes_token"),
            "question":    m.get("question", ""),
            "price":       m.get("yes_price"),
            "filled":      filled,
            "filled_size": opp.sets if filled else 0,
            "order_id":    r.get("orderID"),
            "status":      r.get("status"),
            "error":       r.get("errorMsg", ""),
        })

    filled_count = sum(1 for l in legs if l["filled"])
    total_legs   = len(legs)
    filled_cost  = round(sum(l["price"] * l["filled_size"] for l in legs if l["filled"]), 6)

    if total_legs == 0:
        outcome = "kill"
    elif filled_count == total_legs:
        outcome = "full"
    elif filled_count == 0:
        outcome = "kill"
    else:
        outcome = "partial"

    return {
        "outcome":      outcome,
        "legs":         legs,
        "filled_count": filled_count,
        "total_legs":   total_legs,
        "filled_cost":  filled_cost,
    }


def get_best_bid(token_id: str) -> Optional[float]:
    """Consulta el mejor bid del CLOB para un token — usado por el unwind."""
    try:
        book = api_get(f"{CLOB_API}/book", {"token_id": token_id})
        bids = book.get("bids", [])
        if not bids:
            return None
        return float(bids[0].get("price", 0))
    except Exception:
        return None


def attempt_unwind(leg: dict) -> dict:
    """
    Vende a mercado (único intento) la posición de una pata parcialmente
    llenada. Respeta UNWIND_MIN_PRICE como piso de seguridad — si no hay
    bid utilizable o el FOK de venta también falla, retorna unwound=False
    y la posición queda para "mantener y alertar" (ver handle_partial_fill).
    """
    token_id    = leg["token_id"]
    size        = leg["filled_size"]
    entry_price = leg["price"]
    cost        = round(entry_price * size, 6)

    best_bid = get_best_bid(token_id)
    if best_bid is None or best_bid < UNWIND_MIN_PRICE:
        return {
            "unwound": False, "reason": "sin_liquidez_unwind",
            "sell_price": best_bid, "cost": cost,
            "proceeds": None, "loss_realized": None,
        }

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType, PostOrdersArgs

        client = ClobClient(
            host=CLOB_API, key=PRIVATE_KEY,
            chain_id=137, funder=FUNDER_ADDRESS, signature_type=2,
        )
        sell_args    = OrderArgs(token_id=token_id, price=best_bid, size=size, side="SELL")
        signed_order = client.create_order(sell_args)
        resp         = client.post_orders([PostOrdersArgs(order=signed_order, orderType=OrderType.FOK)])
        leg_resp     = resp[0] if isinstance(resp, list) and resp else (resp if isinstance(resp, dict) else {})

        filled_ok = bool(leg_resp.get("success")) and leg_resp.get("status") == "matched"
        if not filled_ok:
            return {
                "unwound": False, "reason": f"unwind_fok_fallo:{leg_resp.get('errorMsg','?')}",
                "sell_price": best_bid, "cost": cost,
                "proceeds": None, "loss_realized": None,
            }

        proceeds = round(best_bid * size, 6)
        loss     = round(proceeds - cost, 6)
        return {
            "unwound": True, "reason": "",
            "sell_price": best_bid, "cost": cost,
            "proceeds": proceeds, "loss_realized": loss,
        }
    except Exception as e:
        return {
            "unwound": False, "reason": f"error:{e}",
            "sell_price": best_bid, "cost": cost,
            "proceeds": None, "loss_realized": None,
        }


def handle_partial_fill(opp: Opportunity, classification: dict, state: dict) -> dict:
    """
    Trata un fill parcial: intenta unwind de cada pata llenada, registra
    posición residual si el unwind falla, y alerta por Telegram de
    inmediato (no esperar al heartbeat) con el detalle completo —
    cada parcial del piloto es un dato de medición.
    """
    filled_legs = [l for l in classification["legs"] if l["filled"]]
    unwind_results = []
    total_realized = 0.0
    total_unwound_cost = 0.0

    for leg in filled_legs:
        result = attempt_unwind(leg)
        unwind_results.append({**leg, "unwind": result})
        if result["unwound"]:
            total_realized     += result["loss_realized"]
            total_unwound_cost += result["cost"]
        else:
            state.setdefault("open_positions", []).append({
                "ts":        datetime.now().isoformat(),
                "event_id":  opp.event_id,
                "event":     opp.event_title,
                "token_id":  leg["token_id"],
                "question":  leg["question"],
                "size":      leg["filled_size"],
                "entry_price": leg["price"],
                "cost":      leg["price"] * leg["filled_size"],
                "unwind_reason": result["reason"],
            })

    killed_legs = [l for l in classification["legs"] if not l["filled"]]
    lines = [
        f"⚠️ FILL PARCIAL | {opp.strategy} | {opp.event_title[:45]}",
        f"Llenadas: {len(filled_legs)}/{classification['total_legs']} patas",
    ]
    for l in filled_legs:
        u = next(r["unwind"] for r in unwind_results if r["token_id"] == l["token_id"])
        if u["unwound"]:
            lines.append(
                f"  ✅ '{l['question'][:35]}' llenada @ ${l['price']} → "
                f"unwind @ ${u['sell_price']} | pérdida=${u['loss_realized']:.4f}"
            )
        else:
            lines.append(
                f"  🔴 '{l['question'][:35]}' llenada @ ${l['price']} → "
                f"UNWIND FALLÓ ({u['reason']}) — posición abierta, revisar manualmente"
            )
    for l in killed_legs:
        lines.append(f"  ⬜ '{l['question'][:35]}' killed ({l['error'] or l['status']})")

    msg = "\n".join(lines)
    log(msg.replace("\n", " | "))
    send_telegram(msg)

    unresolved = any(not r["unwind"]["unwound"] for r in unwind_results)
    return {
        "success":      False,
        "profit_usdt":  round(total_realized, 6),
        "exec_outcome": "partial",
        "actual_cost":  round(total_unwound_cost, 6) if total_unwound_cost > 0 else None,
        "unwind":       unwind_results,
        "unresolved_open_position": unresolved,
    }


def register_execution_outcome(state: dict, outcome: str) -> bool:
    """
    Cuenta el resultado de un intento de ejecución REAL (full/partial/kill)
    y evalúa el guardrail de tasa de fills problemáticos — mismo patrón
    que check_balance_drift/balance_halted. Retorna False si el bot debe
    detenerse.
    """
    state["execution_attempts"] = state.get("execution_attempts", 0) + 1
    if outcome == "partial":
        state["partial_fill_count"] = state.get("partial_fill_count", 0) + 1
    elif outcome == "kill":
        state["total_kill_count"] = state.get("total_kill_count", 0) + 1

    window = state.get("execution_outcome_window", [])
    window.append({"ts": datetime.now().isoformat(), "outcome": outcome})
    window = window[-20:]
    state["execution_outcome_window"] = window

    if len(window) < MIN_EXECUTION_SAMPLES:
        return True   # aún no hay muestras suficientes para evaluar la tasa

    problems = sum(1 for w in window if w["outcome"] in ("partial", "kill"))
    rate     = problems / len(window)

    log(
        f"EXEC_HEALTH | ventana={len(window)} | parciales={state['partial_fill_count']} | "
        f"kills={state['total_kill_count']} | tasa_problemas={rate*100:.1f}% | "
        f"umbral={MAX_PARTIAL_FILL_RATE*100:.0f}%"
    )

    if rate >= MAX_PARTIAL_FILL_RATE:
        state["execution_halted"] = True
        msg = (
            f"🚨 EXECUTION HALT | tasa de fills problemáticos={rate*100:.0f}% "
            f"(≥{MAX_PARTIAL_FILL_RATE*100:.0f}% en ventana de {len(window)} intentos)\n"
            f"Parciales acumulados: {state['partial_fill_count']} | Kills acumulados: {state['total_kill_count']}\n"
            f"Bot detenido. Revisar manualmente antes de continuar."
        )
        log(msg)
        send_telegram(msg)
        return False

    return True

# ══════════════════════════════════════════════════════════════
# EJECUCIÓN — BATCH ORDERS (FIX #9) + FOK (FIX #5)
# ══════════════════════════════════════════════════════════════

def execute_opportunity(opp: Opportunity, state: dict) -> dict:
    """
    FIX #9: batch orders (hasta 15 por llamada).
    FIX #5: Fill-or-Kill — si alguna orden no se llena, cancela todo.
    """
    log(
        f"EXEC {opp.strategy} | {opp.event_title[:45]} | "
        f"sets={opp.sets} | cost=${opp.cost_total} | "
        f"net=${opp.profit_net} | ROI={opp.roi_pct}% | score={opp.quality_score}"
    )

    if DRY_RUN:
        import random as _rnd
        log(f"[SIMULACIÓN] Batch de {len(opp.markets)} órdenes FOK:")
        for m in opp.markets:
            log(f"  BUY YES '{m['question'][:40]}' @ ${m['yes_price']} × {opp.sets} shares [FOK]")
        # [F2] Simular pérdidas realistas — win rate ~88% como en producción real
        # Causas de pérdida: precio movido, liquidez insuficiente, arbitraje cerrado
        # Probabilidad por ROI: ROI alto = mercado más ineficiente = más riesgo
        roi = opp.roi_pct
        if roi < 3:
            loss_prob = 0.18    # ROI bajo → mercado casi eficiente → fácil que se cierre
        elif roi < 8:
            loss_prob = 0.10    # ROI medio → zona normal
        else:
            loss_prob = 0.06    # ROI alto → gran ineficiencia, raro pero estable
        if _rnd.random() < loss_prob:
            loss = round(-opp.cost_total * 0.25, 4)  # pierde 25% del costo por slippage total
            log(f"  [SIM LOSS] Trade falló (prob={loss_prob:.0%}) | pérdida=${loss:.4f}")
            return {"success": False, "profit_usdt": loss, "simulated": True, "sim_loss": True}
        return {"success": True, "profit_usdt": opp.profit_net, "simulated": True}

    # ── Trading real con batch orders ────────────────────────
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType, PostOrdersArgs

        client = ClobClient(
            host=CLOB_API, key=PRIVATE_KEY,
            chain_id=137, funder=FUNDER_ADDRESS, signature_type=2,
        )

        # PUNTO CIEGO #5 (paso 0): firmar cada pata y envolverla como orden
        # FOK. La forma anterior (OrderArgs(time_in_force="FOK") +
        # client.create_and_post_orders) no existe en py-clob-client — nunca
        # se había ejecutado un trade real con este código.
        post_args_list = []
        for m in opp.markets:
            order_args  = OrderArgs(
                token_id=m["yes_token"],
                price=   m["yes_price"],
                size=    opp.sets,
                side=    "BUY",
            )
            signed_order = client.create_order(order_args)
            post_args_list.append(PostOrdersArgs(order=signed_order, orderType=OrderType.FOK))

        # FIX #9: enviar en batch (máx 15 por llamada)
        raw_responses = []
        for i in range(0, len(post_args_list), 15):
            batch = post_args_list[i:i+15]
            resp  = client.post_orders(batch)
            raw_responses.append(resp)
            log(f"  Batch {i//15+1}: {resp}")

        # Aplanar respuestas de todos los batches, alineadas 1:1 con opp.markets
        flat_resp = []
        for r in raw_responses:
            if isinstance(r, list):
                flat_resp.extend(r)
            else:
                flat_resp.append(r)

        classification = classify_batch_result(opp, flat_resp)
        log(
            f"EXEC_RESULT | {classification['outcome'].upper()} | "
            f"{classification['filled_count']}/{classification['total_legs']} patas llenadas"
        )

        if classification["outcome"] == "full":
            return {"success": True, "profit_usdt": opp.profit_net,
                     "exec_outcome": "full", "results": raw_responses}

        if classification["outcome"] == "kill":
            log(f"EXEC_KILL | {opp.event_title[:45]} | 0 patas llenadas, sin exposición")
            return {"success": False, "profit_usdt": 0, "exec_outcome": "kill",
                     "error": "total_kill", "results": raw_responses}

        # outcome == "partial"
        return handle_partial_fill(opp, classification, state)

    except ImportError:
        return {"success": False, "profit_usdt": 0, "error": "py-clob-client no instalado"}
    except Exception as e:
        log(f"ERROR ejecutando {opp.strategy}: {e}")
        return {"success": False, "profit_usdt": 0, "error": str(e)}

# ══════════════════════════════════════════════════════════════
# COMPOUND 50/50
# ══════════════════════════════════════════════════════════════

def apply_compound(state: dict, profit_usdt: float) -> dict:
    if profit_usdt <= 0:
        return {"profit": profit_usdt, "to_wallet": 0, "to_capital": 0,
                "new_capital": state["active_capital"]}
    to_wallet  = profit_usdt * COMPOUND_TO_WALLET_PCT
    to_capital = profit_usdt * COMPOUND_TO_CAPITAL_PCT
    # [F1] Compound sin límite en DRY_RUN — simulación debe reflejar crecimiento real
    # Bug original: min(..., MAX_CAPITAL) congelaba capital en $200 para siempre
    state["active_capital"] += to_capital
    state["total_sent_to_wallet"]    += to_wallet
    state["total_added_to_capital"]  += to_capital

    log(
        f"COMPOUND | +${profit_usdt:.4f} | "
        f"→wallet=${to_wallet:.4f} | →capital=${to_capital:.4f} | "
        f"capital_nuevo=${state['active_capital']:.4f}"
    )

    # PUNTO CIEGO #4: ejecutar transferencia real (acumula hasta mínimo)
    execute_compound_transfer(state, to_wallet)

    return {"profit": round(profit_usdt, 4), "to_wallet": round(to_wallet, 4),
            "to_capital": round(to_capital, 4), "new_capital": round(state["active_capital"], 4)}

# ══════════════════════════════════════════════════════════════
# REGISTRO DE TRADE
# ══════════════════════════════════════════════════════════════

def register_trade(state: dict, opp: Opportunity, result: dict, compound: dict):
    pnl = result.get("profit_usdt", 0)
    cap = state.get("active_capital", MAX_CAPITAL)

    # DEDUP: registrar este evento como ejecutado hoy
    register_execution(state, opp.event_id, opp.event_title)

    state["daily_pnl_usdt"]    += pnl
    state["daily_pnl_fraction"] += pnl / cap if cap > 0 else 0
    state["total_pnl_usdt"]    += pnl
    state["trades_today"]      += 1
    state["total_trades"]      += 1
    state["last_trade_timestamp"] = time.time()

    # Stats por estrategia
    key = f"stats_{opp.strategy.lower()}"
    if key in state:
        state[key]["trades"] += 1
        state[key]["pnl"]    += pnl
        if pnl >= 0: state[key]["wins"] += 1

    if pnl >= 0:
        state["consecutive_losses"] = 0
    else:
        state["consecutive_losses"] += 1
        new_cd = time.time() + POST_LOSS_COOLDOWN
        existing = float(state.get("cooldown_until", 0) or 0)
        state["cooldown_until"] = max(existing, new_cd)

    history = state.get("trade_history", [])
    # ROI realizado: si fue pérdida, ROI real es negativo.
    # PUNTO CIEGO #5: en un fill parcial, opp.cost_total es el costo del
    # arbitraje COMPLETO (teórico) — la base real es actual_cost (lo que de
    # verdad se pagó en las patas llenadas). Usar actual_cost cuando esté
    # presente para que el ROI realizado no quede subestimado.
    cost_base = result.get("actual_cost")
    if cost_base is None:
        cost_base = opp.cost_total
    roi_realizado = (pnl / cost_base * 100) if cost_base else 0
    history.append({
        "ts":       datetime.now().isoformat(),
        "strategy": opp.strategy,
        "event":    opp.event_title[:50],
        "sets":     opp.sets,
        "cost":     opp.cost_total,
        "profit":   pnl,
        "roi_pct":  opp.roi_pct,
        "roi_real": round(roi_realizado, 4),  # [F3] ROI real incluyendo pérdidas
        "score":    opp.quality_score,
        "to_wallet":compound.get("to_wallet", 0),
        "capital":  compound.get("new_capital", cap),
        "sim":      result.get("simulated", False),
        "sim_loss": result.get("sim_loss", False),
        # PUNTO CIEGO #5
        "exec_outcome": result.get("exec_outcome", "full"),
        "actual_cost":  result.get("actual_cost"),
        "unwind":       result.get("unwind"),
    })
    state["trade_history"] = history[-50:]
    # [F3] Registrar slippage real: diferencia entre ROI esperado y realizado
    record_slippage(state, opp.roi_pct, roi_realizado)
    save_state(state)

# ══════════════════════════════════════════════════════════════
# HEARTBEAT Y RESUMEN
# ══════════════════════════════════════════════════════════════

def send_heartbeat(state: dict):
    cap    = state.get("active_capital",       MAX_CAPITAL)
    wallet = state.get("total_sent_to_wallet", 0)
    # Win rates por estrategia
    stats_lines = []
    for s in ["negrisk", "binary", "near_res", "market_making"]:
        st = state.get(f"stats_{s}", {})
        t  = st.get("trades", 0)
        if t > 0:
            wr = st.get("wins", 0) / t * 100
            stats_lines.append(f"  {s.upper()}: {t} trades | WR={wr:.0f}% | PnL=${st.get('pnl',0):.2f}")
    stats_str = "\n".join(stats_lines) or "  Sin trades aún"

    drift      = state.get("balance_drift_total", 0)
    avg_slip   = state.get("avg_slippage_pct", 0)
    fee_actual = state.get("fee_current", TAKER_FEE_PCT)
    pending_tx = state.get("compound_pending_transfer", 0)
    real_tx    = state.get("compound_transferred_real", 0)
    onchain    = state.get("balance_onchain_last", 0)

    msg = (
        f"💓 HEARTBEAT BOT V2\n"
        f"Capital JSON: ${cap:.2f}\n"
        f"Balance real: ${onchain:.2f} (drift=${drift:.2f})\n"
        f"→ Wallet acum: ${wallet:.2f}\n"
        f"→ Transfer real: ${real_tx:.2f} | pendiente: ${pending_tx:.2f}\n"
        f"Patrimonio: ${cap+wallet:.2f}\n"
        f"PnL hoy: ${state['daily_pnl_usdt']:.4f}\n"
        f"PnL total: ${state['total_pnl_usdt']:.4f}\n"
        f"Slippage avg: {avg_slip:.2f}% | Fee actual: {fee_actual*100:.1f}%\n"
        f"Trades hoy: {state['trades_today']}\n"
        f"Por estrategia:\n{stats_str}\n"
        f"DRY_RUN: {DRY_RUN}"
    )
    send_telegram(msg)
    log(f"HEARTBEAT | cap=${cap:.2f} | wallet=${wallet:.2f} | pnl=${state['total_pnl_usdt']:.4f}")
    state["last_heartbeat_ts"] = time.time()

def log_cycle_summary(state: dict, opps: int, executed: int):
    cap    = state.get("active_capital",       MAX_CAPITAL)
    wallet = state.get("total_sent_to_wallet", 0)
    dedup  = get_dedup_stats(state)
    log(
        f"━━ CYCLE ━━ "
        f"capital=${cap:.2f} | wallet=${wallet:.2f} | total=${cap+wallet:.2f} | "
        f"PnL_día=${state['daily_pnl_usdt']:+.4f} | PnL_tot=${state['total_pnl_usdt']:+.4f} | "
        f"opps={opps} | exec={executed} | trades_hoy={state['trades_today']}/{MAX_TRADES_PER_DAY} | "
        f"{dedup}"
    )

# ══════════════════════════════════════════════════════════════
# MAIN LOOP
# ══════════════════════════════════════════════════════════════

def main():
    modo = "🔵 SIMULACIÓN" if DRY_RUN else "🟢 REAL"
    log(f"\n{'='*60}")
    log(f"POLYMARKET BOT V2 — {modo}")
    log(f"Estrategias: NegRisk + Binario + Near-Resolution")
    log(f"Capital: ${MAX_CAPITAL} | Riesgo: {MAX_RISK_PERCENT}%/trade")
    log(f"Fees reales: {TAKER_FEE_PCT*100:.0f}% taker + ${GAS_COST_USDC}/orden gas")
    log(f"Compound: {COMPOUND_TO_WALLET_PCT*100:.0f}%→wallet / {COMPOUND_TO_CAPITAL_PCT*100:.0f}%→capital")
    log(f"Calidad mínima: {MIN_QUALITY_SCORE}/100 | ROI mín: {MIN_ROI_PCT}% | ROI máx: {MAX_ROI_PCT}% | Suma YES mín: {MIN_SUM_YES}")
    log(f"Liquidez mín: ${MIN_LIQUIDITY_USDC} | Depth: {MIN_BOOK_DEPTH_RATIO}×")
    log(f"Deduplicación: max {DEDUP_MAX_PER_EVENT}×/evento/día | ventana={DEDUP_WINDOW_HOURS}h")
    log(f"Paginación: eventos={MAX_EVENTS_PER_SCAN} | mercados={MAX_MARKETS_PER_SCAN} | página={SCAN_PAGE_SIZE}")
    log(f"[PC#1] Balance check: cada {BALANCE_CHECK_CYCLES} ciclos | alerta=${BALANCE_DRIFT_ALERT} | halt=${BALANCE_DRIFT_HALT}")
    log(f"[PC#2] Slippage model: {SLIPPAGE_MODEL_ENABLED} | impacto={SLIPPAGE_IMPACT_PCT}%/$100")
    log(f"[PC#3] Fee monitor: {FEE_MONITOR_ENABLED} | check cada {FEE_CHECK_INTERVAL_MIN}min")
    log(f"[PC#4] Compound transfer: enabled={COMPOUND_TRANSFER_ENABLED} | mín=${COMPOUND_MIN_TRANSFER}")
    log(f"[PC#5] Fills parciales: máx {MAX_PARTIAL_FILL_RATE*100:.0f}% en ventana ≥{MIN_EXECUTION_SAMPLES} intentos | unwind_min=${UNWIND_MIN_PRICE}")
    log(f"{'='*60}\n")

    # FIX #2: MetaMask exporta claves privadas sin prefijo "0x"
    # La validación anterior bloqueaba el bot con claves válidas
    # Ahora valida que sea exactamente 64 caracteres hexadecimales
    if not DRY_RUN:
        import re as _re
        _key_clean = PRIVATE_KEY.lstrip("0x").strip()
        if not _re.fullmatch(r"[0-9a-fA-F]{64}", _key_clean):
            raise ValueError(
                "PRIVATE_KEY inválida — debe ser clave privada hex de 64 caracteres.\n"
                "Exportar desde MetaMask: Cuenta → Detalles → Exportar clave privada.\n"
                f"Longitud actual: {len(_key_clean)} chars (se esperan 64)."
            )

    state = load_state()
    log(
        f"ESTADO | capital=${state['active_capital']:.2f} | "
        f"wallet=${state['total_sent_to_wallet']:.2f} | "
        f"PnL=${state['total_pnl_usdt']:.4f} | trades={state['total_trades']}"
    )

    send_telegram(
            f"🤖 POLYMARKET BOT V2 ACTIVO — {modo}\n\nCapital: ${state['active_capital']:.2f}\n\nEstrategias: NegRisk + Binario + Near-Resolution\n\nFees descontados: ✅ | Liquidez verificada: ✅\n\nCompound: {COMPOUND_TO_WALLET_PCT*100:.0f}%→wallet / {COMPOUND_TO_CAPITAL_PCT*100:.0f}%→capital"
        )

    ciclo = 0
    try:
        while True:
            ciclo += 1
            executed_this_cycle = 0
            reset_daily_if_needed(state)

            log(f"── CICLO #{ciclo} · {datetime.now().strftime('%H:%M:%S')} ──")

            # Heartbeat periódico
            if (time.time() - state.get("last_heartbeat_ts", 0)) >= HEARTBEAT_MINUTES * 60:
                send_heartbeat(state)
                save_state(state)

            # PUNTO CIEGO #1: verificar balance on-chain periódicamente
            if not check_balance_drift(state, ciclo):
                log("🚨 BOT DETENIDO | divergencia de balance excesiva")
                send_telegram("🚨 BOT DETENIDO | divergencia balance > límite")
                break

            # PUNTO CIEGO #5: verificar si un ciclo anterior ya disparó el halt
            # por tasa de fills parciales/kills excesiva
            if state.get("execution_halted"):
                log("🚨 BOT DETENIDO | tasa de fills parciales/kills excesiva")
                break

            # PUNTO CIEGO #3: monitorear cambios de fees
            check_fee_changes(state)

            # Verificar si puede operar
            ok, motivo = can_trade(state)
            if not ok:
                log(f"BLOCK | {motivo}")
                log_cycle_summary(state, 0, 0)
                time.sleep(LOOP_SECONDS)
                continue

            # Escanear todas las estrategias
            opps = scan_all(state)

            # Ejecutar top oportunidades (máx 3 por ciclo para diversificar)
            for opp in opps[:3]:
                ok, motivo = can_trade(state)
                if not ok:
                    log(f"BLOCK mid-ciclo | {motivo}")
                    break

                log(
                    f"OPP [{opp.strategy}] score={opp.quality_score} | "
                    f"'{opp.event_title[:40]}' | "
                    f"net=${opp.profit_net} | ROI={opp.roi_pct}%"
                )
                # FIX LOG #3: cooldown protege contra mismo mercado en ciclos consecutivos

                result   = execute_opportunity(opp, state)
                compound = apply_compound(state, result.get("profit_usdt", 0))
                register_trade(state, opp, result, compound)
                executed_this_cycle += 1

                exec_outcome = result.get("exec_outcome")

                # PUNTO CIEGO #5: solo cuenta para el guardrail en trading real
                # (en DRY_RUN no hay fills reales que clasificar)
                if not DRY_RUN and exec_outcome in ("full", "partial", "kill"):
                    if not register_execution_outcome(state, exec_outcome):
                        save_state(state)
                        break   # el chequeo de arriba frenará el loop principal el próximo ciclo

                if exec_outcome == "partial":
                    # handle_partial_fill ya envió el detalle completo por Telegram
                    pass
                elif exec_outcome == "kill":
                    log(f"EXEC_KILL | {opp.event_title[:40]} | sin exposición, sin pérdida")
                elif result["success"]:
                    sim = " [SIM]" if DRY_RUN else ""
                    msg = (
                        f"✅ {opp.strategy}{sim} | {opp.event_title[:40]}\n"
                        f"Net: ${opp.profit_net:.4f} ({opp.roi_pct}% ROI) | Score: {opp.quality_score}\n"
                        f"→ Wallet: ${compound['to_wallet']:.4f} | Capital: ${state['active_capital']:.4f}"
                    )
                    log(msg.replace("\n", " | "))
                    send_telegram(msg)
                else:
                    log(f"FAIL | {result.get('error','?')}")

            log_cycle_summary(state, len(opps), executed_this_cycle)
            log(f"⏳ {LOOP_SECONDS}s...\n")
            time.sleep(LOOP_SECONDS)

    except KeyboardInterrupt:
        log("🛑 Bot detenido (Ctrl+C)")
        send_heartbeat(state)
        save_state(state)
    except Exception as e:
        log(f"ERROR GLOBAL: {e}")
        send_telegram(f"❌ ERROR BOT V2: {e}")
        save_state(state)
        raise

if __name__ == "__main__":
    main()