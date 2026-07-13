"""
fix_polymarket.py — Aplica 3 fixes quirúrgicos a polymarket_bot_v2.py
Ejecutar en el VPS: python3 fix_polymarket.py

Fixes:
  F1: Compound — eliminar límite MAX_CAPITAL en DRY_RUN
  F2: Win rate — simular pérdidas realistas en DRY_RUN (12% prob)
  F3: Slippage — conectar record_slippage() en register_trade()
  F4: .env — ajustar parámetros para simulación de calidad
"""
import ast, shutil, os, re
from datetime import datetime

BOT_FILE = "/root/bots/polymarket/polymarket_bot_v2.py"
ENV_FILE = "/root/bots/polymarket/.env"
BACKUP_TS = datetime.now().strftime("%Y%m%d_%H%M")

# ── Backup ────────────────────────────────────────────────────
shutil.copy(BOT_FILE, f"{BOT_FILE}.bak.{BACKUP_TS}")
shutil.copy(ENV_FILE, f"{ENV_FILE}.bak.{BACKUP_TS}")
print(f"✓ Backups creados: .bak.{BACKUP_TS}")

with open(BOT_FILE, "r") as f:
    src = f.read()

# ═══════════════════════════════════════════════════════════════
# FIX 1 — Compound: eliminar límite MAX_CAPITAL en DRY_RUN
# Bug: min(active_capital + to_capital, MAX_CAPITAL) = 0 siempre
#      porque active_capital ya empieza en MAX_CAPITAL ($200)
# Fix: DRY_RUN también crece libremente — es simulación de verdad
# ═══════════════════════════════════════════════════════════════
old_f1 = \
"""    to_capital = profit_usdt * COMPOUND_TO_CAPITAL_PCT
    # ── FIX DRY_RUN: capital simulado no puede superar MAX_CAPITAL ──────
    if DRY_RUN:
        cap_antes = state["active_capital"]
        state["active_capital"] = min(state["active_capital"] + to_capital, MAX_CAPITAL)
        to_capital = state["active_capital"] - cap_antes  # solo lo que realmente entró
    else:
        state["active_capital"] += to_capital  # producción: compound real sin límite
    # ────────────────────────────────────────────────────────────────────"""

new_f1 = \
"""    to_capital = profit_usdt * COMPOUND_TO_CAPITAL_PCT
    # [F1] Compound sin límite en DRY_RUN — simulación debe reflejar crecimiento real
    # Bug original: min(..., MAX_CAPITAL) congelaba capital en $200 para siempre
    state["active_capital"] += to_capital"""

assert old_f1 in src, "F1: ANCHOR no encontrado — verificar manualmente"
src = src.replace(old_f1, new_f1, 1)
print("✓ F1 aplicado: compound sin límite MAX_CAPITAL en DRY_RUN")

# ═══════════════════════════════════════════════════════════════
# FIX 2 — Win rate: simular pérdidas en DRY_RUN
# Bug: execute_opportunity en DRY_RUN siempre devuelve profit_net
#      → win rate 100% imposible en producción real
# Fix: 12% de probabilidad de fallo por:
#      - precio se movió antes de ejecutar (más común)
#      - liquidez insuficiente en un outcome
#      - mercado ya sin ineficiencia cuando llega la orden
# Resultado esperado: win rate ~88% (realista para NegRisk institucional)
# ═══════════════════════════════════════════════════════════════
old_f2 = \
"""    if DRY_RUN:
        log(f"[SIMULACIÓN] Batch de {len(opp.markets)} órdenes FOK:")
        for m in opp.markets:
            log(f"  BUY YES '{m['question'][:40]}' @ ${m['yes_price']} × {opp.sets} shares [FOK]")
        return {"success": True, "profit_usdt": opp.profit_net, "simulated": True}"""

new_f2 = \
"""    if DRY_RUN:
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
        return {"success": True, "profit_usdt": opp.profit_net, "simulated": True}"""

assert old_f2 in src, "F2: ANCHOR no encontrado — verificar manualmente"
src = src.replace(old_f2, new_f2, 1)
print("✓ F2 aplicado: pérdidas simuladas (loss_prob 6-18% según ROI)")

# ═══════════════════════════════════════════════════════════════
# FIX 3 — Slippage: conectar record_slippage() en register_trade()
# Bug: record_slippage() estaba definida pero nunca llamada
#      → slippage_history=[] y avg_slippage_pct=0.0 siempre
# Fix: registrar slippage al final de cada trade usando
#      roi_pct (esperado) vs roi realizado post-pérdidas
# ═══════════════════════════════════════════════════════════════
old_f3 = \
"""    history = state.get("trade_history", [])
    history.append({
        "ts":       datetime.now().isoformat(),
        "strategy": opp.strategy,
        "event":    opp.event_title[:50],
        "sets":     opp.sets,
        "cost":     opp.cost_total,
        "profit":   pnl,
        "roi_pct":  opp.roi_pct,
        "score":    opp.quality_score,
        "to_wallet":compound.get("to_wallet", 0),
        "capital":  compound.get("new_capital", cap),
        "sim":      result.get("simulated", False),
    })
    state["trade_history"] = history[-50:]
    save_state(state)"""

new_f3 = \
"""    history = state.get("trade_history", [])
    # ROI realizado: si fue pérdida, ROI real es negativo
    roi_realizado = (pnl / opp.cost_total * 100) if opp.cost_total > 0 else 0
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
    })
    state["trade_history"] = history[-50:]
    # [F3] Registrar slippage real: diferencia entre ROI esperado y realizado
    record_slippage(state, opp.roi_pct, roi_realizado)
    save_state(state)"""

assert old_f3 in src, "F3: ANCHOR no encontrado — verificar manualmente"
src = src.replace(old_f3, new_f3, 1)
print("✓ F3 aplicado: record_slippage() conectado en register_trade()")

# ── Verificar sintaxis ────────────────────────────────────────
try:
    ast.parse(src)
    print("✓ Sintaxis AST: OK")
except SyntaxError as e:
    print(f"✗ ERROR de sintaxis: {e}")
    print("  → Restaurando backup...")
    shutil.copy(f"{BOT_FILE}.bak.{BACKUP_TS}", BOT_FILE)
    exit(1)

# ── Verificar que los 3 fixes están en el resultado ──────────
checks = [
    ("[F1] Compound sin límite",          "[F1] Compound sin límite" in src),
    ("[F2] Simular pérdidas realistas",    "[F2] Simular pérdidas realistas" in src),
    ("[F3] Registrar slippage real",       "[F3] Registrar slippage real" in src),
    ("loss_prob calculado",                "loss_prob" in src),
    ("record_slippage en register_trade",  src.count("record_slippage(") >= 2),
    ("sim_loss en history",                "sim_loss" in src),
    ("roi_real en history",                "roi_real" in src),
]
print()
all_ok = True
for label, ok in checks:
    print(f"  {'✓' if ok else '✗'} {label}")
    if not ok: all_ok = False

if not all_ok:
    print("\n✗ Checks fallidos — restaurando backup")
    shutil.copy(f"{BOT_FILE}.bak.{BACKUP_TS}", BOT_FILE)
    exit(1)

# ── Escribir archivo final ────────────────────────────────────
with open(BOT_FILE, "w") as f:
    f.write(src)
print(f"\n✓ Archivo guardado: {BOT_FILE}")
print(f"  Líneas: {src.count(chr(10))}")

# ═══════════════════════════════════════════════════════════════
# FIX 4 — .env: parámetros para simulación de calidad
# ═══════════════════════════════════════════════════════════════
with open(ENV_FILE, "r") as f:
    env = f.read()

env_cambios = [
    ("MAX_TRADES_PER_DAY=100", "MAX_TRADES_PER_DAY=20"),
    ("MIN_ROI_PCT=1.5",        "MIN_ROI_PCT=5.0"),
    ("COOLDOWN_SECONDS=300",   "COOLDOWN_SECONDS=600"),
    ("MAX_ROI_PCT=20.0",       "MAX_ROI_PCT=15.0"),
    ("MIN_QUALITY_SCORE=60",   "MIN_QUALITY_SCORE=65"),
]

print()
for old, new in env_cambios:
    if old in env:
        env = env.replace(old, new, 1)
        print(f"  ✓ .env: {old} → {new}")
    else:
        # Puede que ya fue cambiado antes
        var = old.split("=")[0]
        current = [l for l in env.split("\n") if l.startswith(var)]
        print(f"  ~ .env: {var} ya es '{current[0] if current else 'no encontrado'}'")

with open(ENV_FILE, "w") as f:
    f.write(env)
print(f"\n✓ .env actualizado")

# ═══════════════════════════════════════════════════════════════
# RESET PARCIAL DEL STATE — conservar capital, limpiar stats
# ═══════════════════════════════════════════════════════════════
import json
from datetime import date

STATE_FILE = "/root/bots/polymarket/state_polymarket_v2.json"
shutil.copy(STATE_FILE, f"{STATE_FILE}.bak.{BACKUP_TS}")

with open(STATE_FILE, "r") as f:
    state = json.load(f)

# Reset: conservar solo capital y wallet acumulado
state_limpio = {
    "current_day":              str(date.today()),
    "session_start_capital":    200.0,
    "active_capital":           200.0,   # reset a $200 para nueva simulación limpia
    "total_sent_to_wallet":     0.0,     # reset — nueva simulación
    "total_added_to_capital":   0.0,
    "total_pnl_usdt":           0.0,
    "daily_pnl_usdt":           0.0,
    "daily_pnl_fraction":       0.0,
    "trades_today":             0,
    "total_trades":             0,
    "consecutive_losses":       0,
    "last_trade_timestamp":     0,
    "cooldown_until":           0,
    "last_heartbeat_ts":        0,
    "trade_history":            [],
    "stats_negrisk":            {"trades": 0, "pnl": 0.0, "wins": 0},
    "stats_binary":             {"trades": 0, "pnl": 0.0, "wins": 0},
    "stats_near_res":           {"trades": 0, "pnl": 0.0, "wins": 0},
    "stats_market_making":      {"trades": 0, "pnl": 0.0, "wins": 0},
    "executed_today":           {},
    "balance_onchain_last":     0.0,
    "balance_drift_total":      0.0,
    "balance_checks":           0,
    "balance_halted":           False,
    "slippage_history":         [],
    "avg_slippage_pct":         0.0,
    "trades_missed_latency":    0,
    "fee_current":              0.02,
    "fee_last_check_ts":        0,
    "fee_alerts":               0,
    "compound_pending_transfer":0.0,
    "compound_transferred_real":0.0,
}

with open(STATE_FILE, "w") as f:
    json.dump(state_limpio, f, indent=2)

print(f"\n✓ State reseteado — nueva simulación limpia desde $200")
print(f"  Backup del state anterior: {STATE_FILE}.bak.{BACKUP_TS}")
print()
print("=" * 55)
print("TODOS LOS FIXES APLICADOS — reiniciar el bot:")
print("  pm2 restart bot-polymarket --update-env")
print("  pm2 logs bot-polymarket --lines 20")
print("=" * 55)
