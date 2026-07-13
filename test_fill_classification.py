"""
test_fill_classification.py — harness offline para classify_batch_result()
Ejecutar: python3 test_fill_classification.py

Valida, sin red ni dinero real, que la clasificación de fills parciales
(PUNTO CIEGO #5, auditoría 2026-07-06) categoriza correctamente la
respuesta cruda de POST /orders en full / partial / kill, incluyendo los
casos defensivos (status inesperado, campos faltantes).

No requiere DRY_RUN=false ni reiniciar el bot — importa polymarket_bot_v2
como módulo (no ejecuta main(), guardado tras `if __name__ == "__main__"`).
"""
import sys
from dataclasses import replace as _replace

import polymarket_bot_v2 as bot

FAILS = []


def make_opp(n_legs: int, sets: int = 11) -> "bot.Opportunity":
    markets = [
        {"question": f"Leg {i}", "yes_price": 0.4, "yes_token": f"token_{i}"}
        for i in range(n_legs)
    ]
    return bot.Opportunity(
        strategy="NEGRISK", event_title="Test Event", event_id="evt_test",
        sum_cost=0.8, profit_gross=1.0, profit_net=1.0, roi_pct=10.0,
        sets=sets, cost_total=8.8, quality_score=70, markets=markets,
    )


def matched(order_id="0xabc"):
    return {"success": True, "errorMsg": "", "orderID": order_id, "status": "matched",
            "makingAmount": "11", "takingAmount": "4.4",
            "transactionsHashes": ["0xhash"], "tradeIDs": ["1"]}


def killed(err="FOK_ORDER_NOT_FILLED_ERROR"):
    return {"success": False, "errorMsg": err, "orderID": None, "status": None,
            "makingAmount": "0", "takingAmount": "0",
            "transactionsHashes": [], "tradeIDs": []}


def check(label, opp, resp_list, expected_outcome, expected_filled_count):
    result = bot.classify_batch_result(opp, resp_list)
    ok = (result["outcome"] == expected_outcome and
          result["filled_count"] == expected_filled_count and
          result["total_legs"] == len(opp.markets))
    status = "OK" if ok else "FAIL"
    print(f"  [{status}] {label} → outcome={result['outcome']} "
          f"filled={result['filled_count']}/{result['total_legs']} "
          f"(esperado: {expected_outcome}, {expected_filled_count}/{len(opp.markets)})")
    if not ok:
        FAILS.append(label)
    return result


print("=" * 60)
print("TEST classify_batch_result() — 6 casos")
print("=" * 60)

# 1. Fill completo (2 patas, ambas matched)
opp1 = make_opp(2)
check("1. Fill completo (2/2 matched)", opp1, [matched("0xa"), matched("0xb")], "full", 2)

# 2. Kill total (2 patas, ambas killed)
opp2 = make_opp(2)
check("2. Kill total (0/2 matched)", opp2, [killed(), killed()], "kill", 0)

# 3. Parcial 1-de-2
opp3 = make_opp(2)
check("3. Parcial 1/2 (pata 0 llenada, pata 1 killed)", opp3, [matched("0xa"), killed()], "partial", 1)

# 4. Parcial 1-de-3 (N>2 patas — NegRisk multi-outcome)
opp4 = make_opp(3)
check("4. Parcial 1/3 (1 matched, 2 killed)", opp4,
      [matched("0xa"), killed(), killed("FOK_ORDER_NOT_FILLED_ERROR")], "partial", 1)

# 5. Status inesperado (defensivo) — GTC-style "live"/"delayed"/"unmatched"
#    en vez de "matched"/killed. Un FOK real no debería devolver esto, pero
#    la clasificación debe tratarlo como NO-filled, no como éxito.
opp5 = make_opp(2)
weird_resp = [
    {"success": True, "errorMsg": "", "orderID": "0xc", "status": "live",
     "makingAmount": "0", "takingAmount": "0", "transactionsHashes": [], "tradeIDs": []},
    {"success": True, "errorMsg": "", "orderID": "0xd", "status": "unmatched",
     "makingAmount": "0", "takingAmount": "0", "transactionsHashes": [], "tradeIDs": []},
]
check("5. Status inesperado (live/unmatched, no matched)", opp5, weird_resp, "kill", 0)

# 6. Respuesta malformada / campos faltantes / batch más corto que las patas
opp6 = make_opp(3)
malformed_resp = [
    {},                                    # dict vacío — sin 'success' ni 'status'
    matched("0xe"),                       # una pata sí llena
    # ¡falta la 3ra respuesta! (resp_list más corto que opp.markets)
]
check("6. Malformado (dict vacío + resp_list corto)", opp6, malformed_resp, "partial", 1)

print()
if FAILS:
    print(f"✗ {len(FAILS)} caso(s) fallaron: {FAILS}")
    sys.exit(1)
else:
    print(f"✓ Los 6 casos pasaron — classify_batch_result() clasifica correctamente")
    sys.exit(0)
