"""
Nano-piloto V2 - paso 2: orden GTC minuscula, lejos del precio de mercado,
pensada para NO llenarse -- valida la cadena de firma + posteo sin arriesgar
el fill. Despues la cancela y confirma la cancelacion (sirve tambien de test
del camino de cancelacion que usa el bot real).

Mercado elegido (Gamma API, 2026-07-14, alto volumen):
  "France vs. Spain: Team to Advance" -- leg "France" (YES)
  token_id  = 112548421964662546558474258688565408276000153279440324883721010878524791926004
  best bid  = 0.575 / best ask = 0.5775 (CLOB /book, 2026-07-14)
  tick_size = 0.0025 ; minimum_order_size = 5 shares (CLOB /markets)

Orden: BUY 5 @ 0.05 (10x mas barato que el mejor bid 0.575) -> costo $0.25,
no deberia llenarse nunca a ese precio. GTC (sin expiration).

Uso:
  ./venv_v2_proto/bin/python3 nanopilot/step2_gtc_order.py
"""

import asyncio
import json
import sys
from pathlib import Path

ENV_FILE = Path(__file__).parent / ".env.nanopilot"
PAYLOAD_DIR = Path(__file__).parent / "v2_payloads"

TOKEN_ID = "112548421964662546558474258688565408276000153279440324883721010878524791926004"
PRICE = "0.05"
SIZE = "5"
SIDE = "BUY"


def load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        sys.exit(f"No existe {path}.")
    values: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def dump(obj) -> dict:
    return obj.model_dump(mode="json")


async def main() -> None:
    from polymarket.clients.async_secure import AsyncSecureClient

    env = load_env_file(ENV_FILE)
    pk = env.get("PK", "")
    if not pk or not pk.startswith("0x"):
        sys.exit("PK vacia o invalida en .env.nanopilot.")

    client = await AsyncSecureClient.create(private_key=pk)
    try:
        print(f"Posteando GTC {SIDE} {SIZE} @ {PRICE} en token {TOKEN_ID[:12]}...")
        resp = await client.place_limit_order(
            token_id=TOKEN_ID, price=PRICE, size=SIZE, side=SIDE
        )
        print(f"Respuesta: {resp}")

        PAYLOAD_DIR.mkdir(exist_ok=True)
        (PAYLOAD_DIR / "step2_gtc_order_post.json").write_text(
            json.dumps(dump(resp), indent=2, default=str)
        )

        order_id = getattr(resp, "order_id", None) or getattr(resp, "orderId", None)
        if not order_id:
            print("No hay order_id en la respuesta -- probablemente fue rechazada, nada que cancelar.")
            return

        print(f"\norder_id: {order_id}")
        print("Cancelando la orden de prueba...")
        cancel_resp = await client.cancel_order(order_id=order_id)
        print(f"Respuesta de cancelacion: {cancel_resp}")

        (PAYLOAD_DIR / "step2_gtc_order_cancel.json").write_text(
            json.dumps(dump(cancel_resp), indent=2, default=str)
        )
        print(f"\nGuardado en {PAYLOAD_DIR}/step2_gtc_order_post.json y step2_gtc_order_cancel.json")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
