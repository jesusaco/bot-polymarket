"""
Nano-piloto V2 - chequeo de balance. SOLO lectura (get_balance_allowance),
ninguna orden, ningun movimiento de fondos.

Uso:
  ./venv_v2_proto/bin/python3 nanopilot/check_balance.py
"""

import asyncio
import json
import sys
from pathlib import Path

ENV_FILE = Path(__file__).parent / ".env.nanopilot"
PAYLOAD_DIR = Path(__file__).parent / "v2_payloads"


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


async def main() -> None:
    from polymarket.clients.async_secure import AsyncSecureClient

    env = load_env_file(ENV_FILE)
    pk = env.get("PK", "")
    if not pk or not pk.startswith("0x"):
        sys.exit("PK vacia o invalida en .env.nanopilot.")

    client = await AsyncSecureClient.create(private_key=pk)
    try:
        bal = await client.get_balance_allowance(asset_type="COLLATERAL")
        print(f"deposit wallet: {client.wallet}")
        print(f"balance/allowance (COLLATERAL / pUSD): {bal}")

        PAYLOAD_DIR.mkdir(exist_ok=True)
        out_file = PAYLOAD_DIR / "check_balance.json"
        out_file.write_text(
            json.dumps(
                {"wallet": client.wallet, "balance_allowance": bal.model_dump(mode="json")},
                indent=2,
                default=str,
            )
        )
        print(f"\nGuardado en {out_file}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
