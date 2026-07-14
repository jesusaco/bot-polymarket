"""
Nano-piloto V2 - paso 1: SOLO derivacion/auth, sin ordenes ni movimiento de fondos.

Que hace:
  - Carga PK y EXPECTED_SIGNER desde nanopilot/.env.nanopilot (no incluido en
    el repo, lo rellena el usuario). EXPECTED_SIGNER es la EOA nueva completa
    (42 chars) -- el script NUNCA hardcodea una direccion de wallet propia
    (la rotacion de clave del 2026-07-14 fue precisamente por una direccion
    vieja quedando hardcodeada/expuesta en otro lado).
  - AsyncSecureClient.create(private_key=PK) SIN pasar wallet= -> deja que el
    SDK derive el Deposit Wallet por su cuenta (ver AsyncSecureClient.create:
    "wallet: Defaults to the signer's Deposit Wallet").
  - Verifica que el signer derivado (calculado por el SDK a partir de PK)
    coincide con EXPECTED_SIGNER -- falla fuerte si no coincide, como chequeo
    de que la PK correcta esta en el archivo.
  - Imprime el deposit wallet derivado SIN compararlo contra nada -- la cuenta
    es nueva, no hay un valor de referencia todavia. El usuario lo contrasta
    a mano contra polymarket.com (perfil/Settings -> Developers).
  - Imprime wallet_type y signer.
  - NO llama a place_limit_order / place_market_order / post_order(s) / ninguna
    funcion de transferencia. _ensure_wallet_ready() (invocada dentro de
    create()) solo hace un fetch_deployed() de lectura si el wallet ya esta
    desplegado -- no dispara un deploy ni gasta gas en ese caso.
  - No imprime ni persiste api_secret / api_passphrase.

Uso:
  ./venv_v2_proto/bin/python3 nanopilot/step1_wallet_auth.py
"""

import asyncio
import json
import sys
from pathlib import Path

ENV_FILE = Path(__file__).parent / ".env.nanopilot"
PAYLOAD_DIR = Path(__file__).parent / "v2_payloads"


def load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        sys.exit(
            f"No existe {path}. Copia .env.nanopilot.example a .env.nanopilot "
            "y rellena PK antes de correr este script."
        )
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
        sys.exit("PK vacia o con formato invalido en .env.nanopilot (debe empezar con 0x).")

    expected_signer = env.get("EXPECTED_SIGNER", "")
    if not expected_signer or not expected_signer.startswith("0x") or len(expected_signer) != 42:
        sys.exit(
            "EXPECTED_SIGNER vacia o incompleta en .env.nanopilot -- debe ser la "
            "direccion completa (42 chars) de la EOA nueva, para poder verificar "
            "que el signer derivado de PK es el que esperas."
        )

    print("Creando AsyncSecureClient (sin wallet= -> auto-derivacion)...")
    client = await AsyncSecureClient.create(private_key=pk)
    try:
        derived_wallet = client.wallet
        wallet_type = client.wallet_type
        signer = client.signer
        env_name = client.environment.name
        clob_url = client.environment.clob_url
        has_creds = client.credentials is not None

        print(f"environment:      {env_name} ({clob_url})")
        print(f"signer (EOA):     {signer}")
        print(f"wallet derivado:  {derived_wallet}  (deposit wallet -- contrastar a mano en polymarket.com)")
        print(f"wallet_type:      {wallet_type}")
        print(f"credenciales L2 obtenidas: {has_creds}")

        signer_ok = signer.lower() == expected_signer.lower()
        print(f"signer == EXPECTED_SIGNER: {signer_ok}")
        if not signer_ok:
            sys.exit(
                f"ATENCION: PK no corresponde a la EOA esperada. "
                f"esperado={expected_signer} derivado={signer}. Abortando sin guardar payload."
            )

        PAYLOAD_DIR.mkdir(exist_ok=True)
        out_file = PAYLOAD_DIR / "step1_wallet_auth.json"
        out_file.write_text(
            json.dumps(
                {
                    "environment": env_name,
                    "clob_url": clob_url,
                    "signer": signer,
                    "derived_wallet": derived_wallet,
                    "wallet_type": wallet_type,
                    "signer_matches_expected": signer_ok,
                    "credentials_obtained": has_creds,
                },
                indent=2,
            )
        )
        print(f"\nGuardado (sin secretos) en {out_file}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
