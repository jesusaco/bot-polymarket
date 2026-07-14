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

## BLOQUEANTE 2026-07-14 — migración CLOB V2 (leer ANTES de tocar `signature_type` o cualquier código de ejecución real)

Descubierto durante la preparación del piloto EOA ($20), antes de aplicar ningún cambio de `signature_type`:

- Polymarket migró su exchange el **2026-04-28** ("CLOB V2"): contratos nuevos (CTF Exchange V2, NegRisk
  Exchange V2), colateral movido de USDC.e a un token nuevo **pUSD**, y el esquema de firma EIP-712 cambió
  (domain version 1→2, campos del struct de orden distintos — se eliminan `nonce`/`feeRateBps`/`taker`/
  `expiration` y se agregan `timestamp`/`metadata`/`builder`, modelo de fees ya no se calcula en el cliente
  sino que lo determina el protocolo al momento del match). Fuente: `docs.polymarket.com/v2-migration`.
- El venv de este repo tiene `py-clob-client==0.34.6` — la ÚLTIMA versión que existe en PyPI de ese paquete
  (release 2026-02-19, **antes** de la migración). Los docs de Polymarket dicen explícitamente que ese
  paquete legado *"only works against V1 and no longer works against production CLOB V2"*. El paquete
  correcto ahora es uno distinto, `py-clob-client-v2` (1.0.2 en PyPI a la fecha de este hallazgo) — no está
  instalado en este repo.
- Efecto práctico: `execute_opportunity()`, `attempt_unwind()` y `execute_compound_transfer()` (todo el path
  de trading real) están construidos contra un protocolo que dejó de aceptar órdenes reales hace ~2.5 meses.
  Como `DRY_RUN` ha estado en `true` todo este tiempo, nadie lo había notado — pero el primer intento de
  orden real del piloto de $20 muy probablemente sería rechazado por la API del CLOB en vivo, sin importar
  qué `signature_type` se use. Cambiar `signature_type=2→0` por sí solo NO resuelve esto.
- Bug adicional, independiente de la migración: `client.withdraw()` (usado en `execute_compound_transfer`)
  **no existe** en `py_clob_client` 0.34.6 — habría fallado igual aunque el resto funcionara.
- **No escribir un script de approve (Exchange/CTF) contra las direcciones del `py_clob_client` instalado**:
  esas direcciones son de los contratos V1, deprecados desde el 28 de abril — aprobarlas gastaría gas real
  sin sentido porque el exchange en vivo ya no liquida a través de ellas.
- Estado a la fecha de este hallazgo: en scoping (no resuelto). Antes de reactivar el trabajo de
  `signature_type`/approve/ejecución real, hay que responder: (1) si `py-clob-client-v2` en modo EOA evita
  los issues #64/#70 de binding API-key↔deposit-wallet, (2) el mapa completo de qué cambia en las 3 funciones
  de ejecución + las 3 inicializaciones de `ClobClient` + si el USDC de la EOA sirve tal cual o hay que
  convertir a pUSD, (3) si los scanners (Gamma API / order books) también cambiaron o solo la capa de
  ejecución, (4) si `classify_batch_result()` sigue siendo válido con el formato de respuesta de órdenes V2,
  (5) estimación de horas/riesgo y qué se puede probar sin dinero real. Hasta tener esas respuestas, el
  código de ejecución real NO debe tocarse ni el approve script debe escribirse.

### Scoping 2026-07-14 (investigado, sujeto a cambiar — el ecosistema V2 sigue inestable)

- **`signature_type=0` con `funder`=la propia EOA (el plan original de este piloto) está evidenciado como
  RECHAZADO en producción**: `py-clob-client-v2` issue #53 ("EOA basic flow rejected: maker address not
  allowed, please use the deposit wallet flow"), abierto 2026-05-08, sigue abierto, último comentario
  2026-06-24 — y ese único comentario de "workaround" en realidad NO usa `signature_type=0`: usa
  `signature_type=2` (`POLY_GNOSIS_SAFE`) con `funder=DEPOSIT_WALLET_ADDRESS` (una dirección de contrato
  DISTINTA de la EOA), firmando con la clave de la EOA. Conclusión: post-V2, Polymarket parece exigir SIEMPRE
  una identidad de "deposit wallet" (un contrato aparte, no la EOA en sí) para trading programático — el
  plan de "EOA pura sin proxy" no es viable tal como estaba pensado.
- Los issues #64/#70/#77 (que sí preguntó el usuario) son de `signature_type=3` (`POLY_1271`, el deposit
  wallet "nuevo" recomendado), no de sig_type=0 — por eso no aplican directamente, pero confirman el mismo
  patrón: hace falta una dirección de deposit wallet separada de la EOA.
- **Dato más reciente y mejor confirmado** (issue #70, comentarios de `crp4222`/`NSA013`, 2026-07-03):
  `signature_type=3`, `funder=DEPOSIT_WALLET_ADDRESS` (NO la EOA), `key=EOA_PRIVATE_KEY` — funciona en
  producción con `py-clob-client-v2` 1.0.2 sin parches, balance y fills verificados on-chain. PERO el
  comentario más nuevo del hilo (`datarhan`, 2026-07-08, 6 días antes de esta sesión) dice que desde ~1 de
  julio un workaround previo volvió a romperse y recomienda una librería no oficial de terceros
  (`pip install polyrails`) — señal de que esto sigue cambiando semana a semana. **No tratar "funcionó el 3
  de julio" como fundamento estable sin re-verificar el día que se implemente.**
- Antes de escribir una sola línea de migración: confirmar en polymarket.com (perfil/Settings → Developers)
  si esta cuenta/EOA ya tiene un deposit wallet provisto (dirección visible en la URL del perfil,
  `https://polymarket.com/0x...`, DISTINTA de la EOA) — si no existe, hay que averiguar cómo provisionarlo
  sin la UI (no confirmado que sea posible solo por SDK).
- Capa de lectura (scanners): Gamma API y el `/book` de `clob.polymarket.com` (usado en `check_liquidity`)
  parecen NO afectados por la migración — se probaron en vivo el 2026-07-14 y responden con el shape
  esperado. `fetch_current_fee()` → `/fee-rate-bps` SÍ está muerto (404 real, no un 404 de ruta cacheada) —
  `check_fee_changes()` queda inofensivo pero no mide nada real desde el 28 de abril.
- `classify_batch_result()` y el resto de `execute_opportunity()`/`attempt_unwind()` no se pueden validar
  sin instalar `py-clob-client-v2` y ver un payload real de respuesta — el README no expone el shape de
  respuesta ni confirma que exista posteo en batch (v2 muestra `create_and_post_order()` /
  `create_and_post_market_order()`, una orden a la vez; no se encontró equivalente confirmado de
  `post_orders()` con lista — si no existe, ejecutar un arb NegRisk multi-pata cerca-de-simultáneo necesita
  rediseño, no solo cambiar imports).
- Si existe testnet: **sí** — Amoy, `chain_id=80002`, con faucet gratis de MATIC/USDC de prueba. Toda la
  reescritura de la capa de ejecución se puede prototipar y probar ahí antes de tocar la EOA real con los
  $20. **CORREGIDO 2026-07-14 (sesión posterior, ver "Plan vigente" más abajo): esto era incorrecto — no
  hay motor de matching CLOB público en Amoy, solo el relayer de staging. Se descartó el testnet completo a
  favor de un nano-piloto contra producción con montos mínimos.**
- Estimado honesto: 6–10h de trabajo enfocado repartidas en ≥2 sesiones (swap de paquete, resolver
  identidad EOA↔deposit-wallet, reescribir las 3 inicializaciones + `execute_opportunity`/`attempt_unwind`/
  `execute_compound_transfer` para el nuevo API de órdenes y modelo de fees, revalidar
  `classify_batch_result` contra un payload real) — más si el posteo en batch no existe en v2 y hay que
  rediseñar la ejecución multi-pata con `asyncio`/paralelismo manual.

## Piloto EOA $20 — valores temporales (aplicados 2026-07-14)

- Camino elegido: EOA directa (`signature_type=0`) en vez de la deposit-wallet proxy (`signature_type=2`)
  que usa el código actual. **Esto NO se ha aplicado todavía** en `polymarket_bot_v2.py` — está bloqueado
  por el hallazgo de migración CLOB V2 de arriba. Cuando se aplique, es a propósito — **ninguna sesión
  futura debe "corregirlo" de vuelta a `signature_type=2`** sin releer esta sección primero.
- Lo que SÍ se aplicó ya el 2026-07-14 (backups con timestamp `20260714_023607` de `.env`,
  `state_polymarket_v2.json` y `polymarket_bot_v2.py` antes de tocar nada):
  - `.env`: `MAX_CAPITAL` 200→20, `MAX_RISK_PERCENT` 5→25, `BALANCE_DRIFT_HALT` 20→50,
    `BALANCE_DRIFT_ALERT` 5→45. `DRY_RUN` se queda en `true` — el usuario lo cambia manualmente al final,
    después de aprobar el resto.
    Nota: en términos relativos al nuevo `MAX_CAPITAL=20`, `BALANCE_DRIFT_HALT=50` es 2.5× todo el capital
    del piloto (antes era 10% de `MAX_CAPITAL=200`) — mucho más laxo que el guardrail original. Valores
    dados explícitamente por el usuario; señalado aquí por si no era la intención.
  - `state_polymarket_v2.json`: reset de `active_capital`→20.0 y `compound_pending_transfer`→0.0 únicamente;
    el resto del historial (`trade_history`, stats por estrategia, `slippage_history`, `executed_today`,
    etc.) se dejó intacto a propósito.
  - Advertencia operativa: el proceso pm2 sigue vivo con la config vieja en memoria (`MAX_CAPITAL=200`) hasta
    que el usuario reinicie. Con `active_capital=20` ya en el JSON pero `MAX_CAPITAL` viejo en memoria,
    `can_trade()` calculará `min_cap = 200*0.20 = $40 > $20` y bloqueará nuevos trades DRY_RUN con
    `CAPITAL_BAJO` hasta el restart — es inofensivo (sigue en DRY_RUN) pero puede verse raro en los logs.

### ROTACIÓN DE CLAVE 2026-07-14 — EOA y deposit wallet anteriores RETIRADOS

- La EOA `0xE30A...839b` y su deposit wallet `0x799ddf8aF7DCd36908B9f68bA029Ccc3F1D3F192` (documentados abajo)
  quedaron **RETIRADOS** el 2026-07-14 por exposición accidental de la clave privada. El usuario rotó a una
  EOA nueva (`0xb9D97...76f04`, forma truncada — la completa vive únicamente en `.env` de producción y en
  `nanopilot/.env.nanopilot`, ambos read-denied para Claude) y ya actualizó tanto el `.env` de producción
  (con backup previo) como `nanopilot/.env.nanopilot`.
- **Ninguna sesión futura debe volver a usar `0xE30A...839b` ni `0x799ddf...F192` como wallets activas** — se
  dejan documentados abajo solo como contexto histórico de cómo se armó el mapa de tres direcciones por
  cuenta (patrón que se repite: EOA / deposit wallet / address de perfil).

### Identidad de wallets de esta cuenta — VIGENTE (EOA rotada, confirmado 2026-07-14)

- **EOA (signer)**: `0xb9D97...76f04` (forma truncada; completa solo en `.env` de producción y
  `nanopilot/.env.nanopilot`, ambos read-denied para Claude).
- **Deposit wallet confirmado**: `0x9385D0d4F73f5b7Ce73765C0AA54fa63fa889d8c` — derivado por
  `AsyncSecureClient.create()` (SDK oficial `polymarket-client`, sin pasar `wallet=`) en
  `nanopilot/step1_wallet_auth.py`, y contrastado a mano por el usuario contra "Dirección de desarrollador"
  en polymarket.com — **coincide**. Requirió onboarding previo por la web (ver hallazgo justo abajo: EOA
  nueva sin deposit wallet desplegado). `wallet_type` reportado por el SDK: `DEPOSIT_WALLET`. Credenciales
  L2 obtenidas correctamente (`create_or_derive` funcionó sin pisar el bug del issue #70).
  Payload guardado en `nanopilot/v2_payloads/step1_wallet_auth.json`.
- Todavía no hay dirección de perfil de Polymarket (`polymarket.com/0x...`) documentada para la cuenta
  nueva — pendiente si hace falta más adelante.
- **Misma advertencia que la cuenta anterior**: nunca transferir tokens directamente a
  `0x9385D0d4F73f5b7Ce73765C0AA54fa63fa889d8c` — se pierden para siempre. Único fondeo válido: botón
  **Depósito** de la web de Polymarket (USDC→pUSD, acredita internamente).
- **Estado de fondos (2026-07-14, reconciliado)**: ~$5 USDC en el deposit wallet nuevo
  (`0x9385...889d8c`) — depósito de verificación hecho a propósito por el usuario vía el botón **Depósito**
  de la web, para constatar que el deposit wallet quedaba operativo tras el onboarding (ver hallazgo de
  `step1_wallet_auth.py` arriba). El resto (~$22 de los $27.29 originales) permanece en la EOA
  `0xb9D97...76f04`, sin depositar todavía.
  Nota positiva: `nanopilot/check_balance.py` leyendo ese saldo de $5 vía `AsyncSecureClient` confirma que
  las consultas de balance/allowance por SDK **sí funcionan** desde este VPS — el geoblock descubierto hoy
  (ver sección "BLOQUEANTE 2026-07-14 — geoblock VPS" abajo) afecta solo la apertura de posiciones, no la
  lectura.

### Identidad de wallets de esta cuenta — HISTÓRICO, cuenta retirada (confirmado 2026-07-14, ver rotación arriba)

- **Deposit wallet confirmado (RETIRADO)**: `0x799ddf8aF7DCd36908B9f68bA029Ccc3F1D3F192` — verificado en
  Polygonscan como contrato `DepositWallet`, recibió los depósitos del 2026-07-14. Fue el candidato a
  `funder` para la receta del issue #70 (`signature_type=3`, `funder`=este contrato, `key`=la EOA). Distinto
  de la EOA (`0xE30A...839b`, la que firmaba) y del address de perfil de Polymarket (`0x1C04...`). Tres
  direcciones para una sola cuenta — ver bitácora del 2026-07-13 para el detalle de cómo se armó el mapa.
  **No usar — cuenta retirada, ver rotación arriba.**
- **ADVERTENCIA CRÍTICA — nunca transferir tokens directamente a `0x799ddf8aF7DCd36908B9f68bA029Ccc3F1D3F192`**:
  Polymarket advierte que los fondos enviados por transferencia directa a un deposit wallet se pierden para
  siempre. El único fondeo válido es el botón **Depósito** de la web de Polymarket (que convierte USDC→pUSD
  y acredita internamente). Esta dirección se usa ÚNICAMENTE como parámetro `funder` en la config del bot —
  jamás como destino de una transacción de transferencia.
- **Estado actual de fondos (2026-07-14, post-rotación)**: 24.78 USDC + ~29.5 POL consolidados en la EOA
  NUEVA (`0xb9D97...76f04`, $27.29 total) — barrido confirmado on-chain el 2026-07-14 ~11:45 hora local
  (tx fee <$0.01), ejecutado por el usuario antes de que este archivo documentara el riesgo de la clave
  expuesta. La EOA vieja `0xE30A...839b` quedó vacía salvo polvo de gas — riesgo residual nulo, sin acción
  pendiente. Cuando el nano-piloto (ver sección más abajo) confirme el flujo de punta a punta, re-depositar
  vía el botón web de Polymarket (no por transferencia directa, ver advertencia arriba).
- **`BALANCE_DRIFT_HALT=50` / `BALANCE_DRIFT_ALERT=45` — confirmado intencional por el usuario, pero con una
  trampa conocida**: mientras `DRY_RUN=true`, `check_balance_drift()` nunca llama a `get_onchain_balance()`
  (usa la rama `BALANCE_SIM` de estimación). Pero si `FUNDER_ADDRESS` está seteado y algún día se corre con
  `DRY_RUN=false` contra el endpoint real, `get_onchain_balance()` puede devolver `0.0` "por diseño" cuando
  la respuesta de `/positions` es una lista (rama `elif isinstance(resp, list): cash = 0.0`, ver comentario
  `FIX #1` en el código) en vez de `-1.0` (que sí significa "no verificable"). Con `active_capital=$20` y esa
  lectura de `$0`, el drift calculado es `$20` — por debajo de `BALANCE_DRIFT_ALERT=45`, así que ni la alerta
  ni el halt dispararían aunque el balance real estuviera mal. El guardrail queda neutralizado de facto
  durante el piloto por esta combinación de umbrales laxos + lectura de balance potencialmente falsa.
  **Arreglar esto es prioridad 1 post-piloto** (antes de escalar capital más allá de los $20 de prueba).

## Plan vigente — "nano-piloto" V2 contra producción (revisado 2026-07-14, reemplaza el plan de Amoy testnet)

Investigado en sesión del 2026-07-14: **no existe un testnet CLOB público con order-matching real.**
`py-sdk`/`polymarket-client` (el SDK oficial, ver abajo) solo empaqueta un `Environment`: `PRODUCTION`
(`environments.py` en `Polymarket/py-sdk`). `py-clob-client-v2` sí define la constante `AMOY = 80002`, pero
su propio ejemplo con deposit wallet usa por defecto `host = "http://localhost:8080"` — Amoy ahí implica
correr tu propio motor de matching, no un servicio público de Polymarket. Lo único públicamente confirmado
en Amoy es el *relayer* de staging (`relayer-v2-staging.polymarket.dev`, solo despliegue de deposit wallet,
no matching). El plan de prototipar todo en Amoy sin arriesgar dinero real **no es viable** — se reemplaza
por un nano-piloto con montos mínimos reales.

- **SDK a usar**: `polymarket-client` (PyPI, oficial, repo `Polymarket/py-sdk`), NO `py-clob-client-v2`. El
  propio README de `py-clob-client-v2` recomienda migrar a este; el issue #70 (L1 auth liga la API key a la
  EOA en vez de al deposit wallet) sigue abierto y sin fix en `py-clob-client-v2` después de 8 semanas —
  confirmado con los 37 comentarios del issue vía API pública de GitHub, el último de HOY mismo (13:11 UTC).
  Camino confirmado funcionando por 3 reportantes independientes (el más reciente, hoy):
  `AsyncSecureClient.create(private_key=PK)` sin pasar `wallet=` deja que el SDK derive el deposit wallet
  solo, evitando la clase de bug del #70.
- Procedencia del paquete verificada antes de instalar (autor `engineering@polymarket.com`, org GitHub
  `Polymarket` verificada, `pyproject.toml` del repo enlaza de vuelta al mismo paquete/versión, sha256 del
  wheel instalado coincide con el publicado en PyPI). Instalado con versión fijada
  (`polymarket-client==0.1.0b19`) en venv aislado `venv_v2_proto/` — NO toca `venv/` de producción.
- Batch posting SÍ existe en el SDK oficial: `build_post_orders_request()` → POST `/orders`, máximo 15
  órdenes por lote — misma forma que el código actual del bot. Responde la pregunta abierta del scoping
  anterior sobre si haría falta rediseñar la ejecución multi-pata.
- Estructura del prototipo: carpeta `nanopilot/` (scripts + `nanopilot/v2_payloads/` para las respuestas
  crudas capturadas, sin secretos). Credenciales en `nanopilot/.env.nanopilot` (fuera del venv a propósito —
  un venv es desechable/regenerable, no debe cargar secretos; confirmado empíricamente que
  `Read(.env.*)` de `.claude/settings.json` bloquea la lectura de ese archivo para Claude aunque esté en un
  subdirectorio nuevo).
- Secuencia: (1) `nanopilot/step1_wallet_auth.py` — solo derivación/auth, sin órdenes ni movimiento de
  fondos, confirma signer y deposit wallet derivados; (2) orden GTC minúscula lejos del precio (no debe
  llenarse) para validar la cadena de firma sin riesgo; (3) orden marketable diminuta que sí llene, para
  capturar el payload real y revalidar `classify_batch_result()`; (4) batch de 2 patas pequeñas para ver la
  forma real de una respuesta multi-orden. Cada respuesta cruda se guarda en `nanopilot/v2_payloads/` (sin
  `api_secret`/`api_passphrase`) como insumo de la siguiente sesión.
- El bot en PM2, `venv/`, `polymarket_bot_v2.py` y `state_polymarket_v2.json` no se tocan durante el
  nano-piloto — es un experimento aislado con montos mínimos reales, separado del piloto de $20.
- Con eso capturado, la siguiente sesión ataca el mapeo completo de las 3 funciones de ejecución + 3
  inicializaciones de cliente en el bot real, usando el payload real en vez de suposiciones.

### Hallazgo 2026-07-14 (nano-piloto, primer intento de `step1_wallet_auth.py`) — EOA nueva sin deposit wallet

- Con la EOA nueva (post-rotación, nunca usada antes), `AsyncSecureClient.create(private_key=PK)` **no es
  puramente lectura/derivación** como se asumió: internamente llama a `_ensure_wallet_ready()`, que revisa
  si el deposit wallet ya está desplegado (`fetch_deployed`) y, si no lo está, intenta desplegarlo vía el
  flujo *gasless* del relayer (`_deploy_default_deposit_wallet` → `submit_deposit_wallet_create`). Eso
  **exige** una Builder API Key o Relayer API Key (`api_key=` al construir el cliente) — sin una, revienta
  con `UserInputError: Gasless transactions require a Builder API Key or Relayer API Key` **antes** de
  someter nada on-chain (falla segura, no se gastó gas ni se desplegó nada a medias).
- Con la EOA vieja (retirada) esto no se había notado porque su deposit wallet ya estaba desplegado desde
  antes — `fetch_deployed` daba `True` y `_ensure_wallet_ready()` retornaba de inmediato sin tocar el
  camino de deploy.
- Patrón consistente con lo visto en los comentarios del issue #70 (`pmcr9367`, `crp4222`): cuentas que
  primero se conectan/usan por la web de polymarket.com quedan onboardeadas — el deposit wallet se
  despliega gasless con las credenciales internas de Polymarket, sin que el desarrollador necesite su
  propia Builder/Relayer API key.
- **Regla para toda EOA nueva de aquí en adelante: conectarla/usarla primero en polymarket.com (login o
  Settings → Developers) ANTES del primer uso del SDK.** Si en algún momento hace falta desplegar el
  deposit wallet puramente por SDK sin pasar por la web, la alternativa es generar una Builder/Relayer API
  key propia en el dashboard de Polymarket y pasarla como `api_key=` a `AsyncSecureClient.create()` — no
  probado todavía, es el plan B si el onboarding web no alcanza.

## BLOQUEANTE 2026-07-14 — geoblock del VPS (leer ANTES de retomar el nano-piloto en step2)

- Al intentar `nanopilot/step2_gtc_order.py` (orden GTC mínima, ver "Plan vigente" arriba), la apertura de
  posición fue bloqueada: el VPS donde corre este repo está ubicado en Frankfurt, Alemania, y Polymarket
  geobloquea Alemania para operaciones de trading (abrir posiciones). Las consultas de solo-lectura (Gamma
  API, `/book`, y balance/allowance vía SDK — ver `check_balance.py` arriba) **no** están afectadas; el
  bloqueo es específico a colocar órdenes.
- **Decisión tomada**: migrar `bot-polymarket` a una región no restringida. Candidato: **AWS Lightsail,
  Dublín (Irlanda)**. No confirmado/ejecutado todavía — es una decisión, no una migración completada.
- **Efecto en el nano-piloto**: queda **pausado en el step2** (orden GTC) hasta que la migración de región
  se resuelva — no tiene sentido seguir prototipando pasos 3/4 (orden marketable, batch multi-pata) desde
  una IP que no puede colocar órdenes en absoluto.
- Esto también aplica al bot de producción bajo PM2 en este mismo VPS: aunque sigue en `DRY_RUN=true` (no
  coloca órdenes reales), el mismo geoblock aplicaría el día que se intente ejecución real — la migración de
  región es un prerrequisito para CUALQUIER trading real desde esta máquina, no solo para el nano-piloto.

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
