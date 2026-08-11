#!/usr/bin/env python3
"""
Raccoglie uno snapshot del traffico AIS in un'area geografica da aisstream.io
(WebSocket, gratuito) e lo scrive in data/vessels.json per essere pubblicato
su GitHub Pages / raw.githubusercontent.com e letto da TRMNL in polling.

Env richieste:
  AISSTREAM_API_KEY  -> la tua chiave da aisstream.io

Configurazione area: modifica BBOX qui sotto (default: costa Monaco/Riviera).
"""

import asyncio
import json
import os
import time

import websockets

# --- TEST DIAGNOSTICO TEMPORANEO #2 ---
# Verifichiamo se con il campo "APIKey" (corretto) arrivano dati usando
# tutto il mondo come area, prima di tornare a un'area piccola.
BBOX = [[-90, -180], [90, 180]]

LISTEN_SECONDS = 60
OUTPUT_PATH = os.path.join("data", "vessels.json")


async def collect() -> dict:
    api_key = os.environ.get("AISSTREAM_API_KEY", "").strip()
    print(f"DEBUG: lunghezza API key letta dal secret = {len(api_key)} caratteri")
    if not api_key:
        raise SystemExit("ERRORE: il secret AISSTREAM_API_KEY e' vuoto o non impostato.")

    vessels: dict[str, dict] = {}

    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
        subscribe_message = {
            "APIKey": api_key,
            "BoundingBoxes": [BBOX],
            "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
        }
        debug_copy = dict(subscribe_message)
        debug_copy["APIKey"] = api_key[:4] + "..." + api_key[-4:]
        print(f"DEBUG: messaggio di sottoscrizione inviato: {json.dumps(debug_copy)}")
        await ws.send(json.dumps(subscribe_message))

        deadline = time.monotonic() + LISTEN_SECONDS
        messages_received = 0
        while time.monotonic() < deadline:
            timeout = deadline - time.monotonic()
            if timeout <= 0:
                break
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except asyncio.TimeoutError:
                break
            except websockets.exceptions.ConnectionClosed as e:
                print(
                    f"DEBUG: connessione chiusa da aisstream.io - "
                    f"code={e.code} reason={e.reason!r} "
                    f"(messaggi ricevuti prima della chiusura: {messages_received})"
                )
                break

            messages_received += 1
            if messages_received == 1:
                print(f"DEBUG: primo messaggio grezzo ricevuto: {raw[:500]}")
            msg = json.loads(raw)

            # Il primo messaggio dopo un errore di sottoscrizione e' spesso
            # un messaggio di errore testuale, non un AIS message valido.
            if "error" in msg:
                print(f"DEBUG: aisstream.io ha risposto con un errore: {msg}")
                break
            mmsi = str(msg.get("MetaData", {}).get("MMSI", ""))
            if not mmsi:
                continue

            entry = vessels.setdefault(mmsi, {"mmsi": mmsi})
            entry["name"] = msg.get("MetaData", {}).get("ShipName", entry.get("name", "")).strip()

            msg_type = msg.get("MessageType")
            if msg_type == "PositionReport":
                pr = msg["Message"]["PositionReport"]
                entry["lat"] = round(pr.get("Latitude", 0), 4)
                entry["lon"] = round(pr.get("Longitude", 0), 4)
                entry["sog"] = round(pr.get("Sog", 0), 1)  # nodi
                entry["cog"] = round(pr.get("Cog", 0), 1)  # gradi
                entry["nav_status"] = pr.get("NavigationalStatus")
                entry["last_update"] = msg.get("MetaData", {}).get("time_utc")
            elif msg_type == "ShipStaticData":
                sd = msg["Message"]["ShipStaticData"]
                entry["ship_type"] = sd.get("Type")
                entry["destination"] = sd.get("Destination", "").strip()

    print(f"DEBUG: messaggi AIS totali ricevuti in questa sessione: {messages_received}")
    return vessels


def main() -> None:
    vessels_by_mmsi = asyncio.run(collect())

    # Tieni solo le navi di cui conosciamo posizione, ordinate per velocità decrescente
    vessels = [v for v in vessels_by_mmsi.values() if "lat" in v]
    vessels.sort(key=lambda v: v.get("sog", 0), reverse=True)

    snapshot = {
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "count": len(vessels),
        "vessels": vessels[:20],  # limite ragionevole per il display
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    print(f"Scritte {len(vessels)} navi in {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
