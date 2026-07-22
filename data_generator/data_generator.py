"""
Generador de datos sinteticos para deteccion de fraude transaccional.

Produce un raw landing realista para una plataforma MLOps:
  raw/dimensions/customers.jsonl   (referencia)
  raw/dimensions/merchants.jsonl   (referencia)
  raw/dimensions/cards.jsonl       (referencia)
  raw/transactions/dt=YYYY-MM-DD/transactions.jsonl   (feed conocido en el swipe)
  raw/fraud_labels/dt=YYYY-MM-DD/labels.jsonl          (feed retrasado: chargebacks)

Decisiones MLOps clave:
  - event_timestamp != ingestion_timestamp (habilita features point-in-time).
  - La etiqueta de fraude vive en un feed APARTE y llega con retraso
    (label_timestamp = event + N dias), para evitar label leakage.
  - Las particiones por dt se calculan sobre event_timestamp (no ingestion),
    porque es el tiempo logico del negocio.
  - --dirty-rate inyecta defectos a proposito para ejercitar la cuarentena de
    Silver. Cada fila inyectada tiene EXACTAMENTE UN defecto, de modo que los
    conteos concilian 1:1 contra silver.quarantine por reject_reason.

Uso:
  python data_generator.py --customers 2000 --days 60 --out ./output --seed 42
  python data_generator.py --customers 2000 --days 60 --dirty-rate 0.001 --out ./output
"""

from __future__ import annotations
import argparse
import copy
import json
import os
from datetime import datetime, timedelta, timezone

import numpy as np
from faker import Faker

MCC = [
    "grocery", "restaurant", "fuel", "electronics", "travel",
    "online_retail", "atm_withdrawal", "subscription", "luxury", "gambling",
]
CHANNELS = ["pos", "online", "atm"]
COUNTRIES = ["MX", "US", "ES", "BR", "CA", "GB", "DE"]
CARD_TYPES = ["debit", "credit"]
NETWORKS = ["visa", "mastercard", "amex"]
TIERS = ["standard", "gold", "platinum"]
AGE_BANDS = ["18-25", "26-35", "36-50", "51-65", "65+"]
RISK_BANDS = ["low", "medium", "high"]

# Defectos inyectables. Cada uno mapea 1:1 con un reject_reason de
# config/quality_rules.yaml, para poder conciliar contra silver.quarantine.
DEFECTS = [
    ("missing_key",      "llave ausente"),
    ("null_event_ts",    "event_timestamp nulo"),
    ("future_event_ts",  "timestamp futuro"),
    ("null_amount",      "monto nulo"),
    ("negative_amount",  "monto negativo"),
    ("null_customer",    "customer_id nulo"),
    ("null_merchant",    "merchant_id nulo"),
    ("orphan_customer",  "cliente inexistente"),
    ("orphan_merchant",  "comercio inexistente"),
    ("duplicate",        "duplicado (version antigua)"),
]


def uid(rng, prefix: str, n: int = 12) -> str:
    """
    Identificador deterministico derivado del rng SEMBRADO.

    uuid.uuid4() usa el generador del sistema operativo e IGNORA la semilla:
    con --seed 42 producia IDs distintos en cada corrida, volviendo la
    reproducibilidad puramente cosmetica. Peor: como Bronze carga con MERGE
    insert-only sobre transaction_id pero reemplaza las dimensiones, regenerar
    hacia que las transacciones de la generacion anterior quedaran huerfanas
    (su customer_id ya no existia en la dim) y Silver las mandaba enteras a
    cuarentena.

    Sacando los IDs del rng sembrado, la misma semilla produce los mismos IDs:
    Bronze reconoce las filas como existentes y regenerar deja de acumular.
    128 bits en dos tiradas de 64: colisiones despreciables a esta escala.
    """
    hi = int(rng.integers(0, 2**63))
    lo = int(rng.integers(0, 2**63))
    return f"{prefix}_{hi:016x}{lo:016x}"[: len(prefix) + 1 + n]


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_jsonl(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def build_dimensions(fake: Faker, rng: np.random.Generator,
                     n_customers: int, n_merchants: int, start: datetime):
    customers, cards, profiles = [], [], {}
    for _ in range(n_customers):
        cid = uid(rng, "cust", 12)
        home = rng.choice(COUNTRIES, p=[0.55, 0.18, 0.07, 0.07, 0.05, 0.04, 0.04])
        signup = start - timedelta(days=int(rng.integers(30, 1500)))
        customers.append({
            "customer_id": cid,
            "signup_date": signup.strftime("%Y-%m-%d"),
            "home_country": home,
            "age_band": rng.choice(AGE_BANDS),
            "account_tier": rng.choice(TIERS, p=[0.7, 0.22, 0.08]),
        })
        profiles[cid] = {
            "home": home,
            "amt_mean": float(rng.uniform(15, 220)),
            "amt_std": float(rng.uniform(5, 60)),
            "n_cards": int(rng.integers(1, 4)),
            "active_hours": sorted(rng.choice(range(7, 23), size=6, replace=False).tolist()),
            # dispositivos habituales del cliente (telefono, laptop, tablet...).
            # El fraude usara uno NUEVO, fuera de este set: asi is_new_device es
            # una feature legitima y no un delator (ver nota al pie del archivo).
            "devices": [uid(rng, "dev", 8) for _ in range(int(rng.integers(1, 4)))],
        }
        for _ in range(profiles[cid]["n_cards"]):
            card_id = uid(rng, "card", 12)
            cards.append({
                "card_id": card_id,
                "customer_id": cid,
                "issue_date": (signup + timedelta(days=int(rng.integers(0, 400)))).strftime("%Y-%m-%d"),
                "card_type": rng.choice(CARD_TYPES, p=[0.6, 0.4]),
                "network": rng.choice(NETWORKS, p=[0.5, 0.4, 0.1]),
            })
            profiles[cid].setdefault("cards", []).append(card_id)

    merchants = []
    for _ in range(n_merchants):
        mid = uid(rng, "merch", 10)
        cat = rng.choice(MCC)
        risk = "high" if cat in ("gambling", "luxury") else rng.choice(RISK_BANDS, p=[0.7, 0.25, 0.05])
        merchants.append({
            "merchant_id": mid,
            "name": fake.company(),
            "category": cat,
            "country": rng.choice(COUNTRIES),
            "risk_band": risk,
        })
    return customers, cards, merchants, profiles


def make_txn(rng, profile, card_id, merchants, ts, amount=None,
             channel=None, country=None, device_id=None):
    m = merchants[int(rng.integers(len(merchants)))]
    if amount is None:
        amount = max(1.0, float(rng.normal(profile["amt_mean"], profile["amt_std"])))
    ch = channel or str(rng.choice(CHANNELS, p=[0.55, 0.35, 0.10]))
    if device_id is None and ch in ("online", "pos"):
        # actividad normal: uno de los dispositivos habituales del cliente.
        # atm no lleva dispositivo (un cajero no es un device del cliente).
        device_id = str(rng.choice(profile["devices"]))
    return {
        "transaction_id": uid(rng, "txn", 32),
        "event_timestamp": iso(ts),
        "customer_id": None,  # se llena afuera
        "card_id": card_id,
        "merchant_id": m["merchant_id"],
        "amount": round(amount, 2),
        "currency": "MXN" if (country or profile["home"]) == "MX" else "USD",
        "channel": ch,
        "country": country or profile["home"],
        "device_id": device_id,
        "ingestion_timestamp": iso(ts + timedelta(seconds=int(rng.integers(2, 90)))),
    }


def label_delay(rng, is_fraud: bool) -> timedelta:
    days = rng.integers(7, 35) if is_fraud else rng.integers(1, 10)
    return timedelta(days=int(days), hours=int(rng.integers(0, 24)))


def _record(txn, is_fraud, fraud_type, rng, txns_by_day, labels_by_day):
    event_dt = datetime.strptime(txn["event_timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    dt = event_dt.strftime("%Y-%m-%d")
    txns_by_day.setdefault(dt, []).append(txn)
    label_ts = event_dt + label_delay(rng, is_fraud)
    label = {
        "transaction_id": txn["transaction_id"],
        "is_fraud": int(is_fraud),
        "fraud_type": fraud_type,
        "label_timestamp": iso(label_ts),
    }
    labels_by_day.setdefault(label_ts.strftime("%Y-%m-%d"), []).append(label)


def _inject_fraud(rng, cust, p, merchants, start, days, txns_by_day, labels_by_day) -> int:
    cid = cust["customer_id"]
    card = str(rng.choice(p["cards"]))
    day = start + timedelta(days=int(rng.integers(0, days)))
    base = day + timedelta(hours=int(rng.integers(0, 24)), minutes=int(rng.integers(0, 60)))
    scenario = rng.choice(["card_testing", "account_takeover", "geo_impossible"])
    count = 0

    if scenario == "card_testing":
        for i in range(int(rng.integers(6, 15))):
            ts = base + timedelta(minutes=i * int(rng.integers(1, 3)))
            t = make_txn(rng, p, card, merchants, ts, amount=float(rng.uniform(1, 9)),
                         channel="online", device_id=uid(rng, "dev", 8))
            t["customer_id"] = cid
            _record(t, True, scenario, rng, txns_by_day, labels_by_day)
            count += 1

    elif scenario == "account_takeover":
        foreign = str(rng.choice([c for c in COUNTRIES if c != p["home"]]))
        t = make_txn(rng, p, card, merchants, base,
                     amount=float(rng.uniform(800, 5000)), channel="online",
                     country=foreign, device_id=uid(rng, "dev", 8))
        t["customer_id"] = cid
        _record(t, True, scenario, rng, txns_by_day, labels_by_day)
        count += 1

    else:  # geo_impossible
        foreign = str(rng.choice([c for c in COUNTRIES if c != p["home"]]))
        t1 = make_txn(rng, p, card, merchants, base, country=p["home"], channel="pos")
        t2 = make_txn(rng, p, card, merchants, base + timedelta(minutes=int(rng.integers(5, 25))),
                      country=foreign, channel="pos", amount=float(rng.uniform(200, 1200)))
        for t in (t1, t2):
            t["customer_id"] = cid
            _record(t, True, scenario, rng, txns_by_day, labels_by_day)
            count += 1
    return count


def inject_dirty(rng, txns_by_day: dict, dirty_rate: float) -> dict:
    """
    Inyecta filas defectuosas para ejercitar la cuarentena de Silver.

    Cada fila inyectada se ANEXA (no corrompe una existente) y lleva
    EXACTAMENTE UN defecto, de modo que:
        filas inyectadas por defecto == filas en quarantine por reject_reason

    Las filas defectuosas NO reciben etiqueta de fraude: son dato roto que
    sera rechazado antes de llegar al join de labels, y ademas mantener el
    feed de etiquetas con llaves unicas evita romper su MERGE.

    Devuelve el manifiesto {defect_name: conteo} para conciliar despues.
    """
    if dirty_rate <= 0:
        return {}

    total = sum(len(v) for v in txns_by_day.values())
    n_dirty = int(round(total * dirty_rate))
    # garantiza al menos una de cada defecto: una regla que nunca se ejercita
    # no esta probada
    n_dirty = max(n_dirty, len(DEFECTS))

    days = sorted(txns_by_day.keys())
    manifest: dict[str, int] = {}

    for i in range(n_dirty):
        defect, _reason = DEFECTS[i % len(DEFECTS)]
        # toma una transaccion limpia al azar como base
        day = str(rng.choice(days))
        base = copy.deepcopy(txns_by_day[day][int(rng.integers(len(txns_by_day[day])))])

        if defect == "duplicate":
            # copia con ingestion mas ANTIGUA: dedup conserva la ultima version,
            # asi que esta copia es la que debe caer en quarantine.
            # el transaction_id se mantiene igual: en eso consiste el duplicado.
            older = datetime.strptime(base["ingestion_timestamp"], "%Y-%m-%dT%H:%M:%SZ") \
                        .replace(tzinfo=timezone.utc) - timedelta(hours=int(rng.integers(1, 48)))
            base["ingestion_timestamp"] = iso(older)
        else:
            # los demas defectos necesitan llave propia para no colisionar
            base["transaction_id"] = uid(rng, "txn", 32)

            if defect == "missing_key":
                base["transaction_id"] = None
            elif defect == "null_event_ts":
                base["event_timestamp"] = None
            elif defect == "future_event_ts":
                base["event_timestamp"] = iso(datetime.now(timezone.utc)
                                              + timedelta(days=int(rng.integers(2, 90))))
            elif defect == "null_amount":
                base["amount"] = None
            elif defect == "negative_amount":
                base["amount"] = -round(float(rng.uniform(5, 500)), 2)
            elif defect == "null_customer":
                base["customer_id"] = None
            elif defect == "null_merchant":
                base["merchant_id"] = None
            elif defect == "orphan_customer":
                base["customer_id"] = uid(rng, "cust", 12)   # no existe en la dim
            elif defect == "orphan_merchant":
                base["merchant_id"] = uid(rng, "merch", 10)  # no existe en la dim

        # se aterriza en la carpeta dt= de su transaccion base: el particionado
        # del raw es solo layout de archivos; Bronze deriva event_date de la
        # columna, asi que una fila con event_timestamp nulo cae con fecha nula
        txns_by_day[day].append(base)
        manifest[defect] = manifest.get(defect, 0) + 1

    return manifest


def generate(out: str, n_customers: int, n_merchants: int, days: int, seed: int,
             dirty_rate: float):
    rng = np.random.default_rng(seed)
    fake = Faker()
    Faker.seed(seed)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    customers, cards, merchants, profiles = build_dimensions(
        fake, rng, n_customers, n_merchants, start)

    write_jsonl(os.path.join(out, "raw/dimensions/customers.jsonl"), customers)
    write_jsonl(os.path.join(out, "raw/dimensions/cards.jsonl"), cards)
    write_jsonl(os.path.join(out, "raw/dimensions/merchants.jsonl"), merchants)

    txns_by_day: dict[str, list[dict]] = {}
    labels_by_day: dict[str, list[dict]] = {}
    n_fraud = 0

    for cust in customers:
        cid = cust["customer_id"]
        p = profiles[cid]
        daily = {"standard": 0.8, "gold": 1.6, "platinum": 2.8}[cust["account_tier"]]
        for d in range(days):
            day = start + timedelta(days=d)
            n = rng.poisson(daily)
            for _ in range(n):
                hour = int(rng.choice(p["active_hours"]))
                ts = day + timedelta(hours=hour, minutes=int(rng.integers(0, 60)))
                card = str(rng.choice(p["cards"]))
                txn = make_txn(rng, p, card, merchants, ts)
                txn["customer_id"] = cid
                _record(txn, False, None, rng, txns_by_day, labels_by_day)

        if rng.random() < 0.05:
            n_fraud += _inject_fraud(rng, cust, p, merchants, start, days,
                                     txns_by_day, labels_by_day)

    n_clean = sum(len(v) for v in txns_by_day.values())

    # los defectos se inyectan al final, sobre el dataset limpio ya formado
    manifest = inject_dirty(rng, txns_by_day, dirty_rate)

    for dt, rows in txns_by_day.items():
        write_jsonl(os.path.join(out, f"raw/transactions/dt={dt}/transactions.jsonl"), rows)
    for dt, rows in labels_by_day.items():
        write_jsonl(os.path.join(out, f"raw/fraud_labels/dt={dt}/labels.jsonl"), rows)

    total_txn = sum(len(v) for v in txns_by_day.values())
    n_dirty = sum(manifest.values())

    print(f"clientes={len(customers)} tarjetas={len(cards)} comercios={len(merchants)}")
    print(f"transacciones={total_txn} (limpias={n_clean} defectuosas={n_dirty})")
    print(f"fraude={n_fraud} ({100 * n_fraud / max(n_clean, 1):.2f}% de las limpias)")

    if manifest:
        print("\n--- defectos inyectados (esperado en silver.quarantine) ---")
        for name, reason in DEFECTS:
            if name in manifest:
                print(f"  {reason:<32} {manifest[name]:>4}")
        print(f"  {'TOTAL':<32} {n_dirty:>4}")

    print(f"\nraw escrito en: {os.path.join(out, 'raw')}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=2000)
    ap.add_argument("--merchants", type=int, default=300)
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--out", type=str, default="./output")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--dirty-rate", type=float, default=0.0,
                    help="fraccion de filas defectuosas a inyectar (ej. 0.001 = 0.1%%). "
                         "0 = dataset limpio. Ejercita la cuarentena de Silver.")
    a = ap.parse_args()
    generate(a.out, a.customers, a.merchants, a.days, a.seed, a.dirty_rate)


# ==============================================================================
# NOTA DE DISENO — device_id no puede delatar al fraude
# ==============================================================================
# Primera version: las transacciones normales dejaban device_id en NULL y solo
# el fraude estrenaba dispositivo. Resultado: "device_id IS NOT NULL" predecia
# fraude con precision casi perfecta. Un modelo lo habria encontrado en
# segundos y habria aprendido un ARTEFACTO DEL GENERADOR, no una señal real.
# Es el mismo pecado que evitan las etiquetas retrasadas, colado por otra
# puerta: una columna que solo existe cuando la respuesta es "si".
#
# Ahora TODA transaccion online/pos lleva dispositivo: los clientes usan sus
# 1-3 habituales; el fraude usa uno nuevo, fuera de ese set. La señal deja de
# ser "¿hay dispositivo?" (delator) y pasa a ser "¿es un dispositivo que este
# cliente ya habia usado antes?" — una feature legitima, que la capa de
# features calcula mirando solo el pasado del cliente.
#
# Regla general: si una columna existe SOLO en las filas positivas, no es una
# feature, es la etiqueta disfrazada.
# ==============================================================================