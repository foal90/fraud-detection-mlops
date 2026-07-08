"""
Generador de datos sintéticos para detección de fraude transaccional.

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

Uso:
  python generate.py --customers 2000 --days 60 --out ./output --seed 42
"""

from __future__ import annotations
import argparse
import json
import os
import uuid
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
        cid = f"cust_{uuid.uuid4().hex[:12]}"
        home = rng.choice(COUNTRIES, p=[0.55, 0.18, 0.07, 0.07, 0.05, 0.04, 0.04])
        signup = start - timedelta(days=int(rng.integers(30, 1500)))
        customers.append({
            "customer_id": cid,
            "signup_date": signup.strftime("%Y-%m-%d"),
            "home_country": home,
            "age_band": rng.choice(AGE_BANDS),
            "account_tier": rng.choice(TIERS, p=[0.7, 0.22, 0.08]),
        })
        # perfil interno: define el gasto "normal" del cliente (no se expone en la dim)
        profiles[cid] = {
            "home": home,
            "amt_mean": float(rng.uniform(15, 220)),
            "amt_std": float(rng.uniform(5, 60)),
            "n_cards": int(rng.integers(1, 4)),
            "active_hours": sorted(rng.choice(range(7, 23), size=6, replace=False).tolist()),
        }
        for _ in range(profiles[cid]["n_cards"]):
            card_id = f"card_{uuid.uuid4().hex[:12]}"
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
        mid = f"merch_{uuid.uuid4().hex[:10]}"
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
    return {
        "transaction_id": f"txn_{uuid.uuid4().hex}",
        "event_timestamp": iso(ts),
        "customer_id": None,  # se llena afuera
        "card_id": card_id,
        "merchant_id": m["merchant_id"],
        "amount": round(amount, 2),
        "currency": "MXN" if (country or profile["home"]) == "MX" else "USD",
        "channel": channel or str(rng.choice(CHANNELS, p=[0.55, 0.35, 0.10])),
        "country": country or profile["home"],
        "device_id": device_id,
        # la ingesta ocurre poco despues del evento (lag de pipeline)
        "ingestion_timestamp": iso(ts + timedelta(seconds=int(rng.integers(2, 90)))),
    }


def label_delay(rng, is_fraud: bool) -> timedelta:
    # los chargebacks de fraude tardan mas; las confirmaciones legitimas, menos
    days = rng.integers(7, 35) if is_fraud else rng.integers(1, 10)
    return timedelta(days=int(days), hours=int(rng.integers(0, 24)))


def generate(out: str, n_customers: int, n_merchants: int, days: int, seed: int):
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
        # volumen de transaccion por cliente segun tier
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

        # inyeccion de patrones de fraude (~0.6% de clientes por dia de actividad)
        if rng.random() < 0.05:
            n_fraud += _inject_fraud(rng, cust, p, merchants, start, days,
                                     txns_by_day, labels_by_day)

    for dt, rows in txns_by_day.items():
        write_jsonl(os.path.join(out, f"raw/transactions/dt={dt}/transactions.jsonl"), rows)
    for dt, rows in labels_by_day.items():
        write_jsonl(os.path.join(out, f"raw/fraud_labels/dt={dt}/labels.jsonl"), rows)

    total_txn = sum(len(v) for v in txns_by_day.values())
    print(f"clientes={len(customers)} tarjetas={len(cards)} comercios={len(merchants)}")
    print(f"transacciones={total_txn} fraude={n_fraud} ({100*n_fraud/max(total_txn,1):.2f}%)")
    print(f"raw escrito en: {os.path.join(out, 'raw')}")


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
    # la etiqueta se particiona por el dia en que SE CONOCE, no el del evento
    labels_by_day.setdefault(label_ts.strftime("%Y-%m-%d"), []).append(label)


def _inject_fraud(rng, cust, p, merchants, start, days, txns_by_day, labels_by_day) -> int:
    cid = cust["customer_id"]
    card = str(rng.choice(p["cards"]))
    day = start + timedelta(days=int(rng.integers(0, days)))
    base = day + timedelta(hours=int(rng.integers(0, 24)), minutes=int(rng.integers(0, 60)))
    scenario = rng.choice(["card_testing", "account_takeover", "geo_impossible"])
    count = 0

    if scenario == "card_testing":
        # rafaga de montos chicos en minutos (velocity)
        for i in range(int(rng.integers(6, 15))):
            ts = base + timedelta(minutes=i * int(rng.integers(1, 3)))
            t = make_txn(rng, p, card, merchants, ts, amount=float(rng.uniform(1, 9)),
                         channel="online", device_id=f"dev_{uuid.uuid4().hex[:8]}")
            t["customer_id"] = cid
            _record(t, True, scenario, rng, txns_by_day, labels_by_day)
            count += 1

    elif scenario == "account_takeover":
        # monto alto + dispositivo nuevo + pais extranjero
        foreign = str(rng.choice([c for c in COUNTRIES if c != p["home"]]))
        t = make_txn(rng, p, card, merchants, base,
                     amount=float(rng.uniform(800, 5000)), channel="online",
                     country=foreign, device_id=f"dev_{uuid.uuid4().hex[:8]}")
        t["customer_id"] = cid
        _record(t, True, scenario, rng, txns_by_day, labels_by_day)
        count += 1

    else:  # geo_impossible: dos compras lejanas en minutos
        foreign = str(rng.choice([c for c in COUNTRIES if c != p["home"]]))
        t1 = make_txn(rng, p, card, merchants, base, country=p["home"], channel="pos")
        t2 = make_txn(rng, p, card, merchants, base + timedelta(minutes=int(rng.integers(5, 25))),
                      country=foreign, channel="pos", amount=float(rng.uniform(200, 1200)))
        for t in (t1, t2):
            t["customer_id"] = cid
            _record(t, True, scenario, rng, txns_by_day, labels_by_day)
            count += 1
    return count


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--customers", type=int, default=2000)
    ap.add_argument("--merchants", type=int, default=300)
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--out", type=str, default="./output")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    generate(a.out, a.customers, a.merchants, a.days, a.seed)
