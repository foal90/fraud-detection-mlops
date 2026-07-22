"""
features/fraud.py — TIER 2: features especificas del modelo de fraude.

Territorio de DS. Se materializan en gold.fraud_features, que es lo que el
modelo consume: esa tabla trae estas features MAS las compartidas via join.

Que algo viva aqui y no en shared.py no es un juicio de valor: es que hoy
tiene un solo consumidor. Si mañana otro modelo la quiere, se mueve la
definicion a shared.py y gold.fraud_features la sigue exponiendo por el join.
El modelo no se entera. Por eso el tier es metadata y no una carpeta que
obligue a romper consumidores.

Estas features apuntan a los tres patrones de fraude conocidos:
  card_testing     -> rafaga de montos chicos en minutos
  account_takeover -> monto alto + dispositivo nuevo + pais extranjero
  geo_impossible   -> dos compras lejanas en minutos

NOTA: los outliers no se limpian, se MIDEN. En fraude el valor extremo es la
señal, no el ruido (ver la nota al pie de config/quality_rules.yaml).
"""
from pyspark.sql import functions as F

from . import feature

OWNER = "fraud"


# ---------------------------------------------------------------------------
# Desviacion del patron  (caza account_takeover)
# ---------------------------------------------------------------------------

@feature(tier="model", owner=OWNER, entity="customer_id",
         description="Que tan lejos esta el monto del patron historico del cliente")
def amount_zscore(f):
    mean = F.avg("amount").over(f.history)
    std = F.stddev("amount").over(f.history)
    # nulo si el cliente no tiene historial suficiente para tener varianza:
    # dividir entre cero daria infinito, y "sin historia" no es "z-score 0"
    return F.when(std > 0, (F.col("amount") - mean) / std)


@feature(tier="model", owner=OWNER, entity="customer_id",
         description="Razon entre el monto actual y el maximo historico del cliente")
def amount_vs_max_hist(f):
    mx = F.max("amount").over(f.history)
    return F.when(mx > 0, F.col("amount") / mx)


# ---------------------------------------------------------------------------
# Velocidad ponderada  (caza card_testing)
# ---------------------------------------------------------------------------

@feature(tier="model", owner=OWNER, entity="customer_id", window=3600,
         description="Monto promedio en la ultima hora vs el promedio historico")
def small_amount_burst_1h(f):
    recent_avg = F.avg("amount").over(f.window)
    hist_avg = F.avg("amount").over(f.history)
    return F.when(hist_avg > 0, recent_avg / hist_avg)


@feature(tier="model", owner=OWNER, entity="customer_id", window=600,
         description="Comercios distintos tocados en los ultimos 10 minutos")
def distinct_merchants_10min(f):
    return F.size(F.collect_set("merchant_id").over(f.window))


# ---------------------------------------------------------------------------
# Geografia y dispositivo  (caza geo_impossible y account_takeover)
# ---------------------------------------------------------------------------

@feature(tier="model", owner=OWNER, entity="customer_id",
         description="El dispositivo nunca habia sido usado por este cliente")
def is_new_device(f):
    # collect_set sobre el historial: si el dispositivo actual no esta, es nuevo.
    # Nulo cuando no hay dispositivo (canal atm) — ausencia no es novedad.
    seen = F.collect_set("device_id").over(f.history)
    return F.when(F.col("device_id").isNull(), None) \
            .otherwise(~F.array_contains(seen, F.col("device_id")))


@feature(tier="model", owner=OWNER, entity="customer_id",
         description="El pais cambio respecto a la transaccion anterior")
def country_changed(f):
    prev = F.last("country", ignorenulls=True).over(f.history)
    return F.when(prev.isNull(), None).otherwise(F.col("country") != prev)


@feature(tier="model", owner=OWNER, entity="customer_id",
         description="La transaccion ocurre fuera del pais de residencia")
def is_foreign(f):
    return F.col("country") != F.col("home_country")


# ---------------------------------------------------------------------------
# Contexto
# ---------------------------------------------------------------------------

@feature(tier="model", owner=OWNER, entity="customer_id",
         description="Hora del dia de la transaccion")
def hour_of_day(f):
    return F.hour("event_timestamp")


@feature(tier="model", owner=OWNER, entity="customer_id",
         description="El comercio es de categoria de alto riesgo")
def merchant_is_high_risk(f):
    return F.col("risk_band") == F.lit("high")


@feature(tier="model", owner=OWNER, entity="customer_id",
         description="Dias desde que el cliente abrio su cuenta")
def account_age_days(f):
    return F.datediff(F.col("event_timestamp"), F.col("signup_date"))