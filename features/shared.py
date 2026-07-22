"""
features/shared.py — TIER 1: features de entidad, compartidas.

Territorio de DS. Hechos sobre el cliente, sin dueño: cualquier modelo los
puede consumir (fraude hoy, riesgo o churn mañana). Se materializan en
gold.customer_features.

El criterio para que algo viva aqui: ¿tiene significado de negocio por si
solo, independiente del modelo que lo use? "El cliente hizo 5 transacciones
en la ultima hora" lo tiene. "El monto escalado a media 0 y varianza 1" no:
eso es un requisito de un algoritmo y vive con el modelo.

Toda ventana la provee la plataforma ya acotada para excluir la fila actual.
"""
from pyspark.sql import functions as F

from . import feature


# ---------------------------------------------------------------------------
# Velocidad transaccional
# ---------------------------------------------------------------------------

# Una definicion, N features: txn_count_5min, _10min, _1h, _24h.
# Agregar una ventana es meter un numero en la lista. Nunca se MODIFICA una
# ventana existente: eso le cambiaria el valor a quien ya la consume.
@feature(tier="shared", entity="customer_id",
         windows=[300, 600, 3600, 86400],
         description="Transacciones del cliente en la ventana")
def txn_count(f):
    return F.count("*").over(f.window)


@feature(tier="shared", entity="customer_id",
         windows=[300, 3600, 86400],
         description="Monto acumulado del cliente en la ventana")
def amount_sum(f):
    return F.coalesce(F.sum("amount").over(f.window), F.lit(0.0))


@feature(tier="shared", entity="customer_id",
         windows=[3600, 86400],
         description="Monto promedio del cliente en la ventana")
def amount_avg(f):
    return F.avg("amount").over(f.window)


# ---------------------------------------------------------------------------
# Patron historico del cliente
# ---------------------------------------------------------------------------

@feature(tier="shared", entity="customer_id",
         description="Monto promedio historico del cliente (antes de esta txn)")
def amount_avg_hist(f):
    return F.avg("amount").over(f.history)


@feature(tier="shared", entity="customer_id",
         description="Desviacion estandar historica del monto del cliente")
def amount_std_hist(f):
    return F.stddev("amount").over(f.history)


@feature(tier="shared", entity="customer_id",
         description="Transacciones historicas del cliente (antiguedad de relacion)")
def txn_count_hist(f):
    return F.count("*").over(f.history)


# ---------------------------------------------------------------------------
# Secuencia temporal
# ---------------------------------------------------------------------------

@feature(tier="shared", entity="customer_id",
         description="Minutos desde la transaccion anterior del cliente")
def minutes_since_prev_txn(f):
    prev = F.lag("_event_unix", 1).over(f.ordered)
    return (F.col("_event_unix") - prev) / 60.0


@feature(tier="shared", entity="customer_id",
         description="Pais de la transaccion anterior del cliente")
def prev_country(f):
    return F.last("country", ignorenulls=True).over(f.history)


# ---------------------------------------------------------------------------
# Dispositivo
# ---------------------------------------------------------------------------

@feature(tier="shared", entity="customer_id",
         description="Dispositivos distintos que el cliente ha usado antes")
def distinct_devices_hist(f):
    return F.size(F.collect_set("device_id").over(f.history))