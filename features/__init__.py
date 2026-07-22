"""
features/__init__.py — EL CONTRATO entre plataforma y ciencia de datos.

Este archivo es territorio de PLATAFORMA. Los de al lado (shared.py, fraud.py)
son territorio de DS: ahi se escribe cualquier expresion de Spark, sin pedir
permiso ni ampliar un vocabulario.

--------------------------------------------------------------------------
QUE PROVEE LA PLATAFORMA
--------------------------------------------------------------------------
  f.window     WindowSpec ya particionado por la entidad, ordenado por
               event_timestamp y acotado para EXCLUIR la fila actual.
  f.history    Igual, pero sobre todo el pasado del cliente (sin limite).
  f.ordered    Particionado y ordenado, SIN frame. Para lag()/lead(), que
               definen su propio desplazamiento. Ojo: con lead() se ve el
               futuro. La plataforma no lo impide — es decision informada
               de DS, igual que elegir cualquier otro frame.
  f.col(name)  Acceso a columnas de silver.transactions.

  windows=[..]  Una definicion, N features. La misma formula sobre varias
                ventanas se registra como txn_count_5min, txn_count_1h, ...
                Agregar una ventana es meter un numero en la lista: no se
                toca la logica y no se rompe a ningun consumidor. Las
                definiciones son EFECTIVAMENTE APPEND-ONLY: cambiar una que
                ya tiene consumidores les rompe el modelo sin avisarles.

--------------------------------------------------------------------------
QUE GARANTIZA LA PLATAFORMA
--------------------------------------------------------------------------
  - El frame de la ventana: mira hacia atras y excluye la fila actual, para
    que el valor offline coincida con el que se habria calculado online en
    ese instante. (Consistencia train/serve.)
  - Reproducibilidad: mismo input, mismo output.
  - Materializacion, versionado y linaje de cada feature.
  - El apareo contra etiquetas MADURAS al armar el set de entrenamiento.

--------------------------------------------------------------------------
QUE **NO** HACE LA PLATAFORMA
--------------------------------------------------------------------------
  - No opina sobre la formula. Si DS quiere una EWMA, una haversine o una
    entropia, la escribe. No hay lista de tipos permitidos.
  - No valida decisiones de modelado. Si DS decide ignorar f.window y usar
    otro frame, es su decision informada; la plataforma la ejecuta.
  - No hace preprocesamiento del modelo. Escalado, one-hot e imputacion
    viven CON el modelo, no aqui: no son hechos sobre la entidad, son
    requisitos de un algoritmo. Meterlos aqui contamina la feature para el
    siguiente consumidor y es una via clasica de leakage.

--------------------------------------------------------------------------
LOS DOS TIERS
--------------------------------------------------------------------------
  tier="shared"  Hecho sobre la entidad, sin dueño. Vive en
                 gold.customer_features. Cualquier modelo lo puede usar.
  tier="model"   Especifica de un modelo. Vive en gold.<owner>_features.

  El modelo SIEMPRE consume su tabla de tier "model", nunca la compartida
  directo. Esa indireccion es a proposito: si mañana una feature "model" se
  vuelve compartida, se mueve su definicion a tier "shared" y la tabla del
  modelo la sigue exponiendo via el join. El consumidor no se entera.
  Por eso el tier es METADATA, no una jerarquia de carpetas que obligue a
  mudar la feature (y romper a sus consumidores) cuando cambia de estatus.

  Nota de realidad: la mayoria de las features tienen UN consumidor y asi se
  quedan. Eso no es deuda tecnica, es la naturaleza del trabajo. Por eso el
  tier no se decreta por adelantado: se observa cual se compartio de verdad.

  El registro (gold.feature_registry) guarda lo DECLARADO aqui — nombre,
  tier, dueño, ventana — con historial por corrida. NO guarda consumo: el
  `owner` dice quien la definio, no quien la lee. El linaje de consumo sale
  del entrenamiento, que loguea las features que uso cada modelo.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import Column, Window
from pyspark.sql import functions as F

# columna interna: event_timestamp en segundos, para poder acotar ventanas
# por rango de tiempo (rangeBetween necesita un valor numerico)
_ORDER_COL = "_event_unix"


def _window_label(seconds: int) -> str:
    """300 -> '5min', 3600 -> '1h', 86400 -> '24h'"""
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}min"
    return f"{seconds}s"


@dataclass
class FeatureDef:
    name: str
    tier: str            # "shared" | "model"
    owner: str | None    # que modelo la creo (None si shared)
    entity: str          # columna llave: customer_id, merchant_id, ...
    window_seconds: int | None
    description: str
    fn: Callable


REGISTRY: list[FeatureDef] = []


class Ctx:
    """Lo que recibe la funcion de feature. Provee ventanas, no reglas."""

    def __init__(self, entity: str, window_seconds: int | None):
        base = Window.partitionBy(entity).orderBy(F.col(_ORDER_COL))

        if window_seconds is not None:
            # [t - ventana, t - 1s]: estrictamente ANTES de la fila actual.
            # Los timestamps son de granularidad de segundo, asi que -1 excluye
            # exactamente la fila presente.
            self.window = base.rangeBetween(-window_seconds, -1)
        else:
            self.window = base.rowsBetween(Window.unboundedPreceding, -1)

        # todo el pasado de la entidad, siempre disponible
        self.history = base.rowsBetween(Window.unboundedPreceding, -1)
        # sin frame: lag()/lead() traen el suyo
        self.ordered = base
        self.entity = entity
        self.window_seconds = window_seconds

    @staticmethod
    def col(name: str) -> Column:
        return F.col(name)


def feature(*, tier: str = "model", owner: str | None = None,
            entity: str = "customer_id", window: int | None = None,
            windows: list[int] | None = None,
            name: str | None = None, description: str = ""):
    """
    Registra una feature.

    Args:
      tier:    "shared" (hecho de entidad, reutilizable) | "model" (de un modelo)
      owner:   que modelo la creo. Obligatorio si tier="model".
      entity:  columna de particionado de la ventana (la llave de la entidad)
      window:  segundos hacia atras. None = todo el historial previo.
               El nombre queda tal cual.
      windows: lista de ventanas. Registra UNA feature por ventana, con el
               sufijo correspondiente. Excluyente con `window`.
      name:    por defecto, el nombre de la funcion.

    La funcion recibe un Ctx y devuelve una Column de Spark. Cualquier
    expresion es valida.

        # una ventana, nombre explicito
        @feature(tier="shared", entity="customer_id", window=300)
        def txn_count_5min(f):
            return F.count("*").over(f.window)

        # varias ventanas, una definicion
        # -> txn_count_5min, txn_count_1h, txn_count_24h
        @feature(tier="shared", entity="customer_id",
                 windows=[300, 3600, 86400])
        def txn_count(f):
            return F.count("*").over(f.window)
    """
    if window is not None and windows is not None:
        raise ValueError("usa `window` o `windows`, no ambos")

    def deco(fn: Callable) -> Callable:
        if tier == "model" and not owner:
            raise ValueError(f"{fn.__name__}: tier='model' requiere owner")
        if tier not in ("shared", "model"):
            raise ValueError(f"{fn.__name__}: tier debe ser 'shared' o 'model'")

        base_name = name or fn.__name__
        desc = description or (fn.__doc__ or "").strip().split("\n")[0]

        # `windows` genera una feature por ventana; `window` (o None) una sola
        specs = ([(f"{base_name}_{_window_label(w)}", w) for w in windows]
                 if windows else [(base_name, window)])

        for fname, secs in specs:
            REGISTRY.append(FeatureDef(
                name=fname, tier=tier, owner=owner, entity=entity,
                window_seconds=secs, description=desc, fn=fn,
            ))
        return fn
    return deco


def registered(tier: str | None = None, owner: str | None = None) -> list[FeatureDef]:
    """Filtra el registro. Lo usa el motor para decidir que materializar."""
    out = REGISTRY
    if tier:
        out = [f for f in out if f.tier == tier]
    if owner:
        out = [f for f in out if f.owner == owner]
    return out