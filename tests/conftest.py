"""Pytest fixtures compartidos.

La fixture ``spark`` es session-scoped: una unica SparkSession reutilizada
por todos los tests del run. Arrancar Spark es lento (~5-10s), asi que
crear una sesion por test seria inaceptable. Session scope es seguro porque
Spark es thread-safe a nivel de DataFrames; cada test usa DataFrames
independientes.
"""
from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """SparkSession local para tests, con configuracion ligera."""
    session = (
        SparkSession.builder.appName("EUGraphRAG-Tests")
        .master("local[2]")  # 2 cores son suficientes para tests pequenos
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")  # no necesitamos la UI en tests
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
