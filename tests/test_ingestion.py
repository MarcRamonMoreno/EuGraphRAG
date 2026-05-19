"""Tests para spark.ingestion y spark.schemas.

Estrategia:
- Tests unitarios (rapidos, sin red): validan que el schema bronze tiene la
  forma esperada y que un DataFrame con el schema se puede escribir/leer
  por Parquet sin perder forma.
- Test de integracion (descarga HF): marcado con ``@pytest.mark.integration``
  para no ejecutarlo en CI por defecto. Usa subset_size=5 para que sea
  rapido tras la primera descarga (cache HF).
"""
from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from spark.ingestion import ingest_eurlex
from spark.schemas import EURLEX_BRONZE_SCHEMA


def test_eurlex_bronze_schema_has_four_fields() -> None:
    """El schema bronze debe tener exactamente 4 campos en orden conocido."""
    field_names = [f.name for f in EURLEX_BRONZE_SCHEMA.fields]
    assert field_names == ["celex_id", "title", "text", "eurovoc_concepts"]


def test_eurlex_bronze_schema_eurovoc_is_array_of_strings() -> None:
    """eurovoc_concepts debe ser Array<String>, NO Array<Long>.

    Los codigos EuroVoc son identificadores opacos publicados por la EU como
    strings. Si alguien lo cambia a LongType silenciosamente, este test salta.
    """
    eurovoc_field = EURLEX_BRONZE_SCHEMA["eurovoc_concepts"]
    assert isinstance(eurovoc_field.dataType, ArrayType)
    assert isinstance(eurovoc_field.dataType.elementType, StringType)


def test_eurlex_bronze_schema_required_fields() -> None:
    """celex_id y text deben ser obligatorios; title y eurovoc opcionales."""
    fields = {f.name: f for f in EURLEX_BRONZE_SCHEMA.fields}
    assert fields["celex_id"].nullable is False
    assert fields["text"].nullable is False
    assert fields["title"].nullable is True
    assert fields["eurovoc_concepts"].nullable is True


def test_bronze_schema_roundtrip_parquet(spark: SparkSession, tmp_path: Path) -> None:
    """Un DataFrame con el schema bronze sobrevive round-trip por Parquet.

    Validamos los nombres y los tipos. NO validamos el flag nullable porque
    Parquet no lo preserva (ver docstring de spark/schemas.py).
    """
    sample = [
        ("31979D0509", "Council Decision 1979", "Body text 1", ["192", "2356"]),
        ("31998R0001", "Regulation 1/98", "Body text 2", ["3401"]),
    ]
    df = spark.createDataFrame(sample, schema=EURLEX_BRONZE_SCHEMA)
    out = tmp_path / "eurlex"
    df.write.mode("overwrite").parquet(str(out))

    df_back = spark.read.parquet(str(out))
    assert df_back.count() == 2
    assert [f.name for f in df_back.schema.fields] == [
        f.name for f in EURLEX_BRONZE_SCHEMA.fields
    ]
    assert isinstance(df_back.schema["eurovoc_concepts"].dataType, ArrayType)


@pytest.mark.integration
def test_ingest_eurlex_small_smoke(spark: SparkSession, tmp_path: Path) -> None:
    """Smoke test: descarga 5 filas reales, escribe Parquet, valida.

    Marcado integration -> ``pytest -m integration`` para ejecutarlo.
    Requiere red la primera vez; despues usa el cache HF (~/.cache/huggingface).
    """
    out = tmp_path / "bronze_eurlex"
    n = ingest_eurlex(out, split="test", subset_size=5, spark=spark)
    assert n == 5

    df = spark.read.parquet(str(out))
    assert df.count() == 5
    # Una sanity check minima: celex_id debe parecer un ID EUR-Lex (no vacio).
    ids = [row.celex_id for row in df.select("celex_id").collect()]
    assert all(isinstance(i, str) and len(i) >= 8 for i in ids)
