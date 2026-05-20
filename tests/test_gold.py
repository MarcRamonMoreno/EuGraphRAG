"""Tests para spark.gold (silver -> gold: tablas de nodos y aristas).

Foco en la correccion de ``F.explode`` (el patron nuevo): un documento con N
temas debe producir N aristas, los documentos sin temas no producen aristas
pero SI siguen siendo nodos Document, y los temas se deduplican entre docs.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from spark.gold import build_belongs_to, build_documents, build_topics, silver_to_gold
from spark.schemas import (
    EURLEX_GOLD_BELONGS_TO_SCHEMA,
    EURLEX_GOLD_DOCUMENTS_SCHEMA,
    EURLEX_GOLD_TOPICS_SCHEMA,
    EURLEX_SILVER_SCHEMA,
)

# Silver de prueba: doc1 (2 temas), doc2 (3 temas, solapa con doc1),
# doc3 (array vacio), doc4 (array null). Asi probamos explode + dedup + huerfanos.
_SILVER_ROWS = [
    ("32014R0727", "3", 2014, "R", "Regulation", "t1", "x" * 60, ["192", "2356"]),
    ("32010D0008", "3", 2010, "D", "Decision", "t2", "y" * 60, ["2356", "3401", "192"]),
    ("31975R2481", "3", 1975, "R", "Regulation", "t3", "z" * 60, []),
    ("31980D0126", "3", 1980, "D", "Decision", "t4", "w" * 60, None),
]


def _silver_df(spark: SparkSession):
    return spark.createDataFrame(_SILVER_ROWS, schema=EURLEX_SILVER_SCHEMA)


def test_documents_one_row_per_doc(spark: SparkSession) -> None:
    """documents tiene 1 fila por documento, incluso los que no tienen temas."""
    docs = build_documents(_silver_df(spark))
    assert docs.count() == 4  # los 4 docs, incluidos el de array vacio y el null
    assert [f.name for f in docs.schema.fields] == [
        f.name for f in EURLEX_GOLD_DOCUMENTS_SCHEMA.fields
    ]


def test_documents_celex_is_unique(spark: SparkSession) -> None:
    """La clave de nodo celex_id no se repite (requisito de MERGE en Neo4j)."""
    docs = build_documents(_silver_df(spark))
    assert docs.select("celex_id").distinct().count() == docs.count()


def test_topics_are_distinct_codes(spark: SparkSession) -> None:
    """topics = codigos EuroVoc unicos a traves de todos los documentos."""
    topics = build_topics(_silver_df(spark))
    codes = {r.eurovoc_code for r in topics.collect()}
    assert codes == {"192", "2356", "3401"}  # 2356 y 192 solapaban -> deduplicados
    assert [f.name for f in topics.schema.fields] == [
        f.name for f in EURLEX_GOLD_TOPICS_SCHEMA.fields
    ]


def test_belongs_to_explodes_array(spark: SparkSession) -> None:
    """belongs_to: 1 arista por par (doc, tema); arrays vacios/null -> 0 aristas."""
    edges = build_belongs_to(_silver_df(spark))
    assert edges.count() == 5  # doc1: 2 + doc2: 3 + doc3: 0 + doc4: 0
    doc1 = {r.eurovoc_code for r in edges.filter("celex_id = '32014R0727'").collect()}
    assert doc1 == {"192", "2356"}
    doc3 = edges.filter("celex_id = '31975R2481'").count()
    assert doc3 == 0  # array vacio no genera aristas
    assert [f.name for f in edges.schema.fields] == [
        f.name for f in EURLEX_GOLD_BELONGS_TO_SCHEMA.fields
    ]


def test_silver_to_gold_writes_three_tables(spark: SparkSession, tmp_path: Path) -> None:
    """silver_to_gold escribe documents/topics/belongs_to con los conteos correctos."""
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    _silver_df(spark).write.mode("overwrite").parquet(str(silver_dir))

    counts = silver_to_gold(silver_dir, gold_dir, spark=spark)
    assert counts == {"documents": 4, "topics": 3, "belongs_to": 5}

    # Las tres subcarpetas existen y se releen.
    for name in ("documents", "topics", "belongs_to"):
        assert (gold_dir / name).exists()
        assert spark.read.parquet(str(gold_dir / name)).count() == counts[name]
