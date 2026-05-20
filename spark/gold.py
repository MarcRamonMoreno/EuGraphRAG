"""Transformaciones silver -> gold: reproyeccion al modelo de grafo (Neo4j).

Gold deja de ser "una fila por documento" y pasa a ser el modelo del grafo:
una tabla plana por tipo de NODO y una tabla plana por tipo de ARISTA. Esa es
la forma que Neo4j carga en bulk con ``UNWIND + MERGE`` (lo veremos en W6).

Alcance de esta version (nucleo):
  - ``documents``  : nodos Document  (proyeccion directa de silver)
  - ``topics``     : nodos Topic     (codigos EuroVoc distintos)
  - ``belongs_to`` : aristas (:Document)-[:BELONGS_TO]->(:Topic)

La pieza nueva es ``F.explode``: convierte el array ``eurovoc_concepts`` en una
fila por par (documento, tema). Es la operacion inversa a un ``groupBy`` que
agrega: aqui DESagregamos un array en filas.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

# Propiedades del nodo Document (orden = EURLEX_GOLD_DOCUMENTS_SCHEMA).
DOCUMENT_NODE_COLUMNS = ["celex_id", "title", "year", "doc_type", "doc_type_label", "text"]


def build_documents(silver: DataFrame) -> DataFrame:
    """Tabla de nodos Document: 1 fila por documento.

    ``dropDuplicates(["celex_id"])`` impone que la clave del nodo sea unica: en
    Neo4j un ``MERGE`` por ``celex_id`` necesita una sola fila por clave o
    crearia/actualizaria de forma ambigua. Lo garantizamos en gold en vez de
    asumir que silver ya viene sin duplicados.

    Args:
        silver: DataFrame de la capa silver.

    Returns:
        DataFrame con las columnas de ``DOCUMENT_NODE_COLUMNS``, clave unica.
    """
    return silver.select(*DOCUMENT_NODE_COLUMNS).dropDuplicates(["celex_id"])


def build_topics(silver: DataFrame) -> DataFrame:
    """Tabla de nodos Topic: codigos EuroVoc distintos.

    ``explode`` despliega el array a una fila por codigo; ``distinct`` colapsa
    los repetidos entre documentos (un mismo tema aparece en muchos docs).
    ``explode`` ya descarta arrays nulos/vacios; el filtro isNotNull cubre el
    caso de elementos nulos dentro del array.

    Args:
        silver: DataFrame de la capa silver (con ``eurovoc_concepts``).

    Returns:
        DataFrame de una columna ``eurovoc_code``, valores unicos.
    """
    return (
        silver.select(F.explode("eurovoc_concepts").alias("eurovoc_code"))
        .where(F.col("eurovoc_code").isNotNull())
        .distinct()
    )


def build_belongs_to(silver: DataFrame) -> DataFrame:
    """Tabla de aristas (:Document)-[:BELONGS_TO]->(:Topic).

    Una fila por par (documento, tema). ``dropDuplicates`` evita aristas
    repetidas si un mismo codigo apareciera dos veces en el array de un doc.

    Args:
        silver: DataFrame de la capa silver.

    Returns:
        DataFrame con columnas ``celex_id`` y ``eurovoc_code``.
    """
    return (
        silver.select("celex_id", F.explode("eurovoc_concepts").alias("eurovoc_code"))
        .where(F.col("eurovoc_code").isNotNull())
        .dropDuplicates(["celex_id", "eurovoc_code"])
    )


def silver_to_gold(
    input_path: Path,
    output_dir: Path,
    spark: SparkSession | None = None,
) -> dict[str, int]:
    """Lee silver, construye las tablas de nodos/aristas y las escribe en gold.

    Cada tabla se escribe con ``coalesce(1)``: son pequenas y Neo4j las carga
    enteras, asi que un solo fichero por tabla evita el small-files problem.
    ``coalesce`` estrecha particiones SIN shuffle (a diferencia de
    ``repartition``, que rebaraja); ideal cuando solo queremos menos ficheros.

    Args:
        input_path: Carpeta Parquet silver de entrada.
        output_dir: Carpeta base gold; se crean subcarpetas documents/topics/
            belongs_to dentro. Se sobrescribe (idempotente).
        spark: SparkSession a reutilizar; si es None se crea una local.

    Returns:
        Dict {nombre_tabla: numero_de_filas} releido desde disco.
    """
    spark = spark or _build_local_spark()

    logger.info("Reading silver from: %s", input_path)
    silver = spark.read.parquet(str(input_path))

    tables: dict[str, DataFrame] = {
        "documents": build_documents(silver),
        "topics": build_topics(silver),
        "belongs_to": build_belongs_to(silver),
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for name, df in tables.items():
        dest = output_dir / name
        df.coalesce(1).write.mode("overwrite").parquet(str(dest))
        counts[name] = spark.read.parquet(str(dest)).count()
        logger.info("gold %s: %d rows -> %s", name, counts[name], dest)

    logger.info("Silver -> gold complete: %s", counts)
    return counts


def _build_local_spark() -> SparkSession:
    """SparkSession local con defaults razonables para la transformacion gold."""
    return (
        SparkSession.builder.appName("EUGraphRAG-Silver-To-Gold")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    """Entry point CLI: ``python -m spark.gold``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/silver/eurlex"),
        help="Carpeta silver de entrada (default: data/silver/eurlex)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/gold/eurlex"),
        help="Carpeta base gold de salida (default: data/gold/eurlex)",
    )
    args = parser.parse_args()

    counts = silver_to_gold(args.input, args.output)
    print(f"Wrote gold tables to {args.output}: {counts}")


if __name__ == "__main__":
    main()
