"""Ingestion bronze: HuggingFace -> data/bronze/.

Politica de bronze: persistimos los datos tal y como vienen del origen. No
limpiamos NULLs, no normalizamos fechas, no extraemos campos derivados.
La unica transformacion es estructural (StructType explicito) para que el
fichero Parquet sea legible por Spark sin tener que volver a inferir tipos.

Idempotente: cada llamada hace overwrite del destino. Si quieres append,
gestiona deduplicacion explicitamente en una capa superior.
"""
from __future__ import annotations

import argparse
import logging
import tempfile
from pathlib import Path

import datasets as hf_ds
from pyspark.sql import SparkSession

from spark.schemas import EURLEX_BRONZE_SCHEMA

logger = logging.getLogger(__name__)

HF_DATASET_ID = "NLP-AUEB/eurlex"


def ingest_eurlex(
    output_path: Path,
    split: str = "train",
    subset_size: int | None = None,
    spark: SparkSession | None = None,
) -> int:
    """Descarga NLP-AUEB/eurlex desde HuggingFace y lo persiste en Parquet.

    El pipeline interno es HF -> Parquet temporal -> Spark -> Parquet final.
    El paso por un Parquet temporal evita materializar todo el dataset en
    memoria del driver (lo que pasaria con ``ds.to_pandas()`` en 57k filas),
    y permite a Spark aplicar el schema explicito durante la lectura.

    Args:
        output_path: Carpeta destino para los ficheros Parquet bronze. Se
            crea si no existe. El contenido previo se sobrescribe.
        split: Nombre del split HF (``train``, ``validation``, ``test``).
        subset_size: Si se especifica, carga solo las primeras N filas del
            split. ``None`` carga todo. Util para dev/test rapidos.
        spark: SparkSession existente para reutilizar. Si es ``None`` se
            crea una sesion local con configuracion por defecto.

    Returns:
        Numero de filas escritas en ``output_path``.
    """
    spark = spark or _build_local_spark()

    split_expr = f"{split}[:{subset_size}]" if subset_size else split
    logger.info("Loading HuggingFace dataset: %s split=%s", HF_DATASET_ID, split_expr)
    ds = hf_ds.load_dataset(HF_DATASET_ID, split=split_expr, trust_remote_code=True)
    logger.info("HF dataset loaded: %d rows, columns=%s", len(ds), ds.column_names)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_parquet = Path(tmpdir) / "eurlex.parquet"
        logger.info("Materializing HF dataset to temp Parquet: %s", tmp_parquet)
        ds.to_parquet(str(tmp_parquet))

        logger.info("Re-reading temp Parquet with Spark + bronze schema")
        df = spark.read.schema(EURLEX_BRONZE_SCHEMA).parquet(str(tmp_parquet))

        output_path.mkdir(parents=True, exist_ok=True)
        logger.info("Writing bronze Parquet to: %s", output_path)
        df.write.mode("overwrite").parquet(str(output_path))

        row_count = df.count()
        logger.info("Bronze ingestion complete: %d rows -> %s", row_count, output_path)
        return row_count


def _build_local_spark() -> SparkSession:
    """SparkSession local con defaults razonables para bronze ingestion."""
    return (
        SparkSession.builder.appName("EUGraphRAG-Bronze-Ingestion")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    """Entry point CLI: ``python -m spark.ingestion --output data/bronze/eurlex``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/bronze/eurlex"),
        help="Carpeta destino bronze (default: data/bronze/eurlex)",
    )
    parser.add_argument(
        "--split",
        default="train",
        choices=["train", "validation", "test"],
        help="Split HF a ingestar (default: train)",
    )
    parser.add_argument(
        "--subset-size",
        type=int,
        default=None,
        help="Limitar a las primeras N filas (default: todo el split)",
    )
    args = parser.parse_args()

    n = ingest_eurlex(args.output, split=args.split, subset_size=args.subset_size)
    print(f"Wrote {n} rows to {args.output}")


if __name__ == "__main__":
    main()
