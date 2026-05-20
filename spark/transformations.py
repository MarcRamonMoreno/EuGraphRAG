"""Transformaciones bronze -> silver para el corpus EUR-Lex.

Politica de silver: limpiar y estructurar SIN destruir informacion que las
capas superiores necesiten. Concretamente:
  - Derivamos campos a partir del celex_id (sector, ano, tipo de documento).
  - Limpiamos el texto quitando la cabecera de publicacion del Diario Oficial,
    pero PRESERVAMOS la estructura de parrafos (no aplastamos a una sola linea)
    porque el chunking de RAG en la fase 3 la va a necesitar.
  - Particionamos por (year, doc_type) en estilo Hive para pruning de queries.

Diseno funcional: cada paso es una funcion ``DataFrame -> DataFrame`` pura, asi
se encadenan con ``df.transform(paso)`` y se testean por separado sin tocar
disco. Es el mismo patron de composicion (pipe) que veras en LangChain.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import IntegerType

from spark.schemas import CELEX_DOC_TYPE_LABELS, DOC_TYPE_LABEL_FALLBACK

logger = logging.getLogger(__name__)

# Cabecera de publicacion del Diario Oficial al inicio del cuerpo. Ejemplo:
#   "1.7.2014 EN Official Journal of the European Union L 192/42\n"
# Es metadato de publicacion, no contenido legal -> lo quitamos en silver.
# Anclado con ^ (Java regex: ^ = inicio de toda la cadena, no por linea), asi
# solo elimina la cabecera del principio y nunca toca el cuerpo.
_OJ_HEADER_RE = (
    r"^\s*\d{1,2}\.\d{1,2}\.\d{4}\s+[A-Z]{2}\s+"
    r"Official Journal of the European Union\s+[LC]\s+\d+/\d+\s*"
)


def with_celex_components(df: DataFrame) -> DataFrame:
    """Descompone ``celex_id`` en sector, year (int) y doc_type (la letra).

    Usa ``regexp_extract`` con patrones anclados al inicio, de modo que sufijos
    como ``(01)`` (corrigenda) no afectan al parseo. Si el id estuviera
    malformado, ``regexp_extract`` devuelve "" y el cast de year produce NULL;
    eso lo detecta luego ``quality_checks``, no lo enmascaramos aqui.

    Args:
        df: DataFrame bronze; debe tener la columna ``celex_id``.

    Returns:
        El mismo DataFrame con ``sector``, ``year`` y ``doc_type`` anadidas.
    """
    return (
        df
        # sector = primer digito
        .withColumn("sector", F.regexp_extract("celex_id", r"^(\d)", 1))
        # year = los 4 digitos que siguen al sector ; casteado a entero
        .withColumn(
            "year",
            F.regexp_extract("celex_id", r"^\d(\d{4})", 1).cast(IntegerType()),
        )
        # doc_type = la primera letra tras el bloque de ano
        .withColumn("doc_type", F.regexp_extract("celex_id", r"^\d\d{4}([A-Z])", 1))
    )


def with_doc_type_label(df: DataFrame) -> DataFrame:
    """Traduce la letra ``doc_type`` a una etiqueta legible (``doc_type_label``).

    Construimos un map de Spark a partir del dict de Python de forma data-driven
    en vez de una cadena de ``F.when``. Ventaja: anadir un tipo nuevo es editar
    el dict en schemas.py, no tocar este codigo. ``coalesce`` da el fallback
    "Other" para letras no mapeadas (p.ej. tipos raros de sector 3).

    Args:
        df: DataFrame con la columna ``doc_type``.

    Returns:
        El mismo DataFrame con la columna ``doc_type_label``.
    """
    # F.create_map espera [k1, v1, k2, v2, ...] como Columns literales.
    mapping_pairs: list[Column] = [F.lit(x) for kv in CELEX_DOC_TYPE_LABELS.items() for x in kv]
    label_map = F.create_map(*mapping_pairs)
    return df.withColumn(
        "doc_type_label",
        F.coalesce(label_map[F.col("doc_type")], F.lit(DOC_TYPE_LABEL_FALLBACK)),
    )


def with_clean_text(df: DataFrame) -> DataFrame:
    """Limpia ``text`` (cabecera + espacios) y recorta ``title``.

    Pasos sobre ``text``:
      1. Quitar la cabecera del Diario Oficial del principio.
      2. Colapsar runs de espacios/tabuladores horizontales a un solo espacio.
      3. Colapsar 3+ saltos de linea seguidos a 2 (preserva separacion de
         parrafos sin dejar huecos enormes).
      4. ``trim`` de los extremos.

    Decision de diseno: NO colapsamos los saltos de linea a espacios. Mantener
    parrafos es clave para que el chunking de RAG (fase 3) corte por unidades
    semanticas y no a ciegas.

    Args:
        df: DataFrame con columnas ``text`` y ``title``.

    Returns:
        El mismo DataFrame con ``text`` limpio y ``title`` recortado.
    """
    # OJO: F.trim solo quita espacios ASCII (0x20), NO \n ni \t. Para recortar
    # cualquier whitespace de los extremos usamos regexp_replace(^\s+|\s+$).
    clean = F.col("text")
    clean = F.regexp_replace(clean, _OJ_HEADER_RE, "")  # 1. cabecera fuera
    clean = F.regexp_replace(clean, r"[ \t]+", " ")  # 2. espacios horizontales
    clean = F.regexp_replace(clean, r"\n{3,}", "\n\n")  # 3. saltos excesivos
    clean = F.regexp_replace(clean, r"^\s+|\s+$", "")  # 4. recorta extremos
    title = F.regexp_replace(F.col("title"), r"^\s+|\s+$", "")
    return df.withColumn("text", clean).withColumn("title", title)


def bronze_to_silver(
    input_path: Path,
    output_path: Path,
    spark: SparkSession | None = None,
) -> int:
    """Lee bronze, aplica las transformaciones y escribe silver particionado.

    El write usa ``repartition("year", "doc_type")`` ANTES de
    ``partitionBy(...)``. Esto provoca un shuffle (Exchange) que coloca todas
    las filas de un mismo (year, doc_type) en la misma tarea, de modo que cada
    carpeta de particion recibe exactamente 1 fichero. Sin ese repartition,
    cada particion de entrada escribiria en cada carpeta de salida -> explosion
    de ficheros pequenos (el "small files problem").

    Aviso de cardinalidad: con solo ~100 filas, particionar por year (~40
    valores) x doc_type (3) sobre-particiona muchisimo (carpetas de 1-3 filas).
    En produccion con 57k+ filas tiene mas sentido; aun asi, year puede ser
    demasiado fino y a veces se agrupa por decada. Lo dejamos por year+doc_type
    porque es lo que el plan pide y demuestra el patron de particionado Hive.

    Args:
        input_path: Carpeta Parquet bronze de entrada.
        output_path: Carpeta destino silver. Se sobrescribe (idempotente).
        spark: SparkSession a reutilizar; si es None se crea una local.

    Returns:
        Numero de filas escritas (releido desde silver como verificacion).
    """
    spark = spark or _build_local_spark()

    logger.info("Reading bronze from: %s", input_path)
    bronze = spark.read.parquet(str(input_path))

    silver = (
        bronze.transform(with_celex_components)
        .transform(with_doc_type_label)
        .transform(with_clean_text)
        .select(
            "celex_id",
            "sector",
            "year",
            "doc_type",
            "doc_type_label",
            "title",
            "text",
            "eurovoc_concepts",
        )
    )

    output_path.mkdir(parents=True, exist_ok=True)
    logger.info("Writing silver (partitioned by year, doc_type) to: %s", output_path)
    (
        silver
        # Shuffle deliberado -> 1 fichero por carpeta de particion (ver docstring).
        .repartition("year", "doc_type")
        .write.mode("overwrite")
        .partitionBy("year", "doc_type")
        .parquet(str(output_path))
    )

    # Releemos para verificar lo que realmente aterrizo en disco (round-trip).
    row_count = spark.read.parquet(str(output_path)).count()
    logger.info("Silver transform complete: %d rows -> %s", row_count, output_path)
    return row_count


def _build_local_spark() -> SparkSession:
    """SparkSession local con defaults razonables para la transformacion silver."""
    return (
        SparkSession.builder.appName("EUGraphRAG-Bronze-To-Silver")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main() -> None:
    """Entry point CLI: ``python -m spark.transformations``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/bronze/eurlex"),
        help="Carpeta bronze de entrada (default: data/bronze/eurlex)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/silver/eurlex"),
        help="Carpeta silver de salida (default: data/silver/eurlex)",
    )
    args = parser.parse_args()

    n = bronze_to_silver(args.input, args.output)
    print(f"Wrote {n} rows to {args.output}")


if __name__ == "__main__":
    main()
