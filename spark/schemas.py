"""Schemas explicitos (StructType) para cada capa del medallion.

Single source of truth: cualquier modulo que escriba o lea Parquet en esta
pipeline debe importar el schema desde aqui en lugar de redefinirlo. Esto
permite que un cambio de contrato sea grep-eable y testeable en un solo sitio.

Nota sobre Parquet y nullability: Spark NO preserva la restriccion
``nullable=False`` al hacer round-trip por Parquet. Cuando un schema lo marca
asi, la garantia se aplica solo en escritura (Spark rechazaria un NULL ahi).
Al leer, Spark asume ``nullable=True`` por defecto. La validacion estricta de
NULLs en el lado de lectura es responsabilidad de ``quality_checks.py``.
"""
from __future__ import annotations

from pyspark.sql.types import ArrayType, StringType, StructField, StructType

EURLEX_BRONZE_SCHEMA: StructType = StructType(
    [
        StructField("celex_id", StringType(), nullable=False),
        StructField("title", StringType(), nullable=True),
        StructField("text", StringType(), nullable=False),
        StructField("eurovoc_concepts", ArrayType(StringType()), nullable=True),
    ]
)
"""Schema bronze para NLP-AUEB/eurlex.

Fiel al contrato de HuggingFace (verificado en notebooks/03_pyspark_pipeline):
- celex_id: ID oficial EUR-Lex (ej. "31979D0509"). Obligatorio.
- title: titulo legible. A veces faltan en docs antiguos -> nullable.
- text: cuerpo completo (header + recitals + articulado). Obligatorio.
- eurovoc_concepts: lista de codigos EuroVoc como strings (NO ints) tal y
  como los publica la EU; ej. ["192", "2356", "2560"].
"""
