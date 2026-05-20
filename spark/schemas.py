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

from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

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

# ---------------------------------------------------------------------------
# Diccionarios de dominio CELEX
# ---------------------------------------------------------------------------
# Un identificador CELEX de legislacion tiene la forma: S YYYY T NNNN
#   S    -> sector (1 digito): 3 = legislacion derivada (lo que tenemos aqui).
#   YYYY -> ano de adopcion (4 digitos).
#   T    -> tipo de documento (1 letra): R, L, D, ...
#   NNNN -> numero secuencial (puede llevar sufijos como "(01)" en corrigenda).
#
# Estos diccionarios son la fuente de verdad para traducir los codigos opacos
# a etiquetas legibles. Mantenerlos como dict de Python (y no como cadenas de
# F.when en el codigo) permite: (a) testearlos sin Spark, (b) construir el map
# de Spark de forma data-driven, y (c) reutilizarlos luego al crear nodos Neo4j.

CELEX_SECTOR_LABELS: dict[str, str] = {
    "1": "Treaties",
    "2": "International Agreements",
    "3": "Legislation",
    "4": "Complementary Legislation",
    "5": "Preparatory Acts",
    "6": "Case Law",
    "7": "National Transposition",
    "8": "National Case Law",
    "9": "Parliamentary Questions",
}
"""Sectores CELEX. Nuestro corpus NLP-AUEB/eurlex es todo sector 3."""

CELEX_DOC_TYPE_LABELS: dict[str, str] = {
    # Los tres tipos que dominan nuestro corpus (R/D/L) son exactos y verificados.
    "R": "Regulation",
    "L": "Directive",
    "D": "Decision",
    # Resto de descriptores de sector 3 (best-effort segun la documentacion
    # CELEX de EUR-Lex). Cualquier letra no listada cae a "Other" via coalesce.
    "H": "Recommendation",
    "A": "Opinion",
    "G": "Resolution",
    "F": "Framework Decision",
    "J": "Joint Action",
    "M": "Merger Decision",
    "O": "Guideline",
    "Q": "Institutional Arrangement",
    "S": "ECSC General Decision",
    "X": "Other Act",
    "C": "Other (OJ C series)",
    "E": "EEA/EFTA Act",
    "K": "ECSC Recommendation",
}
"""Tipo de documento CELEX (sector 3) -> etiqueta legible.

R/L/D son los criticos y estan verificados contra los datos reales. El resto
es best-effort; la referencia autoritativa es la tabla de descriptores CELEX
de EUR-Lex. Letras desconocidas se etiquetan como "Other" en silver."""

DOC_TYPE_LABEL_FALLBACK = "Other"
"""Etiqueta usada cuando la letra de tipo no esta en CELEX_DOC_TYPE_LABELS."""


# ---------------------------------------------------------------------------
# Schema silver
# ---------------------------------------------------------------------------
EURLEX_SILVER_SCHEMA: StructType = StructType(
    [
        StructField("celex_id", StringType(), nullable=False),
        StructField("sector", StringType(), nullable=False),
        StructField("year", IntegerType(), nullable=False),
        StructField("doc_type", StringType(), nullable=False),
        StructField("doc_type_label", StringType(), nullable=False),
        StructField("title", StringType(), nullable=True),
        StructField("text", StringType(), nullable=False),
        StructField("eurovoc_concepts", ArrayType(StringType()), nullable=True),
    ]
)
"""Schema logico de la capa silver (bronze + columnas derivadas + texto limpio).

Es el orden de columnas que produce ``bronze_to_silver`` ANTES de escribir.
Aviso: al escribir con ``partitionBy("year", "doc_type")``, esas dos columnas
NO viven dentro de los ficheros Parquet sino en la ruta (Hive-style). Spark las
reanade al leer, pero las situa al final y reinfiere su tipo. Por eso los tests
de round-trip comparan por nombre de campo, no por orden posicional.
"""


# ---------------------------------------------------------------------------
# Schemas gold (modelo de grafo: tablas de nodos y de aristas para Neo4j)
# ---------------------------------------------------------------------------
# Gold no es "datos limpios" como silver, sino silver REPROYECTADO a la forma
# que Neo4j carga en bulk (UNWIND + MERGE): una tabla plana por tipo de nodo y
# una tabla plana por tipo de arista. Cada fila = un nodo o una relacion.

EURLEX_GOLD_DOCUMENTS_SCHEMA: StructType = StructType(
    [
        StructField("celex_id", StringType(), nullable=False),
        StructField("title", StringType(), nullable=True),
        StructField("year", IntegerType(), nullable=False),
        StructField("doc_type", StringType(), nullable=False),
        StructField("doc_type_label", StringType(), nullable=False),
        StructField("text", StringType(), nullable=False),
    ]
)
"""Tabla de nodos Document. ``celex_id`` es la clave del nodo (unica -> MERGE).

Incluimos ``text`` porque gold alimenta tanto Neo4j (Document.full_text) como
ChromaDB (fuente de embeddings en la fase 3). Si la carga a Neo4j se volviera
pesada, se podria escindir el texto a una tabla aparte; por ahora gold es el
registro canonico autocontenido del documento."""

EURLEX_GOLD_TOPICS_SCHEMA: StructType = StructType(
    [
        StructField("eurovoc_code", StringType(), nullable=False),
    ]
)
"""Tabla de nodos Topic: codigos EuroVoc distintos. ``eurovoc_code`` es la clave.

Los codigos son strings opacos publicados por la EU; no tenemos sus etiquetas
legibles aqui (vendrian del tesauro EuroVoc, pendiente para mas adelante)."""

EURLEX_GOLD_BELONGS_TO_SCHEMA: StructType = StructType(
    [
        StructField("celex_id", StringType(), nullable=False),
        StructField("eurovoc_code", StringType(), nullable=False),
    ]
)
"""Tabla de aristas (:Document)-[:BELONGS_TO]->(:Topic). Una fila por par."""
