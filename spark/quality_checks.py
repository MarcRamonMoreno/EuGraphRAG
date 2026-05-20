"""Validaciones de data quality para la capa silver.

Filosofia: "fail fast, fail loud". Es mejor que el pipeline pete aqui, con un
mensaje claro, que dejar pasar datos corruptos que luego generan nodos basura
en Neo4j o chunks vacios en el vector store (donde el bug es mucho mas dificil
de rastrear).

Cada check es una funcion pura que CUENTA violaciones (no lanza). El orquestador
``assert_silver_quality`` agrega los conteos en un ``QualityReport`` y decide si
abortar. Separar "medir" de "decidir" permite: (a) testear cada check aislado,
y (b) en el futuro logar el reporte sin abortar (modo warning) si conviene.

Nota Spark: estos checks disparan jobs (count/filter son acciones o casi). En un
pipeline real se cachearia el DataFrame antes de correr varios checks para no
recomputar la cadena cada vez. Con ~100 filas no merece la pena.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

# Limites por defecto para silver EUR-Lex.
MIN_TEXT_LENGTH = 50  # un acto legal real tiene mucho mas que 50 chars
MIN_VALID_YEAR = 1951  # Tratado CECA (1951): inicio del acervo comunitario
NOT_NULL_COLUMNS = ("celex_id", "year", "doc_type", "text")


class DataQualityError(Exception):
    """Se lanza cuando la capa silver no cumple el contrato de calidad."""


@dataclass
class QualityReport:
    """Resumen de violaciones encontradas en un DataFrame silver."""

    total_rows: int
    null_violations: dict[str, int] = field(default_factory=dict)
    short_text_rows: int = 0
    year_out_of_range_rows: int = 0

    @property
    def total_violations(self) -> int:
        """Suma de todas las filas que violan algun check."""
        return (
            sum(self.null_violations.values()) + self.short_text_rows + self.year_out_of_range_rows
        )

    @property
    def is_ok(self) -> bool:
        """True si no hay ninguna violacion."""
        return self.total_violations == 0


def count_null_violations(
    df: DataFrame, columns: tuple[str, ...] = NOT_NULL_COLUMNS
) -> dict[str, int]:
    """Cuenta filas con NULL en cada columna que deberia ser obligatoria.

    Recordatorio: Parquet no preserva ``nullable=False``, asi que la garantia
    de no-nulos hay que reimponerla aqui en el lado de lectura.
    """
    return {col: df.filter(F.col(col).isNull()).count() for col in columns}


def count_short_text(df: DataFrame, min_length: int = MIN_TEXT_LENGTH) -> int:
    """Cuenta filas cuyo ``text`` es nulo o mas corto que ``min_length``.

    Un texto demasiado corto suele indicar que la limpieza se comio el cuerpo
    o que el documento llego truncado del origen.
    """
    return df.filter(F.col("text").isNull() | (F.length(F.col("text")) < min_length)).count()


def count_year_out_of_range(
    df: DataFrame,
    min_year: int = MIN_VALID_YEAR,
    max_year: int | None = None,
) -> int:
    """Cuenta filas con ``year`` nulo o fuera de [min_year, max_year].

    ``max_year`` por defecto es el ano actual + 1 (margen para legislacion
    publicada a caballo de fin de ano). Un year fuera de rango casi siempre
    significa que el parseo del celex_id fallo.
    """
    if max_year is None:
        max_year = datetime.now().year + 1
    return df.filter(
        F.col("year").isNull() | (F.col("year") < min_year) | (F.col("year") > max_year)
    ).count()


def run_silver_checks(
    df: DataFrame,
    min_text_length: int = MIN_TEXT_LENGTH,
    min_year: int = MIN_VALID_YEAR,
    max_year: int | None = None,
) -> QualityReport:
    """Ejecuta todos los checks y devuelve un ``QualityReport`` (no lanza)."""
    return QualityReport(
        total_rows=df.count(),
        null_violations=count_null_violations(df),
        short_text_rows=count_short_text(df, min_text_length),
        year_out_of_range_rows=count_year_out_of_range(df, min_year, max_year),
    )


def assert_silver_quality(
    df: DataFrame,
    min_text_length: int = MIN_TEXT_LENGTH,
    min_year: int = MIN_VALID_YEAR,
    max_year: int | None = None,
) -> QualityReport:
    """Corre los checks y lanza ``DataQualityError`` si hay alguna violacion.

    Devuelve el reporte tambien en el caso OK, por si el llamante quiere logarlo.
    Esta es la funcion que un DAG de Airflow llamaria como gate entre silver y
    la siguiente etapa.
    """
    report = run_silver_checks(df, min_text_length, min_year, max_year)
    if report.is_ok:
        logger.info("Silver quality OK: %d rows, 0 violations", report.total_rows)
        return report

    raise DataQualityError(
        f"Silver quality FAILED: {report.total_violations} violations over "
        f"{report.total_rows} rows | nulls={report.null_violations} "
        f"short_text={report.short_text_rows} "
        f"year_out_of_range={report.year_out_of_range_rows}"
    )
