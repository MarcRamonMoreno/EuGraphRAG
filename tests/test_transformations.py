"""Tests para spark.transformations y spark.quality_checks.

Estrategia: todos unitarios y rapidos (sin red). Construimos DataFrames en
memoria con el schema bronze, aplicamos las transformaciones puras y validamos
el resultado. El parseo de celex_id es lo mas critico (un fallo silencioso ahi
corrompe los nodos de Neo4j rio abajo), asi que se cubre con casos reales del
dataset, incluidos los corrigenda con sufijo "(01)".
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from spark.quality_checks import (
    DataQualityError,
    assert_silver_quality,
    count_null_violations,
    count_short_text,
    count_year_out_of_range,
    run_silver_checks,
)
from spark.schemas import EURLEX_BRONZE_SCHEMA, EURLEX_SILVER_SCHEMA
from spark.transformations import (
    bronze_to_silver,
    with_celex_components,
    with_clean_text,
    with_doc_type_label,
)

# (celex_id, sector_esperado, year_esperado, doc_type_esperado, label_esperada)
# Los 4 primeros son ids reales del bronze; el "(01)" valida tolerancia a
# corrigenda; el ...L... valida Directiva; el ...Z... valida el fallback "Other".
CELEX_CASES = [
    ("32014R0727", "3", 2014, "R", "Regulation"),
    ("31975R2481", "3", 1975, "R", "Regulation"),
    ("32010D0008", "3", 2010, "D", "Decision"),
    ("32003D0253(01)", "3", 2003, "D", "Decision"),
    ("32014L0024", "3", 2014, "L", "Directive"),
    ("32014Z0001", "3", 2014, "Z", "Other"),
]


def _bronze_df(spark: SparkSession, rows: list[tuple]):
    """Helper: crea un DataFrame bronze a partir de tuplas con su schema."""
    return spark.createDataFrame(rows, schema=EURLEX_BRONZE_SCHEMA)


@pytest.mark.parametrize("celex_id,sector,year,doc_type,label", CELEX_CASES)
def test_celex_parsing(
    spark: SparkSession,
    celex_id: str,
    sector: str,
    year: int,
    doc_type: str,
    label: str,
) -> None:
    """Cada celex_id se descompone en (sector, year, doc_type, label) correctos."""
    rows = [(celex_id, "Some title", "Body text long enough for silver.", ["192"])]
    df = _bronze_df(spark, rows).transform(with_celex_components).transform(with_doc_type_label)
    r = df.collect()[0]
    assert r.sector == sector
    assert r.year == year
    assert r.doc_type == doc_type
    assert r.doc_type_label == label


def test_year_is_integer_type(spark: SparkSession) -> None:
    """year debe ser entero (para pruning de particiones y rangos), no string."""
    df = _bronze_df(spark, [("32014R0727", "t", "body", None)]).transform(with_celex_components)
    assert df.schema["year"].dataType.typeName() == "integer"


def test_clean_text_removes_oj_header(spark: SparkSession) -> None:
    """La cabecera del Diario Oficial se elimina del inicio del texto."""
    raw = (
        "1.7.2014 EN Official Journal of the European Union L 192/42\n"
        "COMMISSION REGULATION\nof 30 June 2014\nBody of the act."
    )
    df = _bronze_df(spark, [("32014R0727", "t", raw, None)]).transform(with_clean_text)
    text = df.collect()[0].text
    assert "Official Journal of the European Union" not in text
    assert text.startswith("COMMISSION REGULATION")


def test_clean_text_preserves_paragraphs(spark: SparkSession) -> None:
    """No aplastamos saltos de linea: 3+ se colapsan a 2, los simples se quedan."""
    raw = "Para uno.\n\n\n\nPara dos.\nLinea contigua."
    df = _bronze_df(spark, [("32014R0727", "t", raw, None)]).transform(with_clean_text)
    text = df.collect()[0].text
    assert "Para uno.\n\nPara dos." in text  # 4 saltos -> 2
    assert "Para dos.\nLinea contigua" in text  # salto simple preservado


def test_clean_text_collapses_horizontal_whitespace(spark: SparkSession) -> None:
    """Runs de espacios/tabuladores se colapsan a un solo espacio."""
    df = _bronze_df(spark, [("32014R0727", "t", "word1     word2\tword3", None)]).transform(
        with_clean_text
    )
    assert df.collect()[0].text == "word1 word2 word3"


def test_clean_text_trims_title(spark: SparkSession) -> None:
    """El title (que en bronze viene con \\n final) se recorta."""
    df = _bronze_df(spark, [("32014R0727", "  Council Decision\n", "body text", None)]).transform(
        with_clean_text
    )
    assert df.collect()[0].title == "Council Decision"


def test_silver_schema_matches_transform_output(spark: SparkSession) -> None:
    """El DataFrame silver (pre-write) tiene exactamente los campos del schema."""
    df = (
        _bronze_df(spark, [("32014R0727", "t", "a body long enough", ["192"])])
        .transform(with_celex_components)
        .transform(with_doc_type_label)
        .transform(with_clean_text)
        .select([f.name for f in EURLEX_SILVER_SCHEMA.fields])
    )
    assert [f.name for f in df.schema.fields] == [f.name for f in EURLEX_SILVER_SCHEMA.fields]


# --------------------------- quality_checks ---------------------------------


def test_quality_detects_null_celex(spark: SparkSession) -> None:
    """count_null_violations cuenta los celex_id nulos."""
    rows = [
        ("32014R0727", 2014, "R", "body ok long enough text here"),
        (None, 2010, "D", "another body text long enough"),
    ]
    df = spark.createDataFrame(rows, ["celex_id", "year", "doc_type", "text"])
    viol = count_null_violations(df)
    assert viol["celex_id"] == 1


def test_quality_detects_short_text(spark: SparkSession) -> None:
    """Textos por debajo del umbral se cuentan como violacion."""
    rows = [("a", "x" * 100), ("b", "short")]
    df = spark.createDataFrame(rows, ["celex_id", "text"])
    assert count_short_text(df, min_length=50) == 1


def test_quality_detects_year_out_of_range(spark: SparkSession) -> None:
    """Years nulos o fuera de rango se cuentan."""
    rows = [(2014,), (1800,), (None,)]
    df = spark.createDataFrame(rows, ["year"])
    assert count_year_out_of_range(df, min_year=1951, max_year=2030) == 2


def test_assert_silver_quality_passes_clean_df(spark: SparkSession) -> None:
    """Un DataFrame silver limpio no lanza y reporta 0 violaciones."""
    rows = [("32014R0727", 2014, "R", "x" * 100), ("32010D0008", 2010, "D", "y" * 100)]
    df = spark.createDataFrame(rows, ["celex_id", "year", "doc_type", "text"])
    report = assert_silver_quality(df, max_year=2030)
    assert report.is_ok
    assert report.total_rows == 2


def test_assert_silver_quality_raises_on_violation(spark: SparkSession) -> None:
    """Con datos sucios, assert_silver_quality lanza DataQualityError."""
    rows = [(None, 1800, "R", "short")]
    # Schema explicito: con celex_id todo-NULL, Spark no puede inferir el tipo.
    df = spark.createDataFrame(rows, "celex_id string, year int, doc_type string, text string")
    with pytest.raises(DataQualityError):
        assert_silver_quality(df, max_year=2030)


# --------------------------- end-to-end -------------------------------------


def test_bronze_to_silver_roundtrip(spark: SparkSession, tmp_path: Path) -> None:
    """bronze_to_silver escribe Parquet particionado y se relee con los campos."""
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"

    rows = [
        ("32014R0727", "Reg 727", "x" * 80, ["192"]),
        ("32010D0008", "Dec 8", "y" * 80, ["2356"]),
        ("32014R0001", "Reg 1", "z" * 80, None),
    ]
    _bronze_df(spark, rows).write.mode("overwrite").parquet(str(bronze_dir))

    n = bronze_to_silver(bronze_dir, silver_dir, spark=spark)
    assert n == 3

    back = spark.read.parquet(str(silver_dir))
    # year y doc_type vuelven como columnas de particion (al final, tipo inferido).
    field_names = {f.name for f in back.schema.fields}
    assert field_names == {f.name for f in EURLEX_SILVER_SCHEMA.fields}
    # Las particiones esperadas existen en disco (Hive-style).
    assert (silver_dir / "year=2014" / "doc_type=R").exists()
    assert (silver_dir / "year=2010" / "doc_type=D").exists()

    # Y la calidad del resultado debe pasar el gate.
    assert run_silver_checks(back, max_year=2030).is_ok
