"""EUGraphRAG Spark pipeline package.

Contiene modulos PySpark para la pipeline bronze -> silver -> gold:
- schemas: definiciones explicitas de StructType (single source of truth).
- ingestion: descarga desde fuentes externas (HuggingFace, EUR-Lex SPARQL...) -> bronze.
- transformations: bronze -> silver (limpieza, normalizacion, particionado).
- quality_checks: validaciones de data quality entre etapas.

Cada modulo expone funciones puras (SparkSession se pasa por parametro) para
que Airflow pueda invocarlas como tareas aisladas y pytest pueda testearlas
con una sesion local.
"""
