.PHONY: up down ps logs db jupyter spark-logs install lint test test-container test-integration ingest-bronze transform-silver transform-gold

# Docker
up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

# Connect to PostgreSQL
db:
	docker compose exec postgres psql -U eugraphrag -d eurlex

# Print the JupyterLab URL (Week 2+ PySpark notebooks)
jupyter:
	@echo "JupyterLab: http://localhost:8888/lab?token=eugraphrag"
	@echo "Spark UI:   http://localhost:4040  (visible solo durante un job)"

# Follow Spark/Jupyter container logs
spark-logs:
	docker compose logs -f spark

# Python
install:
	pip install -e ".[dev]"

lint:
	ruff check . && black --check .

test:
	pytest tests/ -v -m "not integration"

# Tests dentro del contenedor (donde vive PySpark). Excluye integration por defecto.
test-container:
	docker exec eugraphrag-spark sh -c 'cd /home/jovyan/work && python -m pytest tests/ -v -m "not integration"'

# Incluye los tests de integracion (hacen descarga real desde HuggingFace).
test-integration:
	docker exec eugraphrag-spark sh -c 'cd /home/jovyan/work && python -m pytest tests/ -v'

# Ingestion bronze: descarga NLP-AUEB/eurlex split=train (subset 500) -> data/bronze/eurlex/
ingest-bronze:
	docker exec eugraphrag-spark sh -c 'cd /home/jovyan/work && python -m spark.ingestion --split train --subset-size 500 --output data/bronze/eurlex'

# Transform silver: bronze -> silver (parseo CELEX + limpieza + particionado year/doc_type)
transform-silver:
	docker exec eugraphrag-spark sh -c 'cd /home/jovyan/work && python -m spark.transformations --input data/bronze/eurlex --output data/silver/eurlex'

# Transform gold: silver -> gold (tablas de nodos/aristas para Neo4j: documents/topics/belongs_to)
transform-gold:
	docker exec eugraphrag-spark sh -c 'cd /home/jovyan/work && python -m spark.gold --input data/silver/eurlex --output data/gold/eurlex'
