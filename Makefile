.PHONY: up down ps logs db jupyter spark-logs install lint test

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
	pytest tests/ -v
