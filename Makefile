.PHONY: up down ps logs db install lint test

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

# Python
install:
	pip install -e ".[dev]"

lint:
	ruff check . && black --check .

test:
	pytest tests/ -v
