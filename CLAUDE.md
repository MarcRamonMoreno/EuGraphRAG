# CLAUDE.md — EUGraphRAG Project

## Project Overview

**EUGraphRAG** is a personal learning project by Marc Ramon Moreno to build a hybrid Data Engineer + AI Engineer portfolio piece. It implements a full end-to-end pipeline: from raw EU legislation data to a GraphRAG chatbot that answers questions by combining vector search with Knowledge Graph navigation.

**Primary goal:** Fill the skill gaps identified after a technical interview at Velorum Labs AI (Barcelona), where the founders asked about PySpark, NetworkX, scaling to production, and data engineering — areas where Marc had theoretical knowledge but lacked hands-on experience.

**Secondary goal:** Create a portfolio-ready project on GitHub that demonstrates proficiency in the modern data + AI stack.

## About the Author

Marc Ramon Moreno — AI and computational engineer based in Barcelona.

- **Current role:** Process and Project Engineer at IPB-Chemgineering Spain (since Nov 2025)
- **Previous:** Bioprocess and Control Engineer at ESA MELiSSA Pilot Plant — built a full-stack data platform (React/Flask/Docker/ETL pipeline)
- **Previous:** Junior OpenMP Software Developer at Barcelona Supercomputing Center — LLVM/Clang, C++23, RISC-V, CUDA, GPU/TPU offloading
- **Education:** Postgrad AI with Deep Learning (UPC ETSETB), MSc Modelling for Science and Engineering (UAB), BSc Biosystems Engineering (UPC)
- **Key personal project:** ADIAC v4.24 — ~4000-line Python tool for automatic multi-fluid industrial piping network design using MST (Prim), Dijkstra, iterative heuristic optimization, and a rule-based expert system (REGLAS_DISENO)

### Strengths to build on
- Advanced Python, algorithms, and graph theory (MST, Dijkstra, Prim — implemented from scratch in ADIAC)
- Docker, ETL pipelines, React/Flask full-stack (ESA project)
- Deep Learning fundamentals (UPC postgrad), CUDA/HPC (BSC)
- Strong mathematical foundations (optimization, stochastic processes, PDEs, Monte Carlo)

### Skills this project is designed to develop
- PySpark (distributed data processing) — **zero prior experience**
- Apache Airflow (workflow orchestration) — **zero prior experience**
- Neo4j + Cypher (graph database) — **theoretical only**
- NetworkX (graph analysis in Python) — **zero prior experience**
- RAG/GraphRAG with LangChain (AI engineering) — **theoretical only**
- Embeddings + Vector Databases (ChromaDB/FAISS) — **theoretical only**
- FastAPI (REST API) — **zero prior experience**
- Advanced SQL (window functions, CTEs, query optimization) — **intermediate, needs leveling up**

## Architecture

```
EUR-Lex API / HuggingFace datasets
        │
        ▼
   ┌─────────────┐
   │   PySpark    │  ← Bronze: raw JSON/XML
   │   Pipeline   │  ← Silver: cleaned, structured Parquet
   │              │  ← Gold: entities & relationships extracted
   └──────┬──────┘
          │
    ┌─────┴──────┐
    ▼            ▼
┌────────┐  ┌──────────┐
│ Neo4j  │  │ ChromaDB │
│ (KG)   │  │ (vectors)│
└───┬────┘  └────┬─────┘
    │            │
    └─────┬──────┘
          ▼
   ┌─────────────┐
   │  GraphRAG   │  ← Hybrid retriever: Cypher + vector search
   │  Retriever  │  ← LangChain orchestration
   └──────┬──────┘
          ▼
   ┌─────────────┐
   │  LLM (local)│  ← Ollama (Llama 3 / Mistral) or Groq API
   └──────┬──────┘
          ▼
   ┌─────────────┐
   │   FastAPI    │  ← POST /ask → {answer, sources, graph_context}
   └──────┬──────┘
          ▼
   ┌─────────────┐
   │  Streamlit   │  ← Chat UI + Neo4j graph visualization
   └─────────────┘

   Orchestration: Apache Airflow (DAGs for ingestion + processing)
   Containerization: Docker Compose (all services)
   CI/CD: GitHub Actions (lint, test, build)
```

## Project Structure (Target)

```
eugraphrag/
├── CLAUDE.md                    # This file
├── README.md                    # Project documentation with architecture diagram
├── docker-compose.yml           # All services: Neo4j, ChromaDB, Airflow, API, UI
├── .github/
│   └── workflows/
│       └── ci.yml               # GitHub Actions: lint + test + build
├── data/
│   ├── bronze/                  # Raw data from EUR-Lex
│   ├── silver/                  # Cleaned Parquet (PySpark output)
│   └── gold/                    # Entities & relationships ready for Neo4j
├── spark/
│   ├── __init__.py
│   ├── ingestion.py             # Download from EUR-Lex API / HuggingFace
│   ├── transformations.py       # PySpark cleaning & entity extraction
│   ├── schemas.py               # StructType definitions for Spark DataFrames
│   └── quality_checks.py        # Data validation functions
├── graph/
│   ├── __init__.py
│   ├── ontology.py              # Node/relationship type definitions
│   ├── loader.py                # Bulk load Parquet → Neo4j (UNWIND + MERGE)
│   ├── queries.py               # Reusable Cypher queries
│   ├── analysis.py              # NetworkX analysis (PageRank, communities, centrality)
│   └── constraints.sql          # Neo4j indexes and constraints
├── rag/
│   ├── __init__.py
│   ├── chunking.py              # Document chunking strategies
│   ├── embeddings.py            # Sentence-transformers embedding generation
│   ├── vectorstore.py           # ChromaDB setup and loading
│   ├── retriever.py             # Hybrid retriever (vector + graph)
│   ├── chain.py                 # LangChain RAG/GraphRAG chain
│   └── prompts.py               # Prompt templates
├── api/
│   ├── __init__.py
│   ├── main.py                  # FastAPI app
│   ├── models.py                # Pydantic request/response models
│   └── routes.py                # API endpoints
├── ui/
│   └── app.py                   # Streamlit chat interface
├── airflow/
│   └── dags/
│       ├── ingestion_dag.py     # DAG: download + PySpark transform
│       ├── graph_load_dag.py    # DAG: load into Neo4j + run analysis
│       └── embedding_dag.py     # DAG: generate embeddings + load ChromaDB
├── tests/
│   ├── test_transformations.py  # PySpark transformation tests
│   ├── test_queries.py          # Cypher query tests
│   ├── test_retriever.py        # RAG retriever tests
│   └── test_api.py              # FastAPI endpoint tests
├── notebooks/
│   ├── 01_sql_exercises.ipynb   # Week 1: SQL window functions practice
│   ├── 02_pyspark_intro.ipynb   # Week 2: PySpark fundamentals
│   ├── 03_pyspark_pipeline.ipynb # Week 3: EUR-Lex processing
│   ├── 04_neo4j_cypher.ipynb    # Week 5: Cypher practice
│   ├── 05_networkx_analysis.ipynb # Week 7: Graph algorithms
│   ├── 06_embeddings.ipynb      # Week 8: Embedding exploration
│   ├── 07_rag_prototype.ipynb   # Week 9: RAG pipeline
│   └── 08_graphrag_comparison.ipynb # Week 10: RAG vs GraphRAG
├── requirements.txt
├── pyproject.toml
└── Makefile                     # Shortcuts: make up, make test, make ingest
```

## 12-Week Learning Plan

The project is built incrementally over 12 weeks (~8-10h/week). Each phase adds a layer.

### Phase 1 — Data Engineering Foundations (Weeks 1-4)
- **W1:** Advanced SQL (window functions, CTEs, EXPLAIN ANALYZE) + data modeling (star schema for EUR-Lex)
- **W2:** PySpark fundamentals (DataFrames, Spark SQL, lazy eval, partitioning) on Databricks Community Edition
- **W3:** PySpark pipeline for EUR-Lex: ingest, parse, extract entities, write to Parquet (bronze/silver/gold)
- **W4:** Apache Airflow: DAG to orchestrate the PySpark pipeline, scheduling, retries, Docker deployment

### Phase 2 — Knowledge Graphs + Neo4j (Weeks 5-7)
- **W5:** Neo4j + Cypher fundamentals (Neo4j GraphAcademy courses, practice on Movies dataset)
- **W6:** Design EUGraphRAG ontology, bulk load data into Neo4j, validation queries
- **W7:** NetworkX: PageRank, community detection (Louvain), centrality on the EU legislation graph

### Phase 3 — AI Engineering + RAG (Weeks 8-10)
- **W8:** Embeddings with sentence-transformers (all-MiniLM-L6-v2), ChromaDB vector store
- **W9:** RAG pipeline with LangChain: retrieval → prompt → LLM (Ollama local or Groq API)
- **W10:** GraphRAG: hybrid retriever combining ChromaDB vectors + Neo4j Cypher + LLM. Comparative RAG vs GraphRAG

### Phase 4 — Productionization + Portfolio (Weeks 11-12)
- **W11:** Docker Compose for full stack, FastAPI REST API, pytest, GitHub Actions CI/CD
- **W12:** README, architecture diagram (Mermaid), demo recording, LinkedIn publication

## Tech Stack

| Category | Tool | Purpose |
|----------|------|---------|
| Distributed Processing | PySpark | Data transformation at scale |
| Orchestration | Apache Airflow | Pipeline scheduling and monitoring |
| Graph Database | Neo4j + Cypher | Knowledge Graph storage and queries |
| Graph Analysis | NetworkX | In-memory graph algorithms (PageRank, communities) |
| Vector Database | ChromaDB | Embedding storage and similarity search |
| Embeddings | sentence-transformers | Local embedding generation (no API needed) |
| LLM | Ollama (Llama 3 / Mistral) | Local inference, free. Alt: Groq API |
| RAG Framework | LangChain | Chain orchestration, retrievers, prompt management |
| API | FastAPI | REST endpoints for the GraphRAG system |
| Frontend | Streamlit | Chat UI for demo |
| Containerization | Docker + Docker Compose | Multi-service deployment |
| CI/CD | GitHub Actions | Automated testing and builds |
| Data Format | Apache Parquet | Columnar storage for processed data |
| Data Source | EUR-Lex / HuggingFace | EU legislation open data |

## Knowledge Graph Ontology (EUGraphRAG)

### Node Types
- `Document` — legislative document (directive, regulation, decision). Properties: id, title, date, type, celex_number, full_text
- `Article` — individual article within a document. Properties: number, text
- `Country` — EU member state. Properties: name, code
- `Institution` — EU institution (Parliament, Commission, Council). Properties: name, type
- `Topic` — subject area (environment, finance, transport...). Properties: name, category
- `Amendment` — amendment to a document. Properties: id, date, description

### Relationship Types
- `(:Document)-[:CONTAINS]->(:Article)`
- `(:Document)-[:REFERENCES]->(:Document)` — cross-references between legislation
- `(:Document)-[:AMENDS]->(:Document)` — amendment relationships
- `(:Document)-[:AFFECTS]->(:Country)` — countries affected by legislation
- `(:Document)-[:ISSUED_BY]->(:Institution)` — issuing institution
- `(:Document)-[:BELONGS_TO]->(:Topic)` — thematic classification
- `(:Amendment)-[:MODIFIES]->(:Document)` — what the amendment changes

## Data Pipeline Pattern

Follow the **medallion architecture** (bronze/silver/gold):

1. **Bronze (raw):** Raw JSON/XML from EUR-Lex API, stored as-is in `data/bronze/`
2. **Silver (cleaned):** PySpark cleans, deduplicates, normalizes dates, extracts metadata. Parquet partitioned by year and document type in `data/silver/`
3. **Gold (enriched):** Entities and relationships extracted, cross-references resolved. Ready for Neo4j loading. Parquet in `data/gold/`

## Conventions & Guidelines

### Code Style
- Python 3.11+
- Type hints everywhere
- Docstrings for all public functions (Google style)
- Formatter: `black` (line length 100)
- Linter: `ruff`
- PySpark: prefer DataFrame API over RDD. Use explicit schemas (StructType)

### Git Conventions
- Branch per week: `week-01/sql-modeling`, `week-02/pyspark-basics`, etc.
- Conventional commits: `feat:`, `fix:`, `docs:`, `test:`, `chore:`
- PR per week with summary of what was learned
- Keep notebooks clean: restart kernel + run all before committing

### Testing
- `pytest` for all Python tests
- PySpark tests: use local SparkSession in test fixtures
- Neo4j tests: use a test database or testcontainers
- RAG tests: mock LLM responses, test retrieval quality with known Q&A pairs

### Docker
- Each service has its own Dockerfile where needed
- `docker-compose.yml` at root: `docker compose up` should start everything
- Use named volumes for Neo4j and ChromaDB data persistence
- Environment variables in `.env` file (not committed)

## Context for Claude Code

When helping with this project, keep in mind:

1. **Your role is educator/mentor, not just coding assistant.** This is a learning project — Marc is building skills he doesn't have yet. Your primary goal is to help him *understand*, not just produce working code. Concretely:
   - **Explain the "why" behind design decisions**, not just the "what." If you choose a pattern (e.g., `UNWIND + MERGE` for Neo4j bulk loading), explain why it's preferred over alternatives.
   - **When multiple approaches exist, explain the trade-offs** so Marc builds engineering judgment (e.g., "you could use `collect_list` here but it pulls everything into the driver — `groupBy` keeps it distributed").
   - **Use Socratic questions** when it's more valuable than giving the answer directly. If Marc asks something he could reason through from prior knowledge, guide him there instead of handing the answer.
   - **Highlight transferable patterns across tools.** When a concept appears in multiple technologies, name it explicitly (e.g., "this lazy evaluation in Spark is the same idea you'll see in Airflow task dependencies and LangChain's deferred chain execution").
   - **When introducing a new tool or concept for the first time**, give a brief conceptual framing before diving into code. One paragraph of "what this is and where it fits" saves hours of confusion later.
   - **Prioritize clear, well-commented code** with explanations over clever abstractions.

2. **Incremental building.** The project is built week by week. Don't jump ahead — if we're in Week 3, the Neo4j components don't exist yet. Build on what already exists.

3. **Marc's existing strengths.** He's an advanced Python developer who has built a ~4000-line algorithmic tool (ADIAC) with graph algorithms from scratch. He understands MST, Dijkstra, heuristic optimization, complex dataclasses, and rule-based systems deeply. Don't over-explain basic Python — focus on the new tools (PySpark, Neo4j, LangChain, etc.).

4. **Connect to prior knowledge.** When explaining PySpark, compare to pandas operations Marc already knows. When explaining Neo4j, connect to the graph concepts in ADIAC. When explaining RAG, connect to the expert system (REGLAS_DISENO) in ADIAC as a parallel for structured knowledge.

5. **Production-mindset from day 1.** Even though it's a learning project, write code that looks professional: proper error handling, logging, type hints, tests. This repo will be shown to recruiters and hiring managers.

6. **Free tools only.** No paid APIs unless there's a free tier. Prefer local solutions: Ollama for LLMs, sentence-transformers for embeddings, ChromaDB for vectors, Docker for everything.

7. **Target audience for the final repo:** Technical hiring managers at data/AI startups in Barcelona (specifically companies like Velorum Labs AI that work with Knowledge Graphs, GraphRAG, and enterprise data platforms).

## Useful Commands (Target)

```bash
# Start all services
docker compose up -d

# Run PySpark ingestion pipeline
python -m spark.ingestion --source eurlex --output data/bronze/

# Run PySpark transformations
python -m spark.transformations --input data/bronze/ --output data/silver/

# Load into Neo4j
python -m graph.loader --input data/gold/ --neo4j-uri bolt://localhost:7687

# Run NetworkX analysis
python -m graph.analysis --output data/analysis/

# Generate embeddings
python -m rag.embeddings --input data/silver/ --output data/embeddings/

# Start API
uvicorn api.main:app --reload --port 8000

# Start UI
streamlit run ui/app.py

# Run tests
pytest tests/ -v

# Lint
ruff check . && black --check .
```

## Key Learning Resources

- DataTalks.Club Data Engineering Zoomcamp (free, YouTube) — PySpark, Airflow, Docker
- Neo4j GraphAcademy (free, with certificates) — Neo4j Fundamentals, Cypher, Chatbot course
- LangChain RAG From Scratch (free, YouTube) — 14 videos building RAG from zero
- Databricks Community Edition (free cloud Spark) — PySpark practice
- Start Data Engineering (free blog) — Airflow 3.0 tutorial
- Ollama (free local LLMs) — No GPU needed for small models
- sentence-transformers / sbert.net — Free local embedding models

## Session Management

**IMPORTANT:** Before ending each session (when the user says goodbye, asks to wrap up, or the conversation is clearly finishing), you MUST update `memory/project_progress.md` with:
1. What was completed in this session
2. What's currently in progress
3. What the next steps are for the following session
4. The updated date

This ensures continuity across multiple Claude Code sessions. At the START of each session, read the memory files to pick up where we left off.
