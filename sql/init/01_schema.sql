-- =============================================================================
-- EUR-Lex Star Schema for EUGraphRAG
-- =============================================================================
-- This is a STAR SCHEMA design: one fact table at the center, surrounded by
-- dimension tables. Why star schema?
--
-- In analytics, you typically have:
--   - FACTS: events/measurements (here: a document being published)
--   - DIMENSIONS: the context around the fact (who, what, when, where)
--
-- Star schema is optimized for analytical queries (OLAP) because:
--   1. JOINs are always fact → dimension (one hop), never dimension → dimension
--   2. Dimension tables are small → JOINs are fast
--   3. It maps naturally to how humans ask questions:
--      "How many REGULATIONS were issued BY THE COMMISSION in 2023 ABOUT environment?"
--       ^^^^fact^^^^                   ^^^dimension^^^        ^^^^dimension^^^^
--
-- Compare with the NORMALIZED (3NF) design you'd use for transactional systems:
-- normalized = no redundancy, many tables, complex JOINs
-- star schema = controlled redundancy, fewer tables, simple JOINs, fast reads
-- =============================================================================

-- ─────────────────────────────────────────────
-- DIMENSION: Time (date dimension for slicing by year, quarter, month)
-- ─────────────────────────────────────────────
-- Why a separate date dimension instead of just storing the date?
-- Because you'll want to GROUP BY year, quarter, month, day_of_week, etc.
-- A date dimension pre-computes these, avoiding repeated EXTRACT() calls.
CREATE TABLE dim_date (
    date_key    INTEGER PRIMARY KEY,          -- YYYYMMDD format (e.g., 20230615)
    full_date   DATE NOT NULL UNIQUE,
    year        SMALLINT NOT NULL,
    quarter     SMALLINT NOT NULL,
    month       SMALLINT NOT NULL,
    month_name  VARCHAR(20) NOT NULL,
    day         SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,            -- 1=Monday, 7=Sunday (ISO)
    day_name    VARCHAR(20) NOT NULL,
    is_weekend  BOOLEAN NOT NULL
);

-- ─────────────────────────────────────────────
-- DIMENSION: Document Type
-- ─────────────────────────────────────────────
CREATE TABLE dim_document_type (
    type_id   SERIAL PRIMARY KEY,
    type_code VARCHAR(20) NOT NULL UNIQUE,    -- 'REG', 'DIR', 'DEC', etc.
    type_name VARCHAR(100) NOT NULL,          -- 'Regulation', 'Directive', 'Decision'
    description TEXT
);

-- ─────────────────────────────────────────────
-- DIMENSION: Institution (who issued the document)
-- ─────────────────────────────────────────────
CREATE TABLE dim_institution (
    institution_id   SERIAL PRIMARY KEY,
    institution_code VARCHAR(20) NOT NULL UNIQUE,
    institution_name VARCHAR(200) NOT NULL,
    institution_type VARCHAR(50)              -- 'legislative', 'executive', 'judicial'
);

-- ─────────────────────────────────────────────
-- DIMENSION: Topic (subject area classification)
-- ─────────────────────────────────────────────
CREATE TABLE dim_topic (
    topic_id SERIAL PRIMARY KEY,
    topic_code VARCHAR(20) NOT NULL UNIQUE,
    topic_name VARCHAR(200) NOT NULL,
    category   VARCHAR(100)                   -- broader grouping
);

-- ─────────────────────────────────────────────
-- DIMENSION: Country (EU member states affected)
-- ─────────────────────────────────────────────
CREATE TABLE dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_code CHAR(2) NOT NULL UNIQUE,     -- ISO 3166-1 alpha-2
    country_name VARCHAR(100) NOT NULL,
    join_year    SMALLINT                     -- year joined the EU
);

-- ─────────────────────────────────────────────
-- FACT: Documents (the central fact table)
-- ─────────────────────────────────────────────
-- Each row = one EU legislative document published.
-- The fact table references all dimension tables via foreign keys.
-- Measures: article_count, text_length, reference_count, amendment_count
CREATE TABLE fact_documents (
    document_id     SERIAL PRIMARY KEY,
    celex_number    VARCHAR(50) NOT NULL UNIQUE,  -- EUR-Lex unique identifier
    title           TEXT NOT NULL,
    publication_date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    type_id         INTEGER NOT NULL REFERENCES dim_document_type(type_id),
    institution_id  INTEGER NOT NULL REFERENCES dim_institution(institution_id),
    -- Measures (quantitative facts about each document)
    article_count   INTEGER DEFAULT 0,
    text_length     INTEGER DEFAULT 0,            -- character count of full text
    reference_count INTEGER DEFAULT 0,            -- how many other docs it references
    amendment_count INTEGER DEFAULT 0             -- how many times it's been amended
);

-- ─────────────────────────────────────────────
-- BRIDGE: Document ↔ Topic (many-to-many)
-- ─────────────────────────────────────────────
-- A document can belong to multiple topics, and a topic has many documents.
-- In star schema, this is handled with a "bridge table."
CREATE TABLE bridge_document_topic (
    document_id INTEGER NOT NULL REFERENCES fact_documents(document_id),
    topic_id    INTEGER NOT NULL REFERENCES dim_topic(topic_id),
    PRIMARY KEY (document_id, topic_id)
);

-- ─────────────────────────────────────────────
-- BRIDGE: Document ↔ Country (many-to-many)
-- ─────────────────────────────────────────────
CREATE TABLE bridge_document_country (
    document_id INTEGER NOT NULL REFERENCES fact_documents(document_id),
    country_id  INTEGER NOT NULL REFERENCES dim_country(country_id),
    PRIMARY KEY (document_id, country_id)
);

-- ─────────────────────────────────────────────
-- SELF-REFERENCE: Document cross-references
-- ─────────────────────────────────────────────
-- Legislation frequently references other legislation. This table captures
-- those links — it will later become REFERENCES edges in Neo4j.
CREATE TABLE document_references (
    source_document_id INTEGER NOT NULL REFERENCES fact_documents(document_id),
    target_document_id INTEGER NOT NULL REFERENCES fact_documents(document_id),
    reference_type     VARCHAR(20) NOT NULL DEFAULT 'cites',  -- 'cites', 'amends', 'repeals'
    PRIMARY KEY (source_document_id, target_document_id, reference_type)
);

-- ─────────────────────────────────────────────
-- INDEXES for common query patterns
-- ─────────────────────────────────────────────
CREATE INDEX idx_fact_documents_date ON fact_documents(publication_date_key);
CREATE INDEX idx_fact_documents_type ON fact_documents(type_id);
CREATE INDEX idx_fact_documents_institution ON fact_documents(institution_id);
CREATE INDEX idx_dim_date_year ON dim_date(year);
CREATE INDEX idx_document_references_target ON document_references(target_document_id);
