-- =============================================================================
-- Seed Data for EUR-Lex Star Schema
-- =============================================================================
-- Realistic sample data based on actual EU legislation. This gives us enough
-- rows to practice window functions, CTEs, and EXPLAIN ANALYZE meaningfully.
-- =============================================================================

-- ─────────────────────────────────────────────
-- dim_date: Generate dates from 2019 to 2025
-- ─────────────────────────────────────────────
-- This uses generate_series() — a PostgreSQL-specific function that creates
-- a set of values. Very useful for populating date dimensions.
INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d AS full_date,
    EXTRACT(YEAR FROM d)::SMALLINT AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM d)::SMALLINT AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(DAY FROM d)::SMALLINT AS day,
    EXTRACT(ISODOW FROM d)::SMALLINT AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
FROM generate_series('2019-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) AS d;

-- ─────────────────────────────────────────────
-- dim_document_type
-- ─────────────────────────────────────────────
INSERT INTO dim_document_type (type_code, type_name, description) VALUES
('REG', 'Regulation', 'Directly applicable in all member states without transposition'),
('DIR', 'Directive', 'Sets goals that member states must achieve through national legislation'),
('DEC', 'Decision', 'Binding on specific addressees (member states, companies, individuals)'),
('REC', 'Recommendation', 'Non-binding guidance suggesting a course of action'),
('OPI', 'Opinion', 'Non-binding statement by an EU institution');

-- ─────────────────────────────────────────────
-- dim_institution
-- ─────────────────────────────────────────────
INSERT INTO dim_institution (institution_code, institution_name, institution_type) VALUES
('EP',    'European Parliament',                          'legislative'),
('CONS',  'Council of the European Union',                'legislative'),
('COM',   'European Commission',                          'executive'),
('ECB',   'European Central Bank',                        'executive'),
('CJEU',  'Court of Justice of the European Union',       'judicial'),
('ECA',   'European Court of Auditors',                   'judicial'),
('EP_CONS', 'European Parliament and Council (co-decision)', 'legislative');

-- ─────────────────────────────────────────────
-- dim_topic
-- ─────────────────────────────────────────────
INSERT INTO dim_topic (topic_code, topic_name, category) VALUES
('ENV',  'Environment and Climate',           'Sustainability'),
('FIN',  'Financial Services and Banking',    'Economy'),
('DIG',  'Digital Economy and Technology',    'Technology'),
('TRA',  'Transport and Mobility',            'Infrastructure'),
('ENE',  'Energy',                            'Infrastructure'),
('AGR',  'Agriculture and Fisheries',         'Primary Sector'),
('HEA',  'Health and Food Safety',            'Social'),
('JUS',  'Justice and Home Affairs',          'Governance'),
('TRD',  'Trade and Customs',                 'Economy'),
('LAB',  'Employment and Social Policy',      'Social'),
('DEF',  'Defence and Security',              'Governance'),
('EDU',  'Education and Research',            'Social');

-- ─────────────────────────────────────────────
-- dim_country (EU-27 + UK for historical data)
-- ─────────────────────────────────────────────
INSERT INTO dim_country (country_code, country_name, join_year) VALUES
('AT', 'Austria',       1995), ('BE', 'Belgium',       1958),
('BG', 'Bulgaria',      2007), ('HR', 'Croatia',       2013),
('CY', 'Cyprus',        2004), ('CZ', 'Czech Republic', 2004),
('DK', 'Denmark',       1973), ('EE', 'Estonia',       2004),
('FI', 'Finland',       1995), ('FR', 'France',        1958),
('DE', 'Germany',       1958), ('GR', 'Greece',        1981),
('HU', 'Hungary',       2004), ('IE', 'Ireland',       1973),
('IT', 'Italy',         1958), ('LV', 'Latvia',        2004),
('LT', 'Lithuania',     2004), ('LU', 'Luxembourg',    1958),
('MT', 'Malta',         2004), ('NL', 'Netherlands',   1958),
('PL', 'Poland',        2004), ('PT', 'Portugal',      1986),
('RO', 'Romania',       2007), ('SK', 'Slovakia',      2004),
('SI', 'Slovenia',      2004), ('ES', 'Spain',         1986),
('SE', 'Sweden',        1995), ('GB', 'United Kingdom', 1973);

-- ─────────────────────────────────────────────
-- fact_documents: ~60 realistic EU documents
-- ─────────────────────────────────────────────
INSERT INTO fact_documents (celex_number, title, publication_date_key, type_id, institution_id, article_count, text_length, reference_count, amendment_count) VALUES
-- 2019
('32019R0631', 'CO2 emission performance standards for new passenger cars',           20190417, 1, 7, 25, 48000, 12, 3),
('32019L0790', 'Directive on copyright in the Digital Single Market',                 20190517, 2, 7, 32, 62000, 18, 1),
('32019L0882', 'European Accessibility Act',                                          20190617, 2, 7, 45, 71000, 15, 0),
('32019R0881', 'ENISA and ICT cybersecurity certification (Cybersecurity Act)',        20190617, 1, 7, 69, 95000, 22, 2),
('32019L0944', 'Common rules for the internal market for electricity',                20190614, 2, 7, 72, 110000, 35, 4),
('32019R1150', 'Promoting fairness and transparency for online platform users',       20190720, 1, 7, 18, 32000, 8, 0),
('32019L1023', 'Preventive restructuring frameworks and discharge of debt',           20190626, 2, 7, 38, 58000, 20, 1),
('32019R1148', 'European Maritime Single Window environment',                         20190720, 1, 7, 24, 41000, 14, 0),
-- 2020
('32020R0852', 'EU Taxonomy for sustainable activities',                              20200622, 1, 7, 27, 52000, 16, 5),
('32020R0741', 'Minimum requirements for water reuse',                                20200625, 1, 7, 15, 28000, 9, 0),
('32020L2184', 'Quality of water intended for human consumption (Drinking Water)',     20201223, 2, 7, 23, 45000, 19, 2),
('32020R1503', 'European Crowdfunding Service Providers regulation',                  20201020, 1, 7, 51, 78000, 25, 1),
('32020R0672', 'European instrument for temporary support (SURE)',                    20200519, 1, 4, 12, 18000, 6, 0),
('32020L1828', 'Representative actions for the protection of consumers',              20201224, 2, 7, 28, 43000, 17, 0),
('32020R2094', 'European Union Recovery Instrument',                                  20201222, 1, 4, 10, 15000, 8, 0),
('32020R2092', 'General regime of conditionality for the protection of the EU budget', 20201222, 1, 7, 10, 22000, 11, 3),
-- 2021
('32021R0693', 'European Institute of Innovation and Technology (EIT) regulation',    20210428, 1, 7, 22, 35000, 10, 1),
('32021R0694', 'Horizon Europe implementation',                                       20210428, 1, 7, 55, 88000, 30, 2),
('32021R0695', 'Horizon Europe Framework Programme',                                  20210428, 1, 7, 48, 82000, 28, 0),
('32021R1060', 'Common Provisions Regulation for EU funds 2021-2027',                 20210630, 1, 7, 115, 180000, 45, 6),
('32021R1119', 'European Climate Law',                                                20210630, 1, 7, 15, 24000, 12, 2),
('32021R2115', 'CAP Strategic Plans regulation',                                      20211206, 1, 7, 148, 220000, 52, 3),
('32021R2116', 'Financing, management and monitoring of the CAP',                     20211206, 1, 7, 102, 165000, 38, 1),
('32021R1153', 'Carbon Border Adjustment Mechanism (pilot provisions)',               20210710, 1, 3, 36, 55000, 20, 4),
-- 2022
('32022R2065', 'Digital Services Act',                                                20221027, 1, 7, 93, 145000, 42, 2),
('32022R1925', 'Digital Markets Act',                                                 20220914, 1, 7, 54, 98000, 28, 1),
('32022R0868', 'European Data Governance Act',                                        20220603, 1, 7, 38, 62000, 21, 0),
('32022L2464', 'Corporate Sustainability Reporting Directive (CSRD)',                  20221216, 2, 7, 18, 48000, 25, 3),
('32022R2560', 'Markets in Crypto-Assets Regulation (MiCA)',                          20221229, 1, 7, 149, 230000, 55, 0),
('32022L2380', 'General Product Safety Regulation',                                   20221123, 2, 7, 44, 72000, 30, 1),
('32022R0858', 'DLT Pilot Regime for market infrastructures',                         20220602, 1, 7, 20, 36000, 15, 0),
('32022R1369', 'Energy labelling framework regulation',                               20220711, 1, 3, 16, 25000, 10, 2),
-- 2023
('32023R1114', 'Markets in Crypto-Assets (MiCA) - implementing measures',             20230609, 1, 3, 22, 38000, 18, 0),
('32023R2405', 'Euro 7 motor vehicle emission standards',                             20231113, 1, 7, 35, 58000, 22, 0),
('32023L1791', 'Energy Efficiency Directive (recast)',                                 20230920, 2, 7, 40, 85000, 32, 5),
('32023R0956', 'Carbon Border Adjustment Mechanism (CBAM)',                            20230510, 1, 7, 36, 65000, 24, 2),
('32023L2413', 'Renewable Energy Directive (RED III)',                                 20231031, 2, 7, 38, 92000, 35, 7),
('32023R1115', 'Deforestation-free products regulation',                              20230609, 1, 7, 34, 56000, 19, 1),
('32023R2854', 'Data Act',                                                            20231222, 1, 7, 50, 88000, 30, 0),
('32023L2225', 'Corporate Sustainability Due Diligence Directive (CSDDD)',             20231205, 2, 7, 30, 52000, 22, 0),
('32023R0988', 'Ecodesign for Sustainable Products Regulation',                       20230512, 1, 7, 68, 105000, 28, 1),
('32023R2631', 'European Green Bonds standard',                                       20231130, 1, 7, 42, 70000, 20, 0),
-- 2024
('32024R1689', 'Artificial Intelligence Act (AI Act)',                                20240713, 1, 7, 113, 190000, 48, 0),
('32024R0903', 'European Health Data Space regulation',                               20240424, 1, 7, 72, 120000, 35, 0),
('32024R1183', 'European Digital Identity (eIDAS 2)',                                 20240520, 1, 7, 45, 78000, 26, 3),
('32024L1760', 'Right to Repair Directive',                                           20240710, 2, 7, 22, 38000, 14, 0),
('32024R1781', 'Critical Raw Materials Act',                                          20240723, 1, 7, 47, 82000, 28, 1),
('32024R1735', 'Cyber Resilience Act',                                                20240712, 1, 7, 58, 95000, 32, 0),
('32024L1640', 'AI Liability Directive',                                              20240628, 2, 7, 18, 32000, 12, 0),
('32024R0900', 'Net-Zero Industry Act',                                               20240420, 1, 7, 42, 72000, 25, 2),
('32024R1252', 'European Chips Act (implementing regulation)',                        20240605, 1, 3, 28, 48000, 18, 0),
('32024R2847', 'Packaging and Packaging Waste Regulation',                            20241118, 1, 7, 65, 108000, 30, 1),
-- 2025 (projected/recent)
('32025R0100', 'Accessibility requirements for financial services products',          20250115, 1, 3, 20, 34000, 14, 0),
('32025L0200', 'Soil Monitoring and Resilience Directive',                            20250312, 2, 7, 28, 52000, 18, 0),
('32025R0301', 'EU Space Law regulation',                                             20250228, 1, 7, 55, 90000, 22, 0),
('32025R0150', 'Anti-Money Laundering Authority (AMLA) operational regulation',       20250205, 1, 7, 38, 64000, 20, 0),
('32025L0405', 'Directive on combating violence against women',                       20250320, 2, 7, 25, 42000, 15, 0),
('32025R0500', 'European Media Freedom Act (implementing measures)',                  20250401, 1, 3, 18, 30000, 12, 0);

-- ─────────────────────────────────────────────
-- bridge_document_topic: Assign 1-3 topics per document
-- ─────────────────────────────────────────────
-- Using document_id from insertion order (1-based SERIAL)
INSERT INTO bridge_document_topic (document_id, topic_id) VALUES
-- 2019 docs
(1, 1), (1, 4),           -- CO2 standards → Environment, Transport
(2, 3),                    -- Copyright → Digital
(3, 10),                   -- Accessibility → Employment
(4, 3), (4, 11),          -- Cybersecurity → Digital, Defence
(5, 5),                    -- Electricity → Energy
(6, 3),                    -- Platform fairness → Digital
(7, 2), (7, 8),           -- Restructuring → Financial, Justice
(8, 4), (8, 9),           -- Maritime → Transport, Trade
-- 2020 docs
(9, 1), (9, 2),           -- Taxonomy → Environment, Financial
(10, 1), (10, 7),         -- Water reuse → Environment, Health
(11, 7),                   -- Drinking Water → Health
(12, 2), (12, 3),         -- Crowdfunding → Financial, Digital
(13, 2), (13, 10),        -- SURE → Financial, Employment
(14, 8),                   -- Consumer protection → Justice
(15, 2),                   -- Recovery Instrument → Financial
(16, 2), (16, 8),         -- Budget conditionality → Financial, Justice
-- 2021 docs
(17, 12),                  -- EIT → Education
(18, 12), (18, 3),        -- Horizon Europe impl → Education, Digital
(19, 12),                  -- Horizon Europe → Education
(20, 2),                   -- Common Provisions → Financial
(21, 1),                   -- Climate Law → Environment
(22, 6),                   -- CAP Plans → Agriculture
(23, 6), (23, 2),         -- CAP Financing → Agriculture, Financial
(24, 1), (24, 9),         -- CBAM pilot → Environment, Trade
-- 2022 docs
(25, 3), (25, 8),         -- DSA → Digital, Justice
(26, 3),                   -- DMA → Digital
(27, 3),                   -- Data Governance → Digital
(28, 1), (28, 2),         -- CSRD → Environment, Financial
(29, 2), (29, 3),         -- MiCA → Financial, Digital
(30, 7),                   -- Product Safety → Health
(31, 2), (31, 3),         -- DLT Pilot → Financial, Digital
(32, 5),                   -- Energy labelling → Energy
-- 2023 docs
(33, 2), (33, 3),         -- MiCA impl → Financial, Digital
(34, 1), (34, 4),         -- Euro 7 → Environment, Transport
(35, 5),                   -- Energy Efficiency → Energy
(36, 1), (36, 9),         -- CBAM → Environment, Trade
(37, 5), (37, 1),         -- RED III → Energy, Environment
(38, 1), (38, 9),         -- Deforestation → Environment, Trade
(39, 3),                   -- Data Act → Digital
(40, 1), (40, 8),         -- CSDDD → Environment, Justice
(41, 1), (41, 3),         -- Ecodesign → Environment, Digital
(42, 1), (42, 2),         -- Green Bonds → Environment, Financial
-- 2024 docs
(43, 3), (43, 8),         -- AI Act → Digital, Justice
(44, 7), (44, 3),         -- Health Data Space → Health, Digital
(45, 3),                   -- eIDAS 2 → Digital
(46, 1),                   -- Right to Repair → Environment
(47, 1), (47, 9),         -- Critical Raw Materials → Environment, Trade
(48, 3), (48, 11),        -- Cyber Resilience → Digital, Defence
(49, 3), (49, 8),         -- AI Liability → Digital, Justice
(50, 5), (50, 1),         -- Net-Zero → Energy, Environment
(51, 3),                   -- Chips Act → Digital
(52, 1),                   -- Packaging → Environment
-- 2025 docs
(53, 2),                   -- Accessibility financial → Financial
(54, 1), (54, 6),         -- Soil → Environment, Agriculture
(55, 3), (55, 11),        -- Space Law → Digital, Defence
(56, 2), (56, 8),         -- AMLA → Financial, Justice
(57, 8), (57, 10),        -- Violence against women → Justice, Employment
(58, 3);                   -- Media Freedom → Digital

-- ─────────────────────────────────────────────
-- bridge_document_country: Assign countries affected
-- ─────────────────────────────────────────────
-- Most EU regulations/directives affect all EU-27 members.
-- For simplicity, assign all 27 current members to most documents.
-- (Excluding UK=28 for post-2020 documents)
INSERT INTO bridge_document_country (document_id, country_id)
SELECT d.document_id, c.country_id
FROM fact_documents d
CROSS JOIN dim_country c
WHERE c.country_code != 'GB'  -- UK not affected by post-Brexit legislation
  AND d.document_id > 8;      -- 2020 onwards

-- Pre-2020 docs include UK
INSERT INTO bridge_document_country (document_id, country_id)
SELECT d.document_id, c.country_id
FROM fact_documents d
CROSS JOIN dim_country c
WHERE d.document_id <= 8;

-- ─────────────────────────────────────────────
-- document_references: Cross-references between documents
-- ─────────────────────────────────────────────
INSERT INTO document_references (source_document_id, target_document_id, reference_type) VALUES
-- CBAM references EU Taxonomy and Climate Law
(36, 9, 'cites'), (36, 21, 'cites'),
-- MiCA implementing references MiCA
(33, 29, 'cites'),
-- RED III amends previous energy directives
(37, 5, 'amends'), (37, 35, 'cites'),
-- AI Liability references AI Act
(49, 43, 'cites'),
-- CSRD references EU Taxonomy
(28, 9, 'cites'),
-- DSA references Platform Fairness regulation
(25, 6, 'cites'),
-- Data Act references Data Governance Act
(39, 27, 'cites'),
-- Green Bonds references CSRD and Taxonomy
(42, 28, 'cites'), (42, 9, 'cites'),
-- eIDAS 2 references Cybersecurity Act
(45, 4, 'cites'),
-- Net-Zero references CBAM and Climate Law
(50, 36, 'cites'), (50, 21, 'cites'),
-- Chips Act references Horizon Europe
(51, 19, 'cites'),
-- Euro 7 references CO2 standards
(34, 1, 'amends'),
-- Packaging references Ecodesign
(52, 41, 'cites'),
-- Cyber Resilience references AI Act and Cybersecurity Act
(48, 43, 'cites'), (48, 4, 'cites'),
-- CSDDD references CSRD
(40, 28, 'cites'),
-- CAP Financing references CAP Plans
(23, 22, 'cites'),
-- Horizon Europe impl references framework
(18, 19, 'cites'),
-- AMLA references MiCA
(56, 29, 'cites'),
-- Right to Repair references Ecodesign
(46, 41, 'cites'),
-- Health Data Space references eIDAS
(44, 45, 'cites'),
-- DMA references DSA
(26, 25, 'cites'),
-- Budget conditionality references Common Provisions
(16, 15, 'cites');
