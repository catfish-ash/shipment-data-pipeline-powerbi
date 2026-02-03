# Shipment Data Pipeline + Power BI Analytics (SQL + Python + Postgres)

## Overview
This project showcases end-to-end **Data Engineering** skills:
- Generate and ingest shipment data (CSV)
- Run data quality validation checks
- Load to a **Postgres** data warehouse using a **star schema**
- Expose an enriched SQL view for **Power BI** reporting

## Architecture
1) Python generates and cleans shipment data  
2) Data validation enforces quality rules  
3) Postgres hosts staging, dimensions, and fact tables  
4) Power BI connects to a curated view: `dw.vw_fact_shipments_enriched`

## Data Model (Star Schema)
![Star Schema](models/star_schema.png)

Data dictionary: `models/data_dictionary.md`

## Tech Stack
- Python (pandas)
- Postgres (Docker)
- SQL (dimensional modeling)
- Power BI (dashboarding)

## How to Run Locally

### 1) Start Postgres
```bash
cd docker
docker compose up -d
