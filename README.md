# 🚀 ELT Data Pipeline for eCommerce
--- 
## 1. Introduction
This project builds an **end-to-end ELT pipeline** for eCommerce sales data.  
The pipeline includes:
- **Extract**: Pull data from CSV/MySQL.  
- **Load**: Load data into PostgreSQL (staging).  
- **Transform**: Convert data into an analytical model.  
- **Visualize**: Create dashboards with Metabase.  

**Goal:** Provide insights into revenue, product performance, and business intelligence for decision-making.  

---

## 2. System Architecture
```
CSV/MySQL (Source)
↓
[Extract & Load] → Python Scripts
↓
PostgreSQL (Staging → DWH)
↓
[Transform] → SQL / Airflow DAGs
↓
Metabase (Visualization & BI)
```

## 3. Technologies Used
- **Python** → scripts for data extract/load.  
- **MySQL** → source database.  
- **PostgreSQL** → staging & data warehouse.  
- **Apache Airflow** → workflow orchestration.  
- **Docker** → containerization & deployment.  
- **Metabase** → BI dashboard & reporting.  

## 4. How to Run the Project
- **Step 1: Clone the repo**
```bash
git clone https://github.com/your-repo/elt-ecommerce.git
cd elt-ecommerce
```
- **Step 2: Create .env file**
```
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_pass
POSTGRES_DB=elt_dw
POSTGRES_PORT=5432

MYSQL_USER=your_user
MYSQL_PASSWORD=your_pass
MYSQL_DATABASE=source_db
MYSQL_PORT=3307

AIRFLOW_EXECUTOR=LocalExecutor
AIRFLOW_DB_URI=postgresql+psycopg2://your_user:your_pass@postgres:5432/elt_dw

MB_DB_TYPE=postgres
MB_DB_DBNAME=metabase_metadata
MB_DB_PORT=5432
MB_DB_USER=your_user
MB_DB_PASS=your_pass
MB_DB_HOST=postgres
MB_PORT_HOST=3000
```
- **Step 3: Run with Docker Compose**
```
docker compose up -d
```
- **Step 4: Access services**
- Airflow Webserver → http://localhost:8080

- PostgreSQL → localhost:5432

- MySQL → localhost:3307

- Metabase → http://localhost:3000

## 5. Results
- Full pipeline automated with Airflow.
- Staging and warehouse data synchronized in PostgreSQL.
- Metabase Dashboards: